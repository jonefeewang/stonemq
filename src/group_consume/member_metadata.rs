use std::collections::HashSet;
use std::io::Read;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;

use tokio::sync::oneshot;
use tokio::time::Instant;

use super::coordinator::JoinGroupResult;
use super::MemberMetadata;
use super::Protocol;
use crate::request::KafkaError;
use crate::request::KafkaResult;
use crate::request::ProtocolMetadata;
use crate::request::SyncGroupResponse;

impl MemberMetadata {
    /// 创建新的成员元数据
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: impl Into<String>,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
        group_id: impl Into<String>,
        rebalance_timeout: i32,
        session_timeout: i32,
        protocol_type: impl Into<String>,
        supported_protocols: Vec<(String, Bytes)>,
    ) -> Self {
        Self {
            id: id.into(),
            client_id: client_id.into(),
            client_host: client_host.into(),
            _group_id: group_id.into(),
            rebalance_timeout,
            session_timeout,
            protocol_type: protocol_type.into(),
            supported_protocols: supported_protocols
                .into_iter()
                .map(|(name, metadata)| Protocol { name, metadata })
                .collect(),
            assignment: None,
            join_group_cb_sender: None,
            sync_group_cb_sender: None,
            last_heartbeat: Instant::now(),
            is_leaving: false,
        }
    }

    /// 更新支持的协议列表
    pub fn update_supported_protocols(&mut self, protocols: Vec<ProtocolMetadata>) {
        self.supported_protocols = protocols
            .into_iter()
            .map(|p| Protocol {
                name: p.name,
                metadata: Bytes::copy_from_slice(&p.metadata),
            })
            .collect();
    }

    /// 检查提供的协议元数据是否与当前存储的元数据匹配
    pub fn protocol_matches(&self, protocols: &[ProtocolMetadata]) -> bool {
        if protocols.len() != self.supported_protocols.len() {
            return false;
        }

        protocols.iter().enumerate().all(|(i, protocol)| {
            let current = &self.supported_protocols[i];
            current.name == protocol.name && current.metadata == protocol.metadata
        })
    }

    /// 获取支持的协议名称集合
    pub fn protocols(&self) -> HashSet<String> {
        self.supported_protocols
            .iter()
            .map(|p| p.name.clone())
            .collect()
    }

    /// 对候选协议进行投票
    pub fn vote(&self, candidates: &HashSet<String>) -> String {
        self.protocols()
            .intersection(candidates)
            .next()
            .cloned()
            .unwrap_or_default()
    }

    // Getters
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn metadata(&self, protocol: &str) -> Option<Bytes> {
        self.supported_protocols
            .iter()
            .find(|p| p.name == protocol)
            .map(|p| p.metadata.clone())
    }

    // Assignment 相关方法
    pub fn assignment(&self) -> Option<Bytes> {
        self.assignment.clone()
    }

    pub fn set_assignment(&mut self, assignment: Bytes) {
        self.assignment = Some(assignment);
    }

    // Callback channel 相关方法
    pub fn set_join_group_callback(&mut self, tx: oneshot::Sender<JoinGroupResult>) {
        self.join_group_cb_sender = Some(tx);
    }

    pub fn take_join_group_callback(&mut self) -> oneshot::Sender<JoinGroupResult> {
        self.join_group_cb_sender.take().unwrap()
    }

    pub fn set_sync_group_callback(&mut self, tx: oneshot::Sender<SyncGroupResponse>) {
        self.sync_group_cb_sender = Some(tx);
    }

    pub fn take_sync_group_callback(&mut self) -> Option<oneshot::Sender<SyncGroupResponse>> {
        self.sync_group_cb_sender.take()
    }

    // Heartbeat 相关方法
    pub fn last_heartbeat(&self) -> Instant {
        self.last_heartbeat
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
    }

    // 状态检查方法
    pub fn session_timeout(&self) -> i32 {
        self.session_timeout
    }

    pub fn is_awaiting_join(&self) -> bool {
        self.join_group_cb_sender.is_some()
    }

    pub fn is_awaiting_sync(&self) -> bool {
        self.sync_group_cb_sender.is_some()
    }

    pub fn is_leaving(&self) -> bool {
        self.is_leaving
    }

    pub fn set_leaving(&mut self) {
        self.is_leaving = true;
    }

    pub fn serialize(&self, group_protocol: &str) -> KafkaResult<Bytes> {
        let mut buffer = BytesMut::new();
        // id - string
        buffer.put_u32(self.id.len() as u32);
        buffer.put(self.id.as_bytes());
        // client_id - string
        buffer.put_u32(self.client_id.len() as u32);
        buffer.put(self.client_id.as_bytes());
        // client_host - string
        buffer.put_u32(self.client_host.len() as u32);
        buffer.put(self.client_host.as_bytes());
        // session_timeout - int32
        buffer.put_i32(self.session_timeout);
        // rebalance_timeout - int32
        buffer.put_i32(self.rebalance_timeout);
        // subscription
        if let Some(metadata) = self.metadata(group_protocol) {
            buffer.put_i32(metadata.len() as i32);
            buffer.put(metadata);
        } else {
            buffer.put_i32(-1);
        }
        // assignment
        if let Some(assignment) = self.assignment() {
            buffer.put_i32(assignment.len() as i32); // 修改为i32以匹配反序列化
            buffer.put(assignment);
        } else {
            buffer.put_i32(-1);
        }

        Ok(buffer.freeze())
    }
    pub fn deserialize(
        data: &[u8],
        group_id: &str,
        protocol_type: &String,
        protocol: &String,
    ) -> KafkaResult<Self> {
        let mut cursor = std::io::Cursor::new(data);

        // id - string
        let id_len = cursor.get_u32() as usize;
        let mut id_bytes = vec![0; id_len];
        cursor.read_exact(&mut id_bytes).unwrap();
        let id = String::from_utf8(id_bytes)
            .map_err(|e| KafkaError::CoordinatorNotAvailable(format!("无法解析id: {}", e)))?;

        // client_id - string
        let client_id_len = cursor.get_u32() as usize;
        let mut client_id_bytes = vec![0; client_id_len];
        cursor.read_exact(&mut client_id_bytes).unwrap();
        let client_id = String::from_utf8(client_id_bytes).map_err(|e| {
            KafkaError::CoordinatorNotAvailable(format!("无法解析client_id: {}", e))
        })?;

        // client_host - string
        let client_host_len = cursor.get_u32() as usize;
        let mut client_host_bytes = vec![0; client_host_len];
        cursor.read_exact(&mut client_host_bytes).unwrap();
        let client_host = String::from_utf8(client_host_bytes).map_err(|e| {
            KafkaError::CoordinatorNotAvailable(format!("无法解析client_host: {}", e))
        })?;

        // session_timeout - int32
        let session_timeout = cursor.get_i32();

        // rebalance_timeout - int32
        let rebalance_timeout = cursor.get_i32();

        // subscription metadata
        let metadata_len = cursor.get_i32();
        let mut metadata: Option<Bytes> = None;
        if metadata_len > 0 {
            let mut buf = vec![0; metadata_len as usize];
            cursor.read_exact(&mut buf).unwrap();
            metadata = Some(Bytes::from(buf));
        }

        // assignment
        let assignment_len = cursor.get_i32(); // 修改为get_i32以匹配序列化
        let mut assignment: Option<Bytes> = None;
        if assignment_len > 0 {
            let mut buf = vec![0; assignment_len as usize];
            cursor.read_exact(&mut buf).unwrap();
            assignment = Some(Bytes::from(buf));
        }

        let mut member = Self::new(
            id,
            client_id,
            client_host,
            group_id,
            rebalance_timeout,
            session_timeout,
            protocol_type,
            vec![(protocol.to_string(), metadata.unwrap())],
        );

        // 设置assignment
        if let Some(assignment) = assignment {
            member.set_assignment(assignment);
        }

        Ok(member)
    }
}
