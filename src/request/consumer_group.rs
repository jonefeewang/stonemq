use std::{
    collections::{BTreeMap, HashMap},
    io::Read,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{message::TopicPartition, service::Node, AppResult};

use super::errors::{ErrorCode, KafkaError, KafkaResult};


