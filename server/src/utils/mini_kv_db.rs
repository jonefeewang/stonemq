use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use crate::AppResult;

#[derive(Serialize, Deserialize)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    // 打开数据库文件并加载数据，如果文件不存在则创建一个新的数据库
    pub fn open(path: &str) -> AppResult<(KvStore)> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;

        let mut contents = String::new();

        file.read_to_string(&mut contents)?;

        if contents.is_empty() {
            Ok(KvStore {
                store: HashMap::new(),
            })
        } else {
            let store: HashMap<String, String> = serde_json::from_str(&contents)?;
            Ok(KvStore { store })
        }
    }

    // 插入键值对
    pub fn put(&mut self, key: String, value: String) -> AppResult<()> {
        self.store.insert(key, value);
        Ok(())
    }

    // 获取值
    pub fn get(&self, key: &str) -> Option<String> {
        self.store.get(key).cloned()
    }

    // 删除键
    pub fn delete(&mut self, key: &str) -> AppResult<()> {
        self.store.remove(key);
        Ok(())
    }

    // 将数据库保存到文件
    pub fn save(&self, path: &str) -> AppResult<()> {
        let mut file = OpenOptions::new().write(true).truncate(true).open(path)?;

        let contents = serde_json::to_string_pretty(&self.store)?;
        file.set_len(contents.len() as u64)?;
        file.write_all(contents.as_bytes())?;
        Ok(())
    }
}
#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_kv_store() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("kv.db");

        let mut kv_store = KvStore::open(path.to_str().unwrap()).unwrap();
        kv_store
            .put("key1".to_owned(), "value1".to_owned())
            .unwrap();
        kv_store
            .put("key2".to_owned(), "value2".to_owned())
            .unwrap();

        assert_eq!(kv_store.get("key1"), Some("value1".to_owned()));
        assert_eq!(kv_store.get("key2"), Some("value2".to_owned()));

        kv_store.delete("key1").unwrap();
        assert_eq!(kv_store.get("key1"), None);

        kv_store.save(path.to_str().unwrap()).unwrap();

        let store: KvStore = KvStore::open(path.to_str().unwrap()).unwrap();
        assert_eq!(store.get("key1"), None);
        assert_eq!(store.get("key2"), Some("value2".to_owned()));
    }
}
