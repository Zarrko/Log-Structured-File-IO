use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

/// KvStore
#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

#[derive(Debug)]
pub enum KvsError {
    IoError(io::Error),
}

/// Result type
pub type Result<T> = std::result::Result<T, KvsError>;

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::IoError(err)
    }
}

impl KvStore {
    /// Create new KvStore
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// Sets the value of a given key
    pub fn set(&mut self, key: String, value: String) -> Result<KvsError> {
        panic!()
    }

    /// Gets a key value
    pub fn get(&self, key: String) -> Result<Option<String>> {
        panic!()
    }

    /// Remove from KV Store
    pub fn remove(&mut self, key: String) -> Result<()> {
        panic!()
    }

    /// Open log file
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        panic!()
    }
}
