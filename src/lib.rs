#![deny(missing_docs)]
//! A simple key/value store.

pub use error::{KvsError, Result};
pub use kv::KvStore;

mod error;
mod kv;

#[allow(missing_docs)]
pub mod kvs_command {
    include!(concat!(env!("OUT_DIR"), "/kvs_command.rs"));
}