use assert_cmd::prelude::*;
use kvs_project::{KvStore, KvsError, Result};
use predicates::ord::eq;
use predicates::str::{contains, is_empty, PredicateStrExt};
use std::process::Command;
use tempfile::TempDir;
use walkdir::WalkDir;

// `kvs` with no args should exit with a non-zero code.
#[test]
fn cli_no_args() {
    Command::cargo_bin("kvs").unwrap().assert().failure();
}

// `kvs -V` should print the version
#[test]
fn cli_version() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-V"])
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

// `kvs get <KEY>` should print "Key not found" for a non-existent key and exit with zero.
#[test]
fn cli_get_non_existent_key() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found").trim());
}

// `kvs rm <KEY>` should print "Key not found" for an empty database and exit with non-zero code.
#[test]
fn cli_rm_non_existent_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .failure()
        .stdout(eq("Key not found").trim());
}

// `kvs set <KEY> <VALUE>` should print nothing and exit with zero.
#[test]
fn cli_set() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "key1", "value1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());
}

#[test]
fn cli_get_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = KvStore::open(temp_dir.path(), None, None)?;
    store.set_v2("key1".to_owned(), "value1".to_owned())?;
    store.set_v2("key2".to_owned(), "value2".to_owned())?;
    drop(store);

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("value1").trim());

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("value2").trim());

    Ok(())
}

// `kvs rm <KEY>` should print nothing and exit with zero.
#[test]
fn cli_rm_stored() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = KvStore::open(temp_dir.path(), None, None)?;
    store.set_v2("key1".to_owned(), "value1".to_owned())?;
    drop(store);

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found").trim());

    Ok(())
}

#[test]
fn cli_invalid_get() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_set() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "missing_field"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "extra", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_rm() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm"])
        .assert()
        .failure();

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_subcommand() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["unknown", "subcommand"])
        .assert()
        .failure();
}

// Should get previously stored value.
#[test]
fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path(), None, None)?;

    store.set_v2("key1".to_owned(), "value1".to_owned())?;
    store.set_v2("key2".to_owned(), "value2".to_owned())?;

    assert_eq!(store.get_v2("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get_v2("key2".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path(), None, None)?;
    assert_eq!(store.get_v2("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get_v2("key2".to_owned())?, Some("value2".to_owned()));

    Ok(())
}

// Should overwrite existent value.
#[test]
fn overwrite_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path(), None, None)?;

    store.set_v2("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get_v2("key1".to_owned())?, Some("value1".to_owned()));
    store.set_v2("key1".to_owned(), "value2".to_owned())?;
    assert_eq!(store.get_v2("key1".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path(), None, None)?;
    assert_eq!(store.get_v2("key1".to_owned())?, Some("value2".to_owned()));
    store.set_v2("key1".to_owned(), "value3".to_owned())?;
    assert_eq!(store.get_v2("key1".to_owned())?, Some("value3".to_owned()));

    Ok(())
}

// Should get `None` when getting a non-existent key.
#[test]
fn get_non_existent_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path(), None, None)?;

    store.set_v2("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get_v2("key2".to_owned())?, None);

    // Open from disk again and check persistent data.
    drop(store);
    let mut store = KvStore::open(temp_dir.path(), None, None)?;
    assert_eq!(store.get_v2("key2".to_owned())?, None);

    Ok(())
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path(), None, None)?;
    assert!(store.remove_v2("key1".to_owned()).is_err());
    Ok(())
}

#[test]
fn remove_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path(), None, None)?;
    store.set_v2("key1".to_owned(), "value1".to_owned())?;
    assert!(store.remove_v2("key1".to_owned()).is_ok());
    assert_eq!(store.get_v2("key1".to_owned())?, None);
    Ok(())
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[test]
fn compaction() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path(), None, None)?;

    let dir_size = || {
        let entries = WalkDir::new(temp_dir.path()).into_iter();
        let len: walkdir::Result<u64> = entries
            .map(|res| {
                res.and_then(|entry| entry.metadata())
                    .map(|metadata| metadata.len())
            })
            .sum();
        len.expect("fail to get directory size")
    };

    let mut current_size = dir_size();
    for iter in 0..1000 {
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            let value = format!("{}", iter);
            store.set_v2(key, value)?;
        }

        let new_size = dir_size();
        if new_size > current_size {
            current_size = new_size;
            continue;
        }
        // Compaction triggered.

        drop(store);
        // reopen and check content.
        let mut store = KvStore::open(temp_dir.path(), None, None)?;
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            assert_eq!(store.get_v2(key)?, Some(format!("{}", iter)));
        }
        return Ok(());
    }

    panic!("No compaction detected");
}

#[test]
fn test_log_behavior() -> Result<()> {
    use std::fs;
    use tempfile::TempDir;

    // Create a temporary directory for our test
    let temp_dir = TempDir::new().expect("unable to create temporary directory");

    // Create a single KvStore instance
    let mut store = KvStore::open(temp_dir.path(), None, None)?;

    // Write 5 keys
    for i in 0..5 {
        let value = "x".repeat(10);
        store.set_v2(format!("key{}", i), value)?;

        // Print current state
        println!("After setting key{}", i);
        for entry in fs::read_dir(temp_dir.path())? {
            let entry = entry?;
            println!("  {:?}", entry.path());
        }
    }

    Ok(())
}

/// JSON Serialization would have 5x average latency compared to Protobufs
#[test]
fn test_read_write_latency() -> Result<()> {
    // Arrange
    let temp_dir = TempDir::new().expect("unable to create temporary directory");
    let mut store = KvStore::open(temp_dir.path(), None, None)?;
    let start = std::time::Instant::now();

    // Act
    for i in 0..1000{
        store.set_v2(format!("key{}", i), format!("value{}", i))?;
    }
    let duration = start.elapsed();
    println!("Average write latency in: {}us", duration.as_micros() / 1000);

    let start = std::time::Instant::now();
    for i in 0..1000{
        store.get_v2(format!("key{}", i))?;
    }

    let duration = start.elapsed();
    println!("Average read latency in: {}us", duration.as_micros() / 1000);

    Ok(())
}

#[test]
fn test_large_values() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary directory");
    let mut store = KvStore::open(temp_dir.path(), None, None)?;

    // Create a large value (10MB)
    let large_value = "x".repeat(10 * 1024 * 1024);

    // Test with a few large values
    for i in 0..5 {
        store.set_v2(format!("large{}", i), large_value.clone())?;
    }

    // Verify we can read them back
    for i in 0..5 {
        assert_eq!(store.get_v2(format!("large{}", i))?.unwrap().len(), large_value.len());
    }

    Ok(())
}

// Test concurrent access pattern
#[test]
fn test_high_throughput_writes() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary directory");
    let mut store = KvStore::open(temp_dir.path(), None, None)?;

    let start = std::time::Instant::now();

    // Generate many writes as fast as possible
    for i in 0..10_000 {
        store.set_v2(format!("key{}", i), format!("value{}", i))?;
    }

    let duration = start.elapsed();
    let ops_per_sec = 10_000.0 / duration.as_secs_f64();

    println!("Write throughput: {:.2} ops/sec", ops_per_sec);

    Ok(())
}

// Test checksum verification
#[test]
fn test_checksum_verification() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary directory");
    let mut store = KvStore::open(temp_dir.path(), None, None)?;

    // Write some data
    store.set_v2("key1".to_owned(), "value1".to_owned())?;

    // Close the store
    drop(store);

    // Corrupt the log file
    let log_path = temp_dir.path().join("1.log");
    let mut content = std::fs::read(&log_path)?;

    // Modify some bytes in the middle (shouldn't corrupt the length prefix)
    if content.len() > 20 {
        content[15] = content[15].wrapping_add(1);
        std::fs::write(&log_path, content)?;
    }

    // Try to open and read - should detect corruption
    let mut store = KvStore::open(temp_dir.path(), None, None)?;
    match store.get_v2("key1".to_owned()) {
        Err(e) => {
            assert!(matches!(e, KvsError::CorruptedData));
            println!("Error: {:?}", e)
        },
        Ok(_) => panic!("Should have detected corruption"),
    }

    Ok(())
}
