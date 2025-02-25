use std::cmp::max;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::kvs_command::{kvs_command, KvsCommand, KvsRemove, KvsSet};
use crate::{KvsError, Result};
use crc32fast::Hasher;
use prost::Message;
use std::ffi::OsStr;
use std::time::{SystemTime, UNIX_EPOCH};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;
const CURRENT_SCHEMA_VERSION: u64 = 1;

/// For example, this sequence:
/// store.set("key1", "value1")
/// store.set("key1", "value2")
/// store.remove("key1")
/// Would create log entries like this (in JSON format):
/// {"Set": {"key": "key1", "value": "value1"}}  // position 0-40
/// {"Set": {"key": "key1", "value": "value2"}}  // position 41-81
/// {"Remove": {"key": "key1"}}                  // position 82-105
/// The in-memory index would:
///
/// First point "key1" to position 0
/// Then update to point to position 41
/// Finally remove the "key1" entry completely
///
/// When the amount of stale data (40 + 41 = 81 bytes in this example) exceeds the COMPACTION_THRESHOLD (1MB), the store performs compaction by:
//
/// Creating a new log file
/// Only copying the latest valid entries
/// Updating the index to point to the new locations
/// Deleting the old log files
///
/// This is why it's called "log-structured" - all operations are simply appended to a log, and compaction handles cleanup of old/stale data.
pub struct KvStore {
    // directory for the log and other data.
    path: PathBuf,
    // map generation number to the file reader.
    readers: HashMap<u64, BufReaderWithPos<File>>,
    // writer of the current log.
    writer: BufWriterWithPos<File>,
    current_gen: u64,
    index: BTreeMap<String, CommandPos>,
    // the number of bytes representing "stale" commands that could be
    // deleted during a compaction.
    uncompacted: u64,
    current_sequence: Option<u64>,
    reader_buffer_size: usize,
    writer_buffer_size: usize,
}

impl KvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>, reader_buffer_size: Option<usize>, writer_buffer_size: Option<usize>) -> Result<KvStore> {
        let reader_buffer_size = reader_buffer_size.unwrap_or(8 * 1024); // 8kb
        let writer_buffer_size = writer_buffer_size.unwrap_or(8 * 1024);
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        let mut highest_seq = 0;

        let gen_list = sorted_gen_list(&path)?;
        let mut uncompacted = 0;

        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?, reader_buffer_size)?;

            let uncompat = 0;
            let seq = 0;
            (uncompat, seq) = load_v2(gen, &mut reader, &mut index)?;

            uncompacted += uncompat;
            readers.insert(gen, reader);
            highest_seq = max(highest_seq, seq);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_gen, &mut readers, reader_buffer_size, writer_buffer_size)?;

        Ok(KvStore {
            path,
            readers,
            writer,
            current_gen,
            index,
            uncompacted,
            current_sequence: Some(highest_seq),
            reader_buffer_size,
            writer_buffer_size,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    pub fn set_v2(&mut self, key: String, value: String) -> Result<()> {
        let sequence = self.current_sequence.unwrap_or(0) + 1;
        self.current_sequence = Some(sequence);

        let cmd = KvsCommand::set(key, value, sequence);
        let pos = self.writer.pos;

        let cmd_bytes = cmd.encode_to_vec();

        // Write length prefix (4 bytes, little endian)
        self.writer.write_all(&(cmd_bytes.len() as u32).to_le_bytes())?;

        // Write actual message
        self.writer.write_all(&cmd_bytes)?;
        self.writer.flush()?;

        // Update index and track uncompacted bytes
        if let Some(kvs_command::Command::Set(set)) = cmd.command {
            if let Some(old_cmd) = self
                .index
                .insert(set.key, CommandPos { gen: self.current_gen, pos, len: self.writer.pos - pos })
            {
                self.uncompacted += old_cmd.len;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::UnexpectedCommandType` if the given command type unexpected.
    pub fn get_v2(&mut self, key: String) -> Result<Option<String>>{
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self.readers.get_mut(&cmd_pos.gen).expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;

            // Prefix
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            let msg_len = u32::from_le_bytes(len_bytes) as usize;

            // Read message
            let mut msg_bytes = vec![0; msg_len];
            reader.read_exact(&mut msg_bytes)?;

            let cmd = KvsCommand::decode(&msg_bytes[..])?;
            if !cmd.verify_checksum() {
                return Err(KvsError::CorruptedData);
            }

            if let kvs_command::Command::Set(set) = cmd.command {
                Ok(Some(set.value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    pub fn remove_v2(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {

            let sequence = self.current_sequence.unwrap_or(0) + 1;
            self.current_sequence = Some(sequence);

            let cmd = KvsCommand::remove(key, sequence);
            let pos = self.writer.pos;

            let cmd_bytes = cmd.encode_to_vec();

            // Write length prefix (4 bytes, little endian)
            self.writer.write_all(&(cmd_bytes.len() as u32).to_le_bytes())?;

            // Write actual message
            self.writer.write_all(&cmd_bytes)?;
            self.writer.flush()?;

            if let kvs_command::Command::Remove(remove) = cmd.command {
                let old_cmd = self.index.remove(&remove.key).expect("key not found");
                self.uncompacted += old_cmd.len;

                // The remove command itself will be deleted in compaction
                // once a key is removed, both the original set command and the remove command become "stale"
                // and can be eliminated during compaction.
                self.uncompacted += self.writer.pos - pos;
            }

            if self.uncompacted > COMPACTION_THRESHOLD {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Clears stale entries in the log. And rewrites latest values in a new log file
    pub fn compact(&mut self) -> Result<()> {
        println!("Debug: Starting compaction. Current size: {}", self.uncompacted);

        // increase current gen by 2. current_gen + 1 is for the compaction file.
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = self.new_log_file(self.current_gen)?;

        let mut compaction_writer = self.new_log_file(compaction_gen)?;

        let mut new_pos = 0; // pos in the new log file.
        for cmd_pos in &mut self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }

            // Read length prefix
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            let msg_len = u32::from_le_bytes(len_bytes) as usize;

            // Read the message
            let mut msg_bytes = vec![0; msg_len];
            reader.read_exact(&mut msg_bytes)?;

            // Write length prefix to compaction file
            compaction_writer.write_all(&len_bytes)?;

            // Write message bytes to compaction file
            compaction_writer.write_all(&msg_bytes)?;

            // Update index to point to new location
            *cmd_pos = CommandPos { gen: compaction_gen, pos: new_pos, len: 4 + msg_len as u64 };
            new_pos += 4 + msg_len as u64;
        }
        compaction_writer.flush()?;

        // remove stale log files.
        let stale_gens: Vec<_> = self
            .readers
            .keys()
            .filter(|&&gen| gen < compaction_gen)
            .cloned()
            .collect();
        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }
        self.uncompacted = 0;

        Ok(())
    }

    /// Create a new log file with given generation number and add the reader to the readers map.
    ///
    /// Returns the writer to the log.
    fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.path, gen, &mut self.readers, self.writer_buffer_size, self.reader_buffer_size)
    }
}

/// Create a new log file with given generation number and add the reader to the readers map.
///
/// Returns the writer to the log.
fn new_log_file(
    path: &Path,
    gen: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
    reader_buffer_size: usize,
    writer_buffer_size: usize,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
        writer_buffer_size,
    )?;
    readers.insert(gen, BufReaderWithPos::new(File::open(&path)?, reader_buffer_size)?);
    Ok(writer)
}

/// Returns sorted generation numbers in the given directory.
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

/// Load the whole log file and store value locations in the index map.
///
/// Returns how many bytes can be saved after a compaction.
fn load_v2(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<(u64, u64)> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut uncompacted = 0;
    let mut highest_sequence = 0;

    loop {
        let start_pos = pos;

        // Read the message length (4 bytes) prefix:
        // 4 bytes (32 bits) allows us to represent message sizes up to ~4GB
        // ToDo: Use variable length encoding like varint
        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes) {
            Ok(_) => (),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // reached eof
                break;
            },
            Err(e) => return Err(e.into()),
        }

        let msg_len = u32::from_le_bytes(len_bytes) as usize;
        pos += 4;

        // Read message bytes
        let mut msg_bytes = vec![0u8; msg_len];
        reader.read_exact(&mut msg_bytes)?;
        pos += msg_len as u64;

        // Deserialize the protobuf message
        let cmd = match KvsCommand::decode(&msg_bytes[..]) {
            Ok(cmd) => cmd,
            Err(e) => return Err(KvsError::Deserialize(e))
        };

        if !cmd.verify_checksum() {
            return Err(KvsError::CorruptedData);
        }

        highest_sequence = max(highest_sequence, cmd.sequence_number);
        match cmd.command {
            Some(kvs_command::Command::Set(set)) => {
                let key = set.key;
                let new_pos = CommandPos {
                    gen,
                    pos: start_pos,
                    len: pos - start_pos,
                };

                if let Some(old_cmd) = index.insert(key, new_pos){
                    uncompacted += old_cmd.len;
                }
            }

            Some(kvs_command::Command::Remove(remove)) => {
                let key = remove.key;
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                // The remove command itself can be deleted in compaction
                uncompacted += (pos - start_pos);
            }
            None => {
                return Err(KvsError::UnexpectedCommandType);
            }
        }
    }

    Ok((uncompacted, highest_sequence))
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

/// Struct representing a command.
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

trait Checksumable{
    fn calculate_checksum(&self) -> u32;
    fn get_fields_for_checksum(&self) -> Vec<u8>;
}

impl Checksumable for kvs_command::Command{
    fn calculate_checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.get_fields_for_checksum());
        hasher.finalize()
    }

    fn get_fields_for_checksum(&self) -> Vec<u8> {
        match self {
            command @ kvs_command::Command::Set(set) => {
                let mut fields = Vec::new();
                fields.extend_from_slice(set.key.as_bytes());
                fields.extend_from_slice(set.value.as_bytes());
                fields
            }

            command @ kvs_command::Command::Remove(remove) => {
                let mut fields = Vec::new();
                fields.extend_from_slice(remove.key.as_bytes());
                fields
            }
        }
    }
}

impl KvsCommand {
    fn set(key: String, value: String, sequence: u64) -> KvsCommand {

        let command = kvs_command::Command::Set(KvsSet { key, value, key_size: 0, value_size: 0 });
        let checksum = command.calculate_checksum();
        KvsCommand {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            sequence_number: sequence,
            checksum,
            version: CURRENT_SCHEMA_VERSION as u32,
            command: command.into(),
        }
    }

    fn remove(key: String, sequence: u64) -> KvsCommand
    {
        let command = kvs_command::Command::Remove(KvsRemove { key, key_size: 0 });
        let checksum = command.calculate_checksum();
        KvsCommand {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            sequence_number: sequence,
            checksum,
            version: CURRENT_SCHEMA_VERSION as u32,
            command: command.into(),
        }
    }

    fn verify_checksum(&self) -> bool {
        let stored_checksum = self.checksum;

        let calculated_checksum = match &self.command {
            Some(cmd) => cmd.calculate_checksum(),
            None => return false,
        };

        stored_checksum == calculated_checksum
    }
}

/// Represents the position and length of a json-serialized command in the log.
#[derive(Debug)]
struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R, buffer_size: usize) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::with_capacity(buffer_size, inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W, buffer_size: usize) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::with_capacity(buffer_size, inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}