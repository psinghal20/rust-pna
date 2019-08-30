// #![deny(missing_docs)]
//! A basic key value store library

extern crate failure;
#[macro_use] extern crate failure_derive;
mod errors;

pub use errors::{KvsError, Result};
use std::collections::{HashMap, BTreeMap};
use std::{path, option, fs, io, ffi::OsStr};
use serde::{Serialize, Deserialize};
use std::io::prelude::*;
use serde_json;

/// KvStore serves as the storage data structure for
/// our database.
// #[derive(Default)]
pub struct KvStore {
    mem_map: BTreeMap<String, CommandPos>,
    path: path::PathBuf,
    readers: HashMap<u64, io::BufReader<fs::File>>,
    writer: io::BufWriter<fs::File>,
    current_file_no: u64,
    uncompacted_bytes: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set(String, String),
    Rm(String),
}

struct CommandPos {
    file_no: u64,
    start: u64,
    len: u64,
}

impl From<(u64, u64, u64)> for CommandPos {
    fn from((file_no, pos, new_pos): (u64, u64, u64)) -> Self {
        CommandPos {
            file_no: file_no,
            start: pos,
            len: new_pos - pos,
        }
    }
}

const COMPACTION_THRESHOLD: u64 = 1024;

impl KvStore {
    /// Sets a key-value pair into the Key value store
    /// If the store did not have this key present, the key is inserted
    /// If the store did have this key, the value is updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set(key, value);
        let pos = self.writer.seek(io::SeekFrom::Current(0))?;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        let new_pos = self.writer.seek(io::SeekFrom::Current(0))?;
        if let Command::Set(key, ..) = cmd {
            if let Some(old_cmd) = self.mem_map.insert(key, (self.current_file_no, pos, new_pos).into()) {
                self.uncompacted_bytes += old_cmd.len;
            }
        }
        if self.uncompacted_bytes > COMPACTION_THRESHOLD {
            self.compaction()?;
        }
        Ok(())
    }
    /// Returns the value corresponding to the key.
    pub fn get(&mut self, key: String) -> Result<option::Option<String>> {
        if let Some(cmd_pos) = self.mem_map.get(&key) {
            let reader = self.readers.get_mut(&cmd_pos.file_no).expect("Couldn't find the log reader!");
            reader.seek(io::SeekFrom::Start(cmd_pos.start))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let Command::Set(_, value) = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandError)
            }
        } else {
            Ok(None)
        }
    }
    /// Removes a key from the map
    pub fn remove(&mut self, key: String) -> Result<()> {
       if self.mem_map.contains_key(&key) {
           let cmd = Command::Rm(key);
           serde_json::to_writer(&mut self.writer, &cmd)?;
           self.writer.flush()?;
           if let Command::Rm(key) = cmd {
               let old_cmd = self.mem_map.remove(&key).expect("Key not found!");
               self.uncompacted_bytes += old_cmd.len;
           }
           Ok(())
       } else {
           Err(KvsError::NotFoundError(key))
       }
    }

    /// Open specific file from bitcask
    pub fn open(path: &path::Path) -> Result<KvStore>{
        let path = path.to_path_buf();
        fs::create_dir_all(&path)?;
        let mut readers = HashMap::new();
        let mut mem_map = BTreeMap::new();
        let file_list = get_sorted_file_list(&path)?;
        let mut uncompacted_bytes = 0;

        for &file_no in &file_list {
            let mut reader = io::BufReader::new(fs::File::open(log_path(&path, file_no))?);
            uncompacted_bytes += intialise_mem_map(file_no, &mut reader, &mut mem_map)?;
            readers.insert(file_no, reader);
        }

        let current_file_no = file_list.last().unwrap_or(&0) + 1;
        let writer = new_db_file(&path, current_file_no, &mut readers)?;

        let kv_store = KvStore {
            mem_map,
            path,
            readers,
            writer,
            current_file_no,
            uncompacted_bytes,
        };
        Ok(kv_store)
    }

    fn compaction(&mut self) -> Result<()> {
        let compaction_no = self.current_file_no + 1;
        self.current_file_no += 2;
        self.writer = new_db_file(&self.path, self.current_file_no, &mut self.readers)?;
        let mut compaction_writer = new_db_file(&self.path, compaction_no, &mut self.readers)?;

        let mut pos = 0;
        for cmd_pos in self.mem_map.values_mut() {
            let reader = self.readers.get_mut(&cmd_pos.file_no).expect("Couldn't find log reader");
            reader.seek(io::SeekFrom::Start(cmd_pos.start))?;

            let mut cmd_reader = reader.take(cmd_pos.len);
            let len = io::copy(&mut cmd_reader, &mut compaction_writer)?;
            *cmd_pos = (compaction_no, pos, pos + len).into();
            pos += len;
        }
        compaction_writer.flush()?;

        let stale_files: Vec<_> = self.readers.keys().filter(|&&file_no| { file_no < compaction_no }).cloned().collect();
        for stale_file in stale_files {
            self.readers.remove(&stale_file);
            fs::remove_file(log_path(&self.path, stale_file))?;
        }
        self.uncompacted_bytes = 0;
        Ok(())
    }
}

fn intialise_mem_map(file_no: u64, reader: &mut io::BufReader<fs::File>, mem_map: &mut BTreeMap<String, CommandPos>) -> Result<u64> {
    let mut pos = reader.seek(io::SeekFrom::Start(0))?;
    let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted_bytes = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set(key, _) => {
                if let Some(old_cmd) = mem_map.insert(key, (file_no, pos, new_pos).into()) {
                    uncompacted_bytes += old_cmd.len;
                }
            }
            Command::Rm(key) => {
                if let Some(old_cmd) = mem_map.remove(&key) {
                    uncompacted_bytes += old_cmd.len;
                }

                uncompacted_bytes += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted_bytes)
}

fn get_sorted_file_list(path: &path::Path) -> Result<Vec<u64>> {
    let mut file_list: Vec<u64> = fs::read_dir(path)?
                        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
                        .filter(|path| path.is_file() && path.extension() == Some("db".as_ref()))
                        .flat_map(|path| {
                            path.file_name()
                                .and_then(OsStr::to_str)
                                .map(|s| s.trim_end_matches(".db"))
                                .map(str::parse::<u64>)
                        })
                        .flatten()
                        .collect();
    file_list.sort();
    Ok(file_list)
}

fn log_path(path: &path::Path, file_no: u64) -> path::PathBuf {
    path.join(format!("{}.db", file_no))
}

fn new_db_file(path: &path::Path, file_no: u64, readers: &mut HashMap<u64, io::BufReader<fs::File>>) -> Result<io::BufWriter<fs::File>> {
    let path = log_path(path, file_no);
    let writer = io::BufWriter::new(fs::OpenOptions::new()
                 .create(true)
                 .write(true)
                 .append(true)
                 .open(&path)?
    );
    readers.insert(file_no, io::BufReader::new(fs::File::open(&path)?));
    Ok(writer)
}