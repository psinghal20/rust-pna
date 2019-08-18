// #![deny(missing_docs)]
//! A basic key value store library

extern crate failure;
#[macro_use] extern crate failure_derive;
mod errors;

pub use errors::{KvsError, Result};
use std::collections::HashMap;
use std::{path, option, fs, io};
use serde::{Serialize, Deserialize};
use std::io::prelude::*;
use ron;
use walkdir::WalkDir;
use uuid;

/// KvStore serves as the storage data structure for
/// our database.
#[derive(Default)]
pub struct KvStore {
    mem_map: HashMap<String, (usize, u64)>,
    path: path::PathBuf,
    active_file: option::Option<usize>,
    file_list: Vec<path::PathBuf>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set(String, String),
    Rm(String),
    Get(String)
}

const MAX_FILESIZE: u64 = 1024;

impl KvStore {
    /// Creates an empty KVStore
    pub fn new() -> KvStore {
        KvStore {
            mem_map: HashMap::new(),
            path: path::PathBuf::new(),
            active_file: None,
            file_list: Vec::new(),
        }
    }
    /// Sets a key-value pair into the Key value store
    /// If the store did not have this key present, the key is inserted
    /// If the store did have this key, the value is updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        if let None = self.active_file {
            let path = self.path.clone();
            let path = path.join(uuid::Uuid::new_v4().to_string() + ".ron");
            self.file_list.push(path);
            self.active_file = Some(self.file_list.len() - 1);
        }
        let active_file = self.active_file.ok_or(KvsError::PathError)?;
        let mut file = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.file_list.get(active_file).ok_or(KvsError::PathError)?)?;
        file.seek(io::SeekFrom::End(0))?;
        let offset = file.seek(io::SeekFrom::Current(0))?;
        let key_copy = key.clone();
        let cmd = Command::Set(key, value);
        file.write_all(ron::ser::to_string(&cmd)?.as_bytes())?;
        file.flush()?;
        self.mem_map.insert(key_copy, (active_file, offset));
        let file_metadata = file.metadata()?;
        if file_metadata.len() > MAX_FILESIZE {
            self.active_file = None;
        }
        Ok(())
    }
    /// Returns the value corresponding to the key.
    pub fn get(&mut self, key: String) -> Result<option::Option<String>> {
        if let Some((file_index, offset)) = self.mem_map.get(&key) {
            let file = fs::File::open(self.file_list.get(file_index.to_owned()).ok_or(KvsError::PathError)?)?;
            let mut buffered_file = io::BufReader::new(file);
            buffered_file.seek(io::SeekFrom::Start(offset.to_owned()))?;
            let mut buf: Vec<u8> = Vec::new();
            buffered_file.read_until(b')', &mut buf)?;
            let cmd: Command = ron::de::from_bytes(&buf[..])?;
            if let Command::Set(_, value) = cmd {
                Ok(Some(value))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
    /// Removes a key from the map
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let None = self.mem_map.remove(&key) {
            return Err(KvsError::NotFoundError(key));
        }
        let cmd = Command::Rm(key);
        let active_file = self.active_file.ok_or(KvsError::PathError)?;
        let mut file = fs::OpenOptions::new()
                        .append(true)
                        .open(self.file_list.get(active_file).ok_or(KvsError::PathError)?)?;
        file.write_all(ron::ser::to_string(&cmd)?.as_bytes())?;
        let file_metadata = file.metadata()?;
        if file_metadata.len() > MAX_FILESIZE {
            self.active_file = None;
        }
        Ok(())
    }

    /// Open specific file from bitcask
    pub fn open(path: &path::Path) -> Result<KvStore>{
        let mut kv_store = KvStore {
            mem_map: HashMap::new(),
            path: path.to_path_buf(),
            active_file: None,
            file_list: Vec::new(),
        };
        kv_store.intialise_mem_map()?;
        Ok(kv_store)
    }

    fn intialise_mem_map(&mut self) -> Result<()> {
        let mut buf: Vec<u8> = Vec::new();
        for entry in WalkDir::new(&self.path) {
            let mut offset = 0;
            self.file_list.push(entry?.path().to_path_buf());
            let file_index = self.file_list.len() - 1;
            let file = fs::OpenOptions::new()
                    .read(true)
                    .open(self.file_list.get(file_index).ok_or(KvsError::PathError)?)?;
            let len = fs::metadata(self.file_list.get(file_index).ok_or(KvsError::PathError)?)?.len();
            if len < MAX_FILESIZE {
                if let None = self.active_file {
                    self.active_file = Some(file_index);
                }
            }
            let mut buffered_file = io::BufReader::new(file);
            while let Ok(size) = buffered_file.read_until(b')', &mut buf) {
                if size == 0 {
                    break;
                }
                let cmd: Command = ron::de::from_bytes(&buf[..])?;
                match cmd {
                    Command::Set(key, _) => {
                        self.mem_map.insert(key, (file_index, offset));
                    }
                    Command::Rm(key) => {
                        self.mem_map.remove(&key);
                    }
                    _ => {}
                }
                offset = offset + size as u64;
                buf.clear();
            }
        }
        Ok(())
    }
}
