// #![deny(missing_docs)]
//! A basic key value store library

extern crate failure;
#[macro_use] extern crate failure_derive;
mod errors;

pub use errors::{KvsError, Result};
use std::collections::HashMap;
use std::collections::HashSet;
use std::{path, option, fs, io};
use serde::{Serialize, Deserialize};
use std::io::prelude::*;
use ron;
use walkdir::WalkDir;
use walkdir::DirEntry;
use uuid;

/// KvStore serves as the storage data structure for
/// our database.
#[derive(Default)]
pub struct KvStore {
    mem_map: HashMap<String, (String, u64)>,
    path: path::PathBuf,
    active_file: option::Option<String>,
    // file_list: Vec<path::PathBuf>,
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
            // file_list: Vec::new(),
        }
    }
    /// Sets a key-value pair into the Key value store
    /// If the store did not have this key present, the key is inserted
    /// If the store did have this key, the value is updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        if let None = self.active_file {
            let path = self.path.clone();
            let path = path.join(uuid::Uuid::new_v4().to_string() + ".ron");
            self.active_file = Some(path.file_name().unwrap().to_str().unwrap().into());
        }
        let active_file = self.active_file.clone().ok_or(KvsError::PathError)?;
        let mut file = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&active_file)?;
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
        if self.check_for_compaction() {
            self.compaction()?;
        }
        Ok(())
    }
    /// Returns the value corresponding to the key.
    pub fn get(&mut self, key: String) -> Result<option::Option<String>> {
        println!("{:?}", self.mem_map);
        if let Some((file_name, offset)) = self.mem_map.get(&key) {
            let file = fs::File::open(file_name)?;
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
        let active_file = self.active_file.clone().ok_or(KvsError::PathError)?;
        let mut file = fs::OpenOptions::new()
                        .append(true)
                        .open(&active_file)?;
        file.write_all(ron::ser::to_string(&cmd)?.as_bytes())?;
        let file_metadata = file.metadata()?;
        if file_metadata.len() > MAX_FILESIZE {
            self.active_file = None;
        }
        if self.check_for_compaction() {
            self.compaction()?;
        }
        Ok(())
    }

    /// Open specific file from bitcask
    pub fn open(path: &path::Path) -> Result<KvStore>{
        let mut kv_store = KvStore {
            mem_map: HashMap::new(),
            path: path.to_path_buf(),
            active_file: None,
            // file_list: Vec::new(),
        };
        kv_store.intialise_mem_map()?;
        Ok(kv_store)
    }

    fn intialise_mem_map(&mut self) -> Result<()> {
        let mut buf: Vec<u8> = Vec::new();
        let walker = WalkDir::new(&self.path).min_depth(1).into_iter();
        for entry in walker.filter_entry(|e| is_hidden(e)) {
            let mut offset = 0;
            // self.file_list.push(entry?.path().to_path_buf());
            // let file_index = self.file_list.len() - 1;
            let path = entry?.path().to_path_buf();
            let file_name = path.file_name().unwrap().to_str().unwrap();
            let file = fs::OpenOptions::new()
                    .read(true)
                    .open(&path)?;
            let len = fs::metadata(&path)?.len();
            if len < MAX_FILESIZE {
                if let None = self.active_file {
                    self.active_file = Some(file_name.into());
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
                        self.mem_map.insert(key, (file_name.into(), offset));
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

    fn check_for_compaction(&mut self) -> bool {
        let walker = WalkDir::new(&self.path).min_depth(1).into_iter();
        let file_count = walker.filter_entry(|e| is_hidden(e)).count();
        file_count > 3
    }

    fn compaction(&mut self) -> Result<()> {
        println!("Trigerring compaction!");
        let active_file = self.active_file.clone().unwrap_or("".to_string());
        let path = self.path.clone();
        let new_file_name = uuid::Uuid::new_v4().to_string() + ".ron";
        let path = path.join(&new_file_name);
        // self.file_list.push(path);
        let mut files_to_remove = HashSet::new();
        let mut buf: Vec<u8> = Vec::new();
        let mut file = fs::File::create(&path)?;
        for (_, (file_name, offset)) in self.mem_map.iter_mut().filter(|value| value.0.to_string() != active_file) {
            files_to_remove.insert(file_name.to_owned());
            let file_read = fs::File::open(&file_name)?;
            let mut buffered_file = io::BufReader::new(file_read);
            buffered_file.seek(io::SeekFrom::Start(offset.to_owned()))?;
            buffered_file.read_until(b')', &mut buf)?;
            *file_name = new_file_name.clone();
            *offset = file.seek(io::SeekFrom::Current(0))?;
            file.write_all(&buf[..])?;
            file.flush()?;
            buf.clear();
        }

        for file_name in files_to_remove {
            fs::remove_file(file_name)?;
        }

        Ok(())
    }
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry.file_name()
         .to_str()
         .map(|s| s.ends_with(".ron"))
         .unwrap_or(false)
}