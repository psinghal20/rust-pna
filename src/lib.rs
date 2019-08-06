// #![deny(missing_docs)]
//! A basic key value store library

extern crate failure;
#[macro_use] extern crate failure_derive;

use std::collections::HashMap;
use std::{path, result, option, fs, io};
use serde::{Serialize, Deserialize};
use std::io::prelude::*;
use ron;

/// KvStore serves as the storage data structure for
/// our database.
#[derive(Default)]
pub struct KvStore {
    mem_map: HashMap<String, u64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set(String, String),
    Rm(String),
    Get(String)
}

impl KvStore {
    /// Creates an empty KVStore
    pub fn new() -> KvStore {
        KvStore {
            mem_map: HashMap::new(),
        }
    }
    /// Sets a key-value pair into the Key value store
    /// If the store did not have this key present, the key is inserted
    /// If the store did have this key, the value is updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.intialise_mem_map()?;
        let mut file = fs::OpenOptions::new()
                    .append(true)
                    .open("kvs.ron")?;
        let offset = file.seek(io::SeekFrom::Current(0))?;
        let key_copy = key.clone();
        let cmd = Command::Set(key, value);
        file.write_all(ron::ser::to_string(&cmd)?.as_bytes())?;
        self.mem_map.insert(key_copy, offset);
        Ok(())
    }
    /// Returns the value corresponding to the key.
    pub fn get(&mut self, key: String) -> Result<option::Option<String>> {
        self.intialise_mem_map()?;
        if let Some(value) = self.mem_map.get(&key) {
            let file = fs::File::open("kvs.ron")?;
            let mut buffered_file = io::BufReader::new(file);
            buffered_file.seek(io::SeekFrom::Start(value.to_owned()))?;
            let mut buf: Vec<u8> = Vec::new();
            buffered_file.read_until(b')', &mut buf)?;
            let cmd: Command = ron::de::from_bytes(&buf[..])?;
            if let Command::Set(_, value) = cmd {
                Ok(Some(value))
            } else {
                Err(KvsError::NotFoundError(key))
            }
        } else {
            Err(KvsError::NotFoundError(key))
        }
    }
    /// Removes a key from the map
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.intialise_mem_map()?;
        if let None = self.mem_map.remove(&key) {
            return Err(KvsError::NotFoundError(key));
        }
        let cmd = Command::Rm(key);
        let mut file = fs::OpenOptions::new()
                        .append(true)
                        .open("kvs.ron")?;
        file.write_all(ron::ser::to_string(&cmd)?.as_bytes())?;
        Ok(())
    }

    /// Open specific file from bitcask
    pub fn open(path: &path::Path) -> Result<KvStore>{
        panic!("Can't open a file!");
    }

    fn intialise_mem_map(&mut self) -> Result<()> {
        let file = fs::File::open("kvs.ron")?;
        let mut buffered_file = io::BufReader::new(file);
        let mut buf: Vec<u8> = Vec::new();
        let mut offset = 0;
        let mut count = 0;
        while let Ok(size) = buffered_file.read_until(b')', &mut buf) {
            // println!("Counter: {}", count);
            // let clone_buf = buf.clone();
            // if let Ok(test_string) = String::from_utf8(clone_buf) {
            //     println!("String: {}", test_string );
            // }
            // count = count+ 1;
            if size == 0 {
                return Ok(());
            }
            let cmd: Command = ron::de::from_bytes(&buf[..])?;
            match cmd {
                Command::Set(key, _) => {
                    self.mem_map.insert(key, offset);
                }
                Command::Rm(key) => {
                    self.mem_map.remove(&key);
                }
                _ => {}
            }
            offset = offset + size as u64;
            buf.clear();
        }
        Ok(())
    }
}

/// KVS Error type
#[derive(Debug, Fail)]
pub enum KvsError {
    #[fail(display = "KVS command io-error")]
    IOError(std::io::Error),
    #[fail(display = "KVS command serialization error")]
    SerError(ron::ser::Error),
    #[fail(display = "KVS command deserialization error")]
    DeError(ron::de::Error),
    #[fail(display = "{} not found!", _0)]
    NotFoundError(String),
    #[fail(display = "KVS misc error")]
    Err,
}

impl From<io::Error> for KvsError {
    fn from(error: io::Error) -> Self {
        KvsError::IOError(error)
    }
}

impl From<ron::ser::Error> for KvsError {
    fn from(error: ron::ser::Error) -> Self {
        KvsError::SerError(error)
    }
}

impl From<ron::de::Error> for KvsError {
    fn from(error: ron::de::Error) -> Self {
        KvsError::DeError(error)
    }
}

/// KVS Result type
pub type Result<T> = result::Result<T, KvsError>;