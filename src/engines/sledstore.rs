use sled::Db;
use std::borrow::Borrow;
use std::path;
use std::str;

use super::KvsEngine;
use crate::{KvsError, Result};
#[derive(Debug, Clone)]
pub struct SledStore {
    pub store: Db,
}

impl SledStore {
    pub fn open(path: &path::Path) -> Result<Self> {
        Ok(SledStore {
            store: sled::open(path)?,
        })
    }
}

impl KvsEngine for SledStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        match self.store.insert(key, &value[..]) {
            Ok(_) => match self.store.flush() {
                Ok(_) => Ok(()),
                Err(e) => Err(KvsError::SledEngineError(e)),
            },
            Err(e) => Err(KvsError::SledEngineError(e)),
        }
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        match self.store.get(key) {
            Ok(o) => match o {
                Some(value) => return Ok(Some(str::from_utf8(value.borrow())?.to_string())),
                None => Ok(None),
            },
            Err(e) => Err(KvsError::SledEngineError(e)),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        match self.store.remove(&key) {
            Ok(opt) => match opt {
                None => Err(KvsError::NotFoundError(key)),
                Some(_) => match self.store.flush() {
                    Ok(_) => Ok(()),
                    Err(e) => Err(KvsError::SledEngineError(e)),
                },
            },
            Err(e) => Err(KvsError::SledEngineError(e)),
        }
    }
}
