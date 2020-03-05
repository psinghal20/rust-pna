use serde_json;
use std::io::prelude::*;
use std::net::{Shutdown, TcpStream};

use crate::common::{Command, Response};
use crate::{KvsError, Result};

pub struct KvsClient {
    stream: TcpStream,
}

impl KvsClient {
    pub fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(&addr)?;
        let client = KvsClient { stream: stream };
        Ok(client)
    }

    pub fn send_command(&mut self, cmd: Command) -> Result<Response> {
        serde_json::to_writer(&mut self.stream, &cmd)?;
        self.stream.flush()?;
        self.stream.shutdown(Shutdown::Write)?;
        let res = serde_json::from_reader(&self.stream)?;
        Ok(res)
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        let res = self.send_command(Command::Get(key.to_owned()))?;
        match res {
            Response::Ok(value) => Ok(value),
            Response::Err(error) => Err(KvsError::Err(error)),
        }
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        match self.send_command(Command::Set(key.to_owned(), value.to_owned()))? {
            Response::Ok(_) => Ok(()),
            Response::Err(e) => Err(KvsError::Err(e)),
        }
    }

    pub fn remove(&mut self, key: &str) -> Result<()> {
        match self.send_command(Command::Rm(key.to_owned()))? {
            Response::Ok(_) => Ok(()),
            Response::Err(e) => Err(KvsError::Err(e)),
        }
    }
}
