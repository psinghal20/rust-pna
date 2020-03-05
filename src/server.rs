use crate::common::{Command, Response};
use crate::KvsEngine;
use crate::Result;
use serde_json;
use slog::Logger;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

pub struct KvsServer<T: KvsEngine> {
    addr: String,
    log: Logger,
    store: T,
}

impl<T: KvsEngine> KvsServer<T> {
    pub fn new(addr: String, store: T, log: Logger) -> Result<Self> {
        Ok(KvsServer { addr, store, log })
    }

    pub fn start(&mut self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr)?;
        for stream in listener.incoming() {
            let stream = stream?;
            info!(self.log, "New connection"; "client addr" => stream.peer_addr()?);
            if let Err(e) = self.handle_connection(stream) {
                error!(self.log, "Error while handling connection: {}", e);
            };
        }
        Ok(())
    }

    fn handle_connection(&mut self, mut stream: TcpStream) -> Result<()> {
        let cmd: Command = serde_json::from_reader(&stream)?;
        let res = match cmd {
            Command::Get(key) => match self.store.get(key) {
                Ok(value) => Response::Ok(value),
                Err(e) => Response::Err(e.to_string()),
            },
            Command::Set(key, value) => match self.store.set(key, value) {
                Ok(_) => Response::Ok(None),
                Err(e) => Response::Err(e.to_string()),
            },
            Command::Rm(key) => match self.store.remove(key) {
                Ok(_) => Response::Ok(None),
                Err(e) => Response::Err(e.to_string()),
            },
        };
        serde_json::to_writer(&mut stream, &res)?;
        stream.flush()?;
        Ok(())
    }
}
