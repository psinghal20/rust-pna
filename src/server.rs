use crate::common::{Command, Response};
use crate::thread_pool::ThreadPool;
use crate::KvsEngine;
use crate::Result;
use serde_json;
use slog::Logger;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

pub struct KvsServer<T: KvsEngine, P: ThreadPool> {
    addr: String,
    log: Logger,
    store: T,
    pool: P,
}

impl<T: KvsEngine, P: ThreadPool> KvsServer<T, P> {
    pub fn new(addr: String, store: T, log: Logger, pool: P) -> Result<Self> {
        Ok(KvsServer {
            addr,
            store,
            log,
            pool,
        })
    }

    pub fn start(&mut self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr)?;
        for stream in listener.incoming() {
            let stream = stream?;
            info!(self.log, "New connection"; "client addr" => stream.peer_addr()?);
            let log = self.log.clone();
            let store = self.store.clone();
            self.pool.spawn(move || {
                if let Err(e) = handle_connection(store, &log, stream) {
                    error!(log, "Error while handling connection: {}", e);
                };
            });
        }
        Ok(())
    }
}

fn handle_connection<T: KvsEngine>(store: T, log: &Logger, mut stream: TcpStream) -> Result<()> {
    let cmd: Command = serde_json::from_reader(&stream)?;
    let res = match cmd {
        Command::Get(key) => {
            debug!(log, "Received get command, key: {}", key);
            match store.get(key) {
                Ok(value) => Response::Ok(value),
                Err(e) => Response::Err(e.to_string()),
            }
        }
        Command::Set(key, value) => {
            debug!(
                log,
                "Received Set command with key: {}, value: {}", key, value
            );
            match store.set(key, value) {
                Ok(_) => Response::Ok(None),
                Err(e) => Response::Err(e.to_string()),
            }
        }
        Command::Rm(key) => {
            debug!(log, "Received Rm command key: {}", key);
            match store.remove(key) {
                Ok(_) => Response::Ok(None),
                Err(e) => {
                    error!(log, "{}", e);
                    Response::Err(e.to_string())
                }
            }
        }
    };
    serde_json::to_writer(&mut stream, &res)?;
    stream.flush()?;
    Ok(())
}
