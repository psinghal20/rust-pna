extern crate structopt;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
#[macro_use]
extern crate clap;
use kvs::{
    KvStore, KvsEngine, KvsError, KvsServer, NaiveThreadPool, Result, SledStore, ThreadPool,
};
use slog::Drain;
use std::env;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Opt {
    #[structopt(short = "V", long = "version")]
    version: bool,
    #[structopt(long, global = true, default_value = "127.0.0.1:4000")]
    addr: String,
    #[structopt(long)]
    engine: Option<Engine>,
}

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub enum Engine {
        kvs,
        sled
    }
}

impl Engine {
    fn from_string(s: &str) -> Self {
        if s == "sled" {
            Self::sled
        } else {
            Self::kvs
        }
    }

    fn to_string(&mut self) -> String {
        match self {
            Self::kvs => String::from("kvs"),
            Self::sled => String::from("sled"),
        }
    }
}

const DEFAULT_ENGINE: Engine = Engine::kvs;

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let mut log = slog::Logger::root(
        drain,
        o!("version" => env!("CARGO_PKG_VERSION"), "addr" => opt.addr.clone()),
    );
    if opt.version {
        println!(env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let current_engine = get_current_engine()?;
    let engine = match current_engine {
        Some(engine) => match opt.engine {
            Some(opt_engine) => {
                if opt_engine.to_string() != engine {
                    error!(log, "Wrong engine provided, current engine: {}", engine);
                    return Err(KvsError::Err("Wrong engine provided".into()));
                }
                opt_engine
            }
            None => Engine::from_string(&engine[..]),
        },
        None => {
            let temp_engine = match opt.engine {
                Some(opt_engine) => opt_engine,
                None => DEFAULT_ENGINE,
            };
            write_current_engine(temp_engine.to_string())?;
            temp_engine
        }
    };
    match engine {
        Engine::kvs => {
            let store = KvStore::open(&env::current_dir()?.as_path())?;
            log = log.new(o!("engine" => "kvs"));
            start_server(store, opt.addr, log.clone())?;
        }
        Engine::sled => {
            let store = SledStore::open(&env::current_dir()?.as_path())?;
            log = log.new(o!("engine" => "sled"));
            start_server(store, opt.addr, log.clone())?;
        }
    }
    Ok(())
}

fn start_server<T: KvsEngine>(store: T, addr: String, log: slog::Logger) -> Result<()> {
    info!(log, "Starting server");
    let pool = NaiveThreadPool::new(4)?;
    KvsServer::new(addr, store, log, pool)?.start()
}

fn get_current_engine() -> Result<Option<String>> {
    let current_engine_file = match File::open("engine.conf") {
        Ok(file) => file,
        Err(e) => match e.kind() {
            io::ErrorKind::NotFound => return Ok(None),
            _ => return Err(e.into()),
        },
    };
    let mut engine_reader = BufReader::new(current_engine_file);
    let mut engine = String::new();
    engine_reader.read_to_string(&mut engine)?;
    Ok(Some(engine))
}

fn write_current_engine(engine: String) -> Result<()> {
    let mut f = File::create("engine.conf")?;
    match f.write(engine.as_bytes()) {
        Ok(_) => Ok(()),
        Err(e) => Err(KvsError::Err(e.to_string())),
    }
}
