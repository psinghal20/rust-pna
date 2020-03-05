extern crate structopt;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
#[macro_use]
extern crate clap;
use kvs::{SledStore, KvStore, KvsEngine, KvsServer, Result};
use slog::Drain;
use std::env;
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
    let engine = match opt.engine {
        Some(engine) => engine,
        None => DEFAULT_ENGINE,
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
    KvsServer::new(addr, store, log)?.start()
}
