extern crate structopt;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
use kvs::{Engine, KvStore, KvsServer, Result};
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

const DEFAULT_ENGINE: Engine = Engine::kvs;

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(
        drain,
        o!("version" => env!("CARGO_PKG_VERSION"), "engine" => "kvs", "addr" => opt.addr.clone()),
    );
    if opt.version {
        println!(env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let engine = match opt.engine {
        Some(engine) => engine,
        None => DEFAULT_ENGINE,
    };
    let store = match engine {
        Engine::kvs => KvStore::open(&env::current_dir()?.as_path())?,
        Engine::sled => panic!("Haven't implemented it till now"),
    };
    info!(log, "Starting server");
    let mut server = KvsServer::new(opt.addr, store, log.clone(), engine)?;
    server.start()?;

    Ok(())
}
