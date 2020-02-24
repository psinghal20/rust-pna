extern crate structopt;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
use kvs::{KvsServer, Result};
use slog::Drain;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Opt {
    #[structopt(short = "V", long = "version")]
    version: bool,
    #[structopt(long, default_value = "127.0.0.1:4000")]
    addr: String,
    #[structopt(long)]
    engine: Option<String>,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(
        drain,
        o!("version" => env!("CARGO_PKG_VERSION"), "engine" => "kvs", "addr" => opt.addr.clone()),
    );
    let engine = String::from("kvs");
    info!(log, "Starting server");
    let mut server = KvsServer::new(opt.addr, engine, log.clone())?;
    server.start()?;
    if opt.version {
        println!(env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    Ok(())
}
