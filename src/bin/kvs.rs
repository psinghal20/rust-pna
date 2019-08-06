extern crate structopt;
use kvs::{KvStore, Result, KvsError};
use std::process;
use structopt::StructOpt;
use std::process::exit;

#[derive(StructOpt)]
struct Opt {
    #[structopt(short = "V", long = "version")]
    version: bool,
    #[structopt(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(StructOpt)]
enum Cmd {
    #[structopt(name = "get")]
    Get { key: String },
    #[structopt(name = "set")]
    Set { key: String, value: String },
    #[structopt(name = "rm")]
    Remove { key: String },
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let mut kv_store = KvStore::new();

    if let Some(cmd) = opt.cmd {
        match cmd {
            Cmd::Get { key } => {
                if let Some(value) = kv_store.get(key)? {
                    println!("value is: {}", value);
                }
                Ok(())
            }
            Cmd::Set { key, value } => {
                kv_store.set(key, value)
            }
            Cmd::Remove { key } => {
                kv_store.remove(key)
            }
        }
    } else if opt.version {
        println!(env!("CARGO_PKG_VERSION"));
        Err(KvsError::Err)
    } else {
        process::exit(1);
    }
}
