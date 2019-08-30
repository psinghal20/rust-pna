extern crate structopt;
use kvs::{KvStore, Result, KvsError};
use std::{process, env};
use structopt::StructOpt;

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
    let mut kv_store = KvStore::open(&env::current_dir()?.as_path())?;
    if let Some(cmd) = opt.cmd {
        match cmd {
            Cmd::Get { key } => {
                if let Ok(result_value) = kv_store.get(key) {
                    if let Some(value) = result_value {
                        println!("{}", value);
                    } else {
                        println!("Key not found");
                    }
                }
                Ok(())
            }
            Cmd::Set { key, value } => {
                kv_store.set(key, value)
            }
            Cmd::Remove { key } => {
                if let Err(KvsError::NotFoundError(_)) = kv_store.remove(key) {
                    println!("Key not found");
                    process::exit(1);
                }
                Ok(())
            }
        }
    } else if opt.version {
        println!(env!("CARGO_PKG_VERSION"));
        Err(KvsError::Err)
    } else {
        process::exit(1);
    }
}
