extern crate structopt;
use kvs::{KvsClient, KvsError, Result};
use std::{env, process};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Opt {
    #[structopt(short = "V", long = "version")]
    version: bool,
    #[structopt(short, long, global = true, default_value = "127.0.0.1:4000")]
    addr: String,
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
    if opt.version {
        println!(env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let mut client = KvsClient::connect(&opt.addr)?;
    if let Some(cmd) = opt.cmd {
        match cmd {
            Cmd::Get { key } => {
                if let Ok(result_value) = client.get(&key) {
                    if let Some(value) = result_value {
                        println!("{}", value);
                    } else {
                        println!("Key not found");
                    }
                }
                Ok(())
            }
            Cmd::Set { key, value } => client.set(&key, &value),
            Cmd::Remove { key } => {
                if let Err(KvsError::NotFoundError(_)) = client.remove(&key) {
                    println!("Key not found");
                    process::exit(1);
                }
                Ok(())
            }
        }
    } else {
        process::exit(1);
    }
}
