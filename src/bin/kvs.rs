extern crate structopt;
use kvs::KvStore;
use std::process;
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

fn main() {
    let opt = Opt::from_args();
    let mut kv_store = KvStore::new();

    if let Some(cmd) = opt.cmd {
        match cmd {
            Cmd::Get { key } => {
                println!("{}", kv_store.get(key.to_string()).unwrap());
            }
            Cmd::Set { key, value } => {
                kv_store.set(key.to_string(), value.to_string());
            }
            Cmd::Remove { key } => {
                kv_store.remove(key.to_string());
            }
        }
    } else if opt.version {
        println!(env!("CARGO_PKG_VERSION"));
    } else {
        process::exit(1);
    }
}
