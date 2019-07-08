extern crate structopt;
use kvs::KvStore;
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

fn main() {
    let opt = Opt::from_args();
    let  kv_store = KvStore::new();

    if let Some(cmd) = opt.cmd {
        match cmd {
            Cmd::Get { key: _ } => {
                eprintln!("unimplemented");
                exit(1);
            }
            Cmd::Set { key: _, value: _ } => {
                eprintln!("unimplemented");
                exit(1);
            }
            Cmd::Remove { key: _ } => {
                eprintln!("unimplemented");
                exit(1);
            }
        }
    } else if opt.version {
        println!(env!("CARGO_PKG_VERSION"));
    } else {
        process::exit(1);
    }
}
