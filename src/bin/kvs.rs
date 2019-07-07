#[macro_use]
extern crate clap;
use kvs::KvStore;
use std::process;

fn main() {
    let matches = clap_app!(kvs =>
        (name: env!("CARGO_PKG_NAME"))
        (version: env!("CARGO_PKG_VERSION"))
        (author: env!("CARGO_PKG_AUTHORS"))
        (about: env!("CARGO_PKG_DESCRIPTION"))
        (@arg version: -V --version "returns the version of the kvs")
        (@subcommand get =>
            (about: "Gets the value for give key")
            (@arg KEY: +required)
        )
        (@subcommand set =>
            (about: "Sets the given value for the given key")
            (@arg KEY: +required)
            (@arg VALUE: +required)
        )
        (@subcommand rm =>
            (about: "Removes the given key from the store")
            (@arg KEY: +required)
        )
    )
    .get_matches();
    let mut kv_store = KvStore::new();

    if let Some(matches) = matches.subcommand_matches("get") {
        kv_store.get(matches.value_of("KEY").unwrap().to_string());
    } else if let Some(matches) = matches.subcommand_matches("set") {
        kv_store.set(
            matches.value_of("KEY").unwrap().to_string(),
            matches.value_of("VALUE").unwrap().to_string(),
        );
    } else if let Some(matches) = matches.subcommand_matches("rm") {
        kv_store.remove(matches.value_of("KEY").unwrap().to_string());
    } else if matches.is_present("version") {
        println!(env!("CARGO_PKG_VERSION"));
    } else {
        process::exit(1);
    }
}
