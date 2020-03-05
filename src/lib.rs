// #![deny(missing_docs)]
//! A basic key value store library

// extern crate failure;
// #[macro_use]
// extern crate failure_derive;
mod client;
mod common;
mod engines;
mod errors;
mod server;

#[macro_use]
extern crate slog;
pub use client::KvsClient;
pub use engines::{SledStore, KvStore, KvsEngine};
pub use errors::{KvsError, Result};
pub use server::KvsServer;
