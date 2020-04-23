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
pub mod thread_pool;

#[macro_use]
extern crate slog;
pub use client::KvsClient;
pub use engines::{KvStore, KvsEngine, SledStore};
pub use errors::{KvsError, Result};
pub use server::KvsServer;
pub use thread_pool::{NaiveThreadPool, RayonThreadPool, SharedQueueThreadPool, ThreadPool};
