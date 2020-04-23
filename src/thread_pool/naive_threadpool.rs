use super::ThreadPool;
use crate::Result;
use std::thread;
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(size: usize) -> Result<NaiveThreadPool> {
        Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
