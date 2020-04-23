use super::ThreadPool;
use crate::Result;
use std::thread;
pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
    fn new(size: usize) -> Result<SharedQueueThreadPool> {
        Ok(SharedQueueThreadPool)
    }
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
    }
}
