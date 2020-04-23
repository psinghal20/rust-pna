use super::ThreadPool;
use crate::Result;

pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
    fn new(size: usize) -> Result<RayonThreadPool> {
        Ok(RayonThreadPool)
    }
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
    }
}
