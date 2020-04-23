use crate::Result;
use std::thread;

pub struct NaiveThreadPool;

impl NaiveThreadPool {}

pub struct SharedQueueThreadPool;

impl SharedQueueThreadPool {}

pub struct RayonThreadPool;

impl RayonThreadPool {}

pub trait ThreadPool {
    fn new(size: usize) -> Result<Self>
    where
        Self: std::marker::Sized;
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

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
