use crate::Result;

pub trait ThreadPool {
    fn new(size: usize) -> Result<Self>
    where
        Self: std::marker::Sized;
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

mod naive_threadpool;
mod rayon_threadpool;
mod shared_queue;

pub use self::naive_threadpool::NaiveThreadPool;
pub use self::rayon_threadpool::RayonThreadPool;
pub use self::shared_queue::SharedQueueThreadPool;
