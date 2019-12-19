use once_cell::sync::Lazy;
use std::future::Future;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

static TOKIO: Lazy<Runtime> = Lazy::new(|| {
    Builder::new()
        .threaded_scheduler()
        .build()
        .expect("Franz.Native: Failed to start tokio runtime")
});

pub fn spawn<T>(task: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    TOKIO.spawn(task)
}
