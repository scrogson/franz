use once_cell::sync::OnceCell;
use rustler::{Env, Term};
use std::future::Future;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

pub fn load(_env: Env, _term: Term) -> bool {
    RUNTIME.get_or_init(|| {
        let mut runtime = Builder::new_multi_thread();
        runtime.enable_all();

        // if let Some(n) = options.worker_threads {
        //     runtime.worker_threads(n);
        // }
        //
        // if options.enable_time {
        //     runtime.enable_time();
        // }
        //
        // if options.enable_io {
        //     runtime.enable_io();
        // }

        runtime
            .build()
            .expect("Franz.Native: Failed to start tokio runtime")
    });

    true
}

pub fn spawn<T>(task: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    // Safety: the unwrap is safe because we initialize the runtime in the `load` function
    RUNTIME.get().unwrap().spawn(task)
}
