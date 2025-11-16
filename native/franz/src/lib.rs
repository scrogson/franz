#![deny(warnings)]

use rustler::{Env, Term};
use tracing_subscriber::{fmt, prelude::*, EnvFilter, Layer};

mod admin;
mod atoms;
mod config;
mod consumer;
mod message;
mod producer;

fn load(_env: Env, load_info: Term) -> bool {
    // Configure tracing with a filter that suppresses rdkafka metadata errors
    // These "UnknownTopicOrPartition" errors are transient and expected during topic creation
    let env_filter = std::env::var("FRANZ_LOG")
        .map(|s| EnvFilter::new(&s))
        .unwrap_or_else(|_| {
            // Default: suppress rdkafka logs (which include transient errors during tests)
            // but allow franz crate warnings and errors
            EnvFilter::new("rdkafka=off,librdkafka=off,warn")
        });

    tracing_subscriber::registry()
        .with(fmt::layer().with_filter(env_filter))
        .init();

    // Configure Tokio runtime for async tasks
    if let Ok(config) = load_info.decode::<rustler::runtime::RuntimeConfig>() {
        rustler::runtime::configure(config).ok();
    }

    true
}

rustler::init!("Elixir.Franz.Native", load = load);
