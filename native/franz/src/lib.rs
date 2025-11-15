use rustler::{Env, Term};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod admin;
mod atoms;
mod config;
mod consumer;
mod message;
mod producer;

fn load(_env: Env, load_info: Term) -> bool {
    // Configure tracing
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_env("FRANZ_LOG"))
        .init();

    // Configure Tokio runtime for async tasks
    if let Ok(config) = load_info.decode::<rustler::runtime::RuntimeConfig>() {
        rustler::runtime::configure(config).ok();
    }

    true
}

rustler::init!("Elixir.Franz.Native", load = load);
