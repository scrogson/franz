use rustler::{Env, Term};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod admin;
mod atoms;
mod config;
mod consumer;
mod message;
mod producer;
mod runtime;

fn load(env: Env, term: Term) -> bool {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_env("FRANZ_LOG"))
        .init();
    runtime::load(env, term);
    true
}

rustler::init!("Elixir.Franz.Native", load = load);
