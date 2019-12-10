use lazy_static::lazy_static;
use rustler::{Env, Term};
use tokio::runtime::{Builder, Runtime};

mod atoms;
mod client;
//mod consumer;

lazy_static! {
    static ref TOKIO: Runtime = Builder::new().threaded_scheduler().build().unwrap();
}

fn load(env: Env, _: Term) -> bool {
    client::load(env);
    true
}

rustler::init!(
    "Elixir.Franz.Native",
    [
        client::start,
        client::poll,
        //consumer::pause,
        //consumer::resume,
        //consumer::stop,
    ],
    load = load
);
