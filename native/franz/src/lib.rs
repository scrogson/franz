use rustler::{Env, Term};

mod admin;
mod atoms;
mod config;
mod consumer;
mod message;
//mod producer;
mod task;

fn load(env: Env, _: Term) -> bool {
    env_logger::init();
    admin::load(env);
    consumer::load(env);
    true
}

rustler::init!(
    "Elixir.Franz.Native",
    [
        admin::start,
        admin::stop,
        admin::create_topics,
        //admin::delete_topics,
        //admin::create_partitions,
        //admin::describe_configs,
        //admin::alter_configs,
        consumer::start,
        consumer::stop,
        consumer::poll,
        //consumer::pause,
        //consumer::resume,
        //producer::start,
        //producer::stop,
        //producer::poll,
    ],
    load = load
);
