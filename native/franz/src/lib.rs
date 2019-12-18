use rustler::{Env, Term};

mod admin;
mod atoms;
mod config;
mod consumer;
mod message;
mod producer;
mod task;

fn load(env: Env, _: Term) -> bool {
    env_logger::init();

    admin::load(env);
    consumer::load(env);
    producer::load(env);

    true
}

rustler::init!(
    "Elixir.Franz.Native",
    [
        // Admin API
        admin::start,
        admin::stop,
        admin::create_topics,
        admin::delete_topics,
        //admin::create_partitions,
        //admin::describe_configs,
        //admin::alter_configs,
        // Consumer API
        consumer::start,
        consumer::stop,
        consumer::subscribe,
        consumer::unsubscribe,
        consumer::poll,
        consumer::commit,
        //consumer::pause,
        //consumer::resume,
        // Producer API
        producer::start,
        producer::stop,
        producer::deliver,
    ],
    load = load
);
