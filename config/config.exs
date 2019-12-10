use Mix.Config

config :franz, Franz.Native,
  mode: :debug,
  env: [{"RUSTFLAGS", "-Z sanitizer=address"}],
  target: "x86_64-unknown-linux-gnu"
