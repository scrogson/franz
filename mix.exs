defmodule Franz.MixProject do
  use Mix.Project

  def project do
    [
      app: :franz,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:rustler,
       github: "rusterlium/rustler", branch: "async-nifs", sparse: "rustler_mix", runtime: false},
      {:telemetry, "~> 1.0"},
      {:broadway, "~> 1.0", optional: true}
    ]
  end
end
