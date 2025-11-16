defmodule Franz.MixProject do
  use Mix.Project

  def project do
    [
      app: :franz,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Docs
      name: "Franz",
      description: "High-performance Kafka client for Elixir powered by Rust NIFs",
      source_url: "https://github.com/scrogson/franz",
      homepage_url: "https://github.com/scrogson/franz",
      docs: docs(),

      # Package
      package: package(),
      licenses: ["MIT"],

      # Test coverage
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.github": :test
      ]
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
      {:broadway, "~> 1.0", optional: true},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      name: "franz",
      files: ~w(lib native priv .formatter.exs mix.exs README.md LICENSE),
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/scrogson/franz",
        "Docs" => "https://hexdocs.pm/franz"
      },
      maintainers: ["Sonny Scroggin"]
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: [
        "README.md",
        "LICENSE"
      ],
      groups_for_modules: [
        "Core API": [
          Franz,
          Franz.Consumer,
          Franz.Producer,
          Franz.Admin,
          Franz.Message
        ],
        "GenServer Wrappers": [
          Franz.Consumer.Server,
          Franz.Producer.Server
        ],
        "Broadway Integration": [
          Franz.BroadwayProducer
        ],
        Configuration: [
          Franz.Consumer.Config,
          Franz.Producer.Config,
          Franz.Admin.Config
        ],
        "Supporting Types": [
          Franz.DeliveryReceipt,
          Franz.Error,
          Franz.NewTopic,
          Franz.TopicPartition
        ]
      ]
    ]
  end
end
