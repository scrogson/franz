defmodule FranzTest do
  use ExUnit.Case
  doctest Franz

  alias Franz.Consumer
  alias Consumer.Config

  test "polling for new messages" do
    config =
      Config.new(
        auto_offset_reset: :beginning,
        bootstrap_servers: "localhost:9094",
        enable_auto_commit: false,
        topics: ["test"]
      )

    {:ok, client} = Consumer.start(config)

    for _ <- 0..100 do
      case Consumer.poll(client, 100) do
        {:ok, nil} -> :ok
        other -> IO.inspect(other)
      end
    end
  end

  test "creating a topic" do
    result =
      Franz.create_topic("localhost:9094", %Franz.NewTopic{
        name: "test2",
        num_partitions: 8,
        replication: 1,
        config: []
      })

    IO.inspect(result)
  end
end
