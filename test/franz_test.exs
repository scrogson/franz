defmodule FranzTest do
  use ExUnit.Case
  doctest Franz

  alias Franz.{Config, Client}

  test "polling for new messages" do
    config = Config.new([
      auto_offset_reset: "beginning",
      brokers: "localhost:9094",
      enable_auto_commit: false,
      topics: ["test"]
    ])

    {:ok, client} = Client.start(config)

    for _ <- 0..100 do
      case Client.poll(client, 100) do
        {:ok, nil} -> :ok
        other -> IO.inspect other
      end
    end
  end
end
