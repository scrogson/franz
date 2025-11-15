defmodule Franz.ErrorTest do
  use ExUnit.Case, async: true
  doctest Franz

  alias Franz.Consumer

  defp test_consumer_config(brokers) do
    Consumer.Config.new(
      group_id: Franz.Utils.random_bytes(),
      auto_offset_reset: :earliest,
      bootstrap_servers: brokers,
      enable_auto_commit: false,
      session_timeout_ms: 3000,
      heartbeat_interval_ms: 1000
    )
  end

  setup do
    {:ok, brokers: "127.0.0.1:9094", invalid_brokers: "127.0.0.1:19999"}
  end

  test "consumer subscribe to empty topic list", %{brokers: brokers} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)

    # Subscribing to empty list should fail
    assert {:error, _reason} = Consumer.subscribe(consumer, [])

    :ok = Consumer.stop(consumer)
  end

  test "create topic that already exists", %{brokers: brokers} do
    topic = Franz.Utils.random_bytes()

    # Create topic first time - should succeed
    assert :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic})

    Process.sleep(200)

    # Try to create same topic again - should return error
    assert {:error, %Franz.Error{type: :topic_already_exists}} =
             Franz.create_topic(brokers, %Franz.NewTopic{name: topic})

    # Cleanup
    :ok = Franz.delete_topic(brokers, topic)
  end

  test "delete non-existent topic", %{brokers: brokers} do
    non_existent_topic = Franz.Utils.random_bytes()

    # Try to delete topic that doesn't exist - should return error
    assert {:error, %Franz.Error{type: :topic_not_found}} =
             Franz.delete_topic(brokers, non_existent_topic)
  end

  test "consumer committed offsets with no prior commits", %{brokers: brokers} do
    topic = Franz.Utils.random_bytes()

    :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic, num_partitions: 2})

    # Wait for topic metadata to propagate
    Process.sleep(200)

    on_exit(fn ->
      :ok = Franz.delete_topic(brokers, topic)
    end)

    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)

    # Get committed offsets when nothing has been committed yet
    {:ok, committed} = Consumer.committed(consumer)

    # All offsets should be :invalid since nothing has been committed
    for {_topic, _partition, offset} <- committed do
      assert offset == :invalid
    end

    :ok = Consumer.stop(consumer)
  end
end
