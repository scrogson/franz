defmodule Franz.ErrorTest do
  use ExUnit.Case, async: true
  doctest Franz

  alias Franz.{Consumer, Error}

  describe "Error module" do
    test "new/3 creates error with all fields" do
      error = Error.new(:timeout, "Operation timed out", %{timeout_ms: 5000})

      assert error.type == :timeout
      assert error.message == "Operation timed out"
      assert error.details == %{timeout_ms: 5000}
    end

    test "new/2 creates error with empty details" do
      error = Error.new(:network_error, "Connection failed")

      assert error.type == :network_error
      assert error.message == "Connection failed"
      assert error.details == %{}
    end

    test "from_kafka_error/1 parses TopicAlreadyExists" do
      error = Error.from_kafka_error("TopicAlreadyExists: topic 'test' already exists")

      assert error.type == :topic_already_exists
      assert error.message == "Topic already exists"
      assert error.details.raw =~ "TopicAlreadyExists"
    end

    test "from_kafka_error/1 parses UnknownTopicOrPartition" do
      error = Error.from_kafka_error("UnknownTopicOrPartition: topic 'test' not found")

      assert error.type == :topic_not_found
      assert error.message == "Unknown topic or partition"
    end

    test "from_kafka_error/1 parses BrokerNotAvailable" do
      error = Error.from_kafka_error("BrokerNotAvailable: no brokers available")

      assert error.type == :broker_not_available
      assert error.message == "Broker not available"
    end

    test "from_kafka_error/1 parses Authentication errors" do
      error = Error.from_kafka_error("Authentication failed for user")

      assert error.type == :authentication_error
      assert error.message == "Authentication failed"
    end

    test "from_kafka_error/1 parses Authorization errors" do
      error = Error.from_kafka_error("Authorization failed for topic")

      assert error.type == :authorization_error
      assert error.message == "Authorization failed"
    end

    test "from_kafka_error/1 parses GroupCoordinator errors" do
      error = Error.from_kafka_error("GroupCoordinator not available")

      assert error.type == :group_coordinator_not_available
      assert error.message == "Group coordinator not available"
    end

    test "from_kafka_error/1 parses OffsetOutOfRange" do
      error = Error.from_kafka_error("OffsetOutOfRange: offset 100 is out of range")

      assert error.type == :offset_out_of_range
      assert error.message == "Offset out of range"
    end

    test "from_kafka_error/1 parses Network errors" do
      error = Error.from_kafka_error("Network error occurred")

      assert error.type == :network_error
      assert error.message == "Network error"
    end

    test "from_kafka_error/1 parses Timeout errors" do
      error = Error.from_kafka_error("Timeout waiting for response")

      assert error.type == :network_error
      assert error.message == "Network error"
    end

    test "from_kafka_error/1 handles unknown error strings" do
      error = Error.from_kafka_error("Some unknown error")

      assert error.type == :unknown_error
      assert error.message == "Some unknown error"
      assert error.details.raw == "Some unknown error"
    end

    test "from_kafka_error/1 handles non-string errors" do
      error = Error.from_kafka_error(:some_atom)

      assert error.type == :unknown_error
      assert error.message == "Unknown error"
      assert error.details.raw == :some_atom
    end

    test "message/1 formats error message" do
      error = Error.new(:timeout, "Operation timed out")
      message = Exception.message(error)

      assert message == "[timeout] Operation timed out"
    end

    test "message/1 includes details when present and not :raw" do
      error = Error.new(:network_error, "Connection failed", %{host: "localhost", port: 9092})
      message = Exception.message(error)

      assert message =~ "[network_error] Connection failed"
      assert message =~ "host: \"localhost\""
      assert message =~ "port: 9092"
    end

    test "message/1 omits :raw details" do
      error =
        Error.new(:topic_not_found, "Topic not found", %{raw: "UnknownTopicOrPartition: test"})

      message = Exception.message(error)

      assert message == "[topic_not_found] Topic not found"
      refute message =~ "UnknownTopicOrPartition"
    end

    test "wrap/1 passes through Franz.Error unchanged" do
      original = Error.new(:timeout, "Timeout")
      {:error, wrapped} = Error.wrap({:error, original})

      assert wrapped == original
    end

    test "wrap/1 converts binary error to Franz.Error" do
      {:error, error} = Error.wrap({:error, "TopicAlreadyExists: test"})

      assert %Error{} = error
      assert error.type == :topic_already_exists
    end

    test "wrap/1 converts :timeout atom to Franz.Error" do
      {:error, error} = Error.wrap({:error, :timeout})

      assert %Error{} = error
      assert error.type == :timeout
      assert error.message == "Operation timed out"
    end

    test "wrap/1 converts {:unexpected_message, msg} to Franz.Error" do
      {:error, error} = Error.wrap({:error, {:unexpected_message, "some message"}})

      assert %Error{} = error
      assert error.type == :unknown_error
      assert error.message == "Unexpected message"
      assert error.details.message == "some message"
    end

    test "wrap/1 converts unknown errors to Franz.Error" do
      {:error, error} = Error.wrap({:error, :some_random_error})

      assert %Error{} = error
      assert error.type == :unknown_error
      assert error.details.error == :some_random_error
    end

    test "wrap/1 passes through non-error tuples" do
      assert Error.wrap({:ok, :value}) == {:ok, :value}
      assert Error.wrap(:ok) == :ok
    end
  end

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
