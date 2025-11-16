defmodule Franz.TelemetryTest do
  use ExUnit.Case, async: false

  alias Franz.{Consumer, Producer, Message}

  # Named handler function to avoid telemetry warnings
  def handle_telemetry_event(event_name, measurements, metadata, config) do
    %{events_table: events_table, test_pid: test_pid} = config
    :ets.insert(events_table, {event_name, measurements, metadata})
    send(test_pid, {:telemetry, event_name})
  end

  setup do
    brokers = "127.0.0.1:9094"
    topic = Franz.Utils.random_bytes()

    :ok =
      Franz.create_topic(brokers, %Franz.NewTopic{
        name: topic,
        num_partitions: 1
      })

    # Wait for topic metadata to propagate
    Process.sleep(50)

    on_exit(fn ->
      :ok = Franz.delete_topic(brokers, topic)
    end)

    {:ok, brokers: brokers, topic: topic}
  end

  test "producer telemetry events are emitted", %{brokers: brokers, topic: topic} do
    # Attach telemetry handler
    events_received = :ets.new(:telemetry_events, [:public, :bag])

    :telemetry.attach_many(
      "test-producer-handler",
      [
        [:franz, :producer, :start],
        [:franz, :producer, :send],
        [:franz, :producer, :flush]
      ],
      &__MODULE__.handle_telemetry_event/4,
      %{events_table: events_received, test_pid: self()}
    )

    # Start producer
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    # Verify start event
    assert_receive {:telemetry, [:franz, :producer, :start]}, 1000
    [{_, measurements, metadata}] = :ets.lookup(events_received, [:franz, :producer, :start])
    assert is_integer(measurements.duration)
    assert measurements.duration > 0
    assert metadata.bootstrap_servers == brokers

    # Send message
    {:ok, _receipt} =
        Producer.send(producer, %Message{
        topic: topic,
        partition: 0,
        key: "test-key",
        payload: "test-payload",
        headers: [{"trace-id", "123"}]
      })

    # Verify send event
    assert_receive {:telemetry, [:franz, :producer, :send]}, 1000
    [{_, measurements, metadata}] = :ets.lookup(events_received, [:franz, :producer, :send])
    assert is_integer(measurements.duration)
    assert metadata.topic == topic
    assert metadata.partition == 0
    assert metadata.has_key == true
    assert metadata.has_payload == true
    assert metadata.headers_count == 1

    # Flush
    :ok = Producer.flush(producer)

    # Verify flush event
    assert_receive {:telemetry, [:franz, :producer, :flush]}, 1000
    [{_, measurements, metadata}] = :ets.lookup(events_received, [:franz, :producer, :flush])
    assert is_integer(measurements.duration)
    assert metadata.timeout_ms == 10_000

    :ok = Producer.stop(producer)

    :telemetry.detach("test-producer-handler")
    :ets.delete(events_received)
  end

  test "consumer telemetry events are emitted", %{brokers: brokers, topic: topic} do
    # Attach telemetry handler
    events_received = :ets.new(:telemetry_events, [:public, :bag])

    :telemetry.attach_many(
      "test-consumer-handler",
      [
        [:franz, :consumer, :start],
        [:franz, :consumer, :subscribe],
        [:franz, :consumer, :message],
        [:franz, :consumer, :commit]
      ],
      &__MODULE__.handle_telemetry_event/4,
      %{events_table: events_received, test_pid: self()}
    )

    # Start consumer
    config =
      Consumer.Config.new(
        group_id: Franz.Utils.random_bytes(),
        auto_offset_reset: :earliest,
        bootstrap_servers: brokers,
        enable_auto_commit: false
      )

    {:ok, consumer} = Consumer.start(config)

    # Verify start event
    assert_receive {:telemetry, [:franz, :consumer, :start]}, 1000
    [{_, measurements, metadata}] = :ets.lookup(events_received, [:franz, :consumer, :start])
    assert is_integer(measurements.duration)
    assert metadata.bootstrap_servers == brokers

    # Subscribe
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])

    # Verify subscribe event
    assert_receive {:telemetry, [:franz, :consumer, :subscribe]}, 1000
    [{_, measurements, metadata}] = :ets.lookup(events_received, [:franz, :consumer, :subscribe])
    assert is_integer(measurements.duration)
    assert metadata.topics == [topic]
    assert metadata.topic_count == 1

    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    # Send a message
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    {:ok, _receipt} =
        Producer.send(producer, %Message{
        topic: topic,
        partition: 0,
        payload: "telemetry-test"
      })

    # Receive message and verify telemetry
    receive do
      {^channel, {:message, %{msg: msg}}} ->
        # Call handle_event to trigger telemetry
        {:message, _msg} = Consumer.handle_event({:message, %{msg: msg}})

        # Verify message event
        assert_receive {:telemetry, [:franz, :consumer, :message]}, 1000

        [{_, _measurements, metadata}] =
          :ets.lookup(events_received, [:franz, :consumer, :message])

        assert metadata.topic == topic
        assert metadata.partition == 0

        # Commit
        :ok = Consumer.commit(consumer, msg)

        # Verify commit event
        assert_receive {:telemetry, [:franz, :consumer, :commit]}, 1000
        [{_, measurements, metadata}] = :ets.lookup(events_received, [:franz, :consumer, :commit])
        assert is_integer(measurements.duration)
        assert metadata.topic == topic
        assert metadata.partition == 0
    after
      5000 -> flunk("Did not receive message")
    end

    :ok = Consumer.stop(consumer)
    :ok = Producer.stop(producer)

    :telemetry.detach("test-consumer-handler")
    :ets.delete(events_received)
  end

  test "admin telemetry events are emitted" do
    brokers = "127.0.0.1:9094"
    topic = Franz.Utils.random_bytes()

    # Attach telemetry handler
    events_received = :ets.new(:telemetry_events, [:public, :bag])

    :telemetry.attach_many(
      "test-admin-handler",
      [
        [:franz, :admin, :create_topic],
        [:franz, :admin, :delete_topic]
      ],
      &__MODULE__.handle_telemetry_event/4,
      %{events_table: events_received, test_pid: self()}
    )

    # Create topic
    :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic})

    # Verify create event
    assert_receive {:telemetry, [:franz, :admin, :create_topic]}, 1000
    [{_, measurements, metadata}] = :ets.lookup(events_received, [:franz, :admin, :create_topic])
    assert is_integer(measurements.duration)
    assert metadata.topic == topic
    assert metadata.num_partitions == 1

    Process.sleep(50)

    # Delete topic
    :ok = Franz.delete_topic(brokers, topic)

    # Verify delete event
    assert_receive {:telemetry, [:franz, :admin, :delete_topic]}, 1000
    [{_, measurements, metadata}] = :ets.lookup(events_received, [:franz, :admin, :delete_topic])
    assert is_integer(measurements.duration)
    assert metadata.topic == topic

    :telemetry.detach("test-admin-handler")
    :ets.delete(events_received)
  end
end
