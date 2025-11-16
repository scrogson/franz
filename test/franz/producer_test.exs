defmodule Franz.ProducerTest do
  use ExUnit.Case, async: true
  doctest Franz

  alias Franz.{Consumer, Producer, Message}

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
    brokers = "127.0.0.1:9094"
    topic = Franz.Utils.random_bytes()

    :ok =
      Franz.create_topic(brokers, %Franz.NewTopic{
        name: topic,
        num_partitions: 3
      })

    # Wait for topic metadata to propagate
    Process.sleep(50)

    on_exit(fn ->
      :ok = Franz.delete_topic(brokers, topic)
    end)

    {:ok, brokers: brokers, topic: topic}
  end

  test "send message without key", %{brokers: brokers, topic: topic} do
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    assert {:ok, receipt} =
             Producer.send(producer, %Message{
               topic: topic,
               partition: 0,
               payload: "test payload"
             })

    assert receipt.topic == topic
    assert receipt.partition == 0
    assert receipt.offset >= 0

    :ok = Producer.stop(producer)
  end

  test "send message with key", %{brokers: brokers, topic: topic} do
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    assert {:ok, receipt} =
             Producer.send(producer, %Message{
               topic: topic,
               partition: 0,
               key: "test-key",
               payload: "test payload"
             })

    assert receipt.topic == topic
    assert receipt.partition == 0
    assert receipt.offset >= 0

    :ok = Producer.stop(producer)
  end

  test "send message to specific partition", %{brokers: brokers, topic: topic} do
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    # Send to partition 0
    assert {:ok, receipt0} =
             Producer.send(producer, %Message{
               topic: topic,
               partition: 0,
               payload: "partition 0"
             })

    assert receipt0.partition == 0

    # Send to partition 1
    assert {:ok, receipt1} =
             Producer.send(producer, %Message{
               topic: topic,
               partition: 1,
               payload: "partition 1"
             })

    assert receipt1.partition == 1

    # Send to partition 2
    assert {:ok, receipt2} =
             Producer.send(producer, %Message{
               topic: topic,
               partition: 2,
               payload: "partition 2"
             })

    assert receipt2.partition == 2

    :ok = Producer.stop(producer)
  end

  test "send multiple messages", %{brokers: brokers, topic: topic} do
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..49 do
      assert {:ok, _receipt} =
               Producer.send(producer, %Message{
                 topic: topic,
                 partition: rem(i, 3),
                 key: "key-#{i}",
                 payload: "payload-#{i}"
               })
    end

    :ok = Producer.stop(producer)
  end

  test "producer can send and consumer can receive", %{brokers: brokers, topic: topic} do
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    # Start consumer
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    # Send a message
    {:ok, _receipt} =
      Producer.send(producer, %Message{
        topic: topic,
        partition: 0,
        key: "integration-key",
        payload: "integration-payload"
      })

    # Receive the message
    receive do
      {^channel, {:message, %{msg: msg}}} ->
        assert msg.key == "integration-key"
        assert msg.payload == "integration-payload"
        assert msg.topic == topic
        assert msg.partition == 0
    after
      5000 -> flunk("Did not receive message")
    end

    :ok = Consumer.stop(consumer)
    :ok = Producer.stop(producer)
  end

  test "producer can send and consumer can receive messages with headers", %{
    brokers: brokers,
    topic: topic
  } do
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    # Start consumer
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    # Send a message with headers
    headers = [
      {"trace-id", "12345"},
      {"user-id", "user-123"},
      {"request-id", "req-abc"}
    ]

    {:ok, _receipt} =
      Producer.send(producer, %Message{
        topic: topic,
        partition: 0,
        key: "header-test-key",
        payload: "header-test-payload",
        headers: headers
      })

    # Receive the message and verify headers
    receive do
      {^channel, {:message, %{msg: msg}}} ->
        assert msg.key == "header-test-key"
        assert msg.payload == "header-test-payload"
        assert msg.topic == topic
        assert msg.partition == 0
        assert length(msg.headers) == 3
        assert {"trace-id", "12345"} in msg.headers
        assert {"user-id", "user-123"} in msg.headers
        assert {"request-id", "req-abc"} in msg.headers
    after
      5000 -> flunk("Did not receive message")
    end

    :ok = Consumer.stop(consumer)
    :ok = Producer.stop(producer)
  end

  test "send message with empty headers", %{brokers: brokers, topic: topic} do
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    # Start consumer
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    # Send a message with empty headers list
    {:ok, _receipt} =
      Producer.send(producer, %Message{
        topic: topic,
        partition: 0,
        key: "no-headers",
        payload: "no-headers-payload",
        headers: []
      })

    # Receive the message and verify no headers
    receive do
      {^channel, {:message, %{msg: msg}}} ->
        assert msg.key == "no-headers"
        assert msg.payload == "no-headers-payload"
        assert msg.headers == []
    after
      5000 -> flunk("Did not receive message")
    end

    :ok = Consumer.stop(consumer)
    :ok = Producer.stop(producer)
  end

  test "producer flush ensures messages are delivered", %{brokers: brokers, topic: topic} do
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    # Send multiple messages rapidly
    for i <- 0..9 do
      {:ok, _receipt} =
        Producer.send(producer, %Message{
          topic: topic,
          partition: 0,
          key: "flush-test-#{i}",
          payload: "flush-payload-#{i}"
        })
    end

    # Flush to ensure all messages are sent
    assert :ok = Producer.flush(producer, 5_000)

    :ok = Producer.stop(producer)

    # Now start a consumer to verify all messages were delivered
    config = test_consumer_config(brokers)
    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    # Receive all 10 messages
    messages =
      for _ <- 0..9 do
        receive do
          {^channel, {:message, %{msg: msg}}} -> msg
        after
          5000 -> flunk("Did not receive all flushed messages")
        end
      end

    assert length(messages) == 10
    assert Enum.all?(messages, fn msg -> String.starts_with?(msg.key, "flush-test-") end)

    :ok = Consumer.stop(consumer)
  end
end
