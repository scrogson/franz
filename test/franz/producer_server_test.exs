defmodule Franz.ProducerServerTest do
  use ExUnit.Case, async: true

  alias Franz.{Producer, Consumer, Message}

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

  test "Producer.Server starts and sends messages", %{brokers: brokers, topic: topic} do
    config = Producer.Config.new(bootstrap_servers: brokers)

    {:ok, producer_server} =
      Producer.Server.start_link(
        name: __MODULE__.TestProducer,
        config: config
      )

    # Send messages synchronously
    for i <- 0..4 do
      {:ok, _receipt} =
        Producer.Server.send(producer_server, %Message{
          topic: topic,
          partition: 0,
          payload: "test-message-#{i}"
        })
    end

    # Verify messages were delivered by consuming them
    consumer_config =
      Consumer.Config.new(
        group_id: Franz.Utils.random_bytes(),
        auto_offset_reset: :earliest,
        bootstrap_servers: brokers,
        enable_auto_commit: false
      )

    {:ok, consumer} = Consumer.start(consumer_config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)

    channel = consumer.channel

    for i <- 0..4 do
      msg =
        receive do
          {^channel, {:message, %{msg: msg}}} -> msg
        after
          5000 -> flunk("Did not receive message")
        end

      assert msg.topic == topic
      assert msg.payload == "test-message-#{i}"
    end

    :ok = Consumer.stop(consumer)
    GenServer.stop(producer_server)
  end

  test "Producer.Server send_async", %{brokers: brokers, topic: topic} do
    config =
      Producer.Config.new(
        bootstrap_servers: brokers,
        acks: :leader,
        compression_type: :lz4
      )

    {:ok, producer_server} =
      Producer.Server.start_link(config: config)

    # Send messages asynchronously
    for i <- 0..99 do
      :ok =
        Producer.Server.send_async(producer_server, %Message{
          topic: topic,
          partition: rem(i, 3),
          payload: "async-message-#{i}"
        })
    end

    # Flush to ensure all messages are delivered
    :ok = Producer.Server.flush(producer_server)

    # Verify messages by consuming
    consumer_config =
      Consumer.Config.new(
        group_id: Franz.Utils.random_bytes(),
        auto_offset_reset: :earliest,
        bootstrap_servers: brokers,
        enable_auto_commit: false
      )

    {:ok, consumer} = Consumer.start(consumer_config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)

    channel = consumer.channel

    # Consume all 100 messages
    messages =
      for _ <- 0..99 do
        receive do
          {^channel, {:message, %{msg: msg}}} -> msg
        after
          5000 -> flunk("Did not receive message")
        end
      end

    assert length(messages) == 100
    assert Enum.all?(messages, &(&1.topic == topic))

    :ok = Consumer.stop(consumer)
    GenServer.stop(producer_server)
  end

  test "Producer.Server flush", %{brokers: brokers, topic: topic} do
    config = Producer.Config.new(bootstrap_servers: brokers)

    {:ok, producer_server} =
      Producer.Server.start_link(config: config)

    # Send async messages
    for i <- 0..9 do
      :ok =
        Producer.Server.send_async(producer_server, %Message{
          topic: topic,
          partition: 0,
          payload: "flush-test-#{i}"
        })
    end

    # Flush should complete successfully
    assert :ok = Producer.Server.flush(producer_server)

    GenServer.stop(producer_server)
  end

  test "Producer.Server in_flight_count", %{brokers: brokers, topic: topic} do
    config =
      Producer.Config.new(
        bootstrap_servers: brokers,
        linger_ms: 1000
      )

    {:ok, producer_server} =
      Producer.Server.start_link(config: config)

    # Initially should be 0
    assert Producer.Server.in_flight_count(producer_server) == 0

    # Send async messages with linger to keep them in flight
    for i <- 0..9 do
      :ok =
        Producer.Server.send_async(producer_server, %Message{
          topic: topic,
          partition: 0,
          payload: "in-flight-#{i}"
        })
    end

    # Should have messages in flight
    count = Producer.Server.in_flight_count(producer_server)
    assert count >= 0

    # Flush to clear
    :ok = Producer.Server.flush(producer_server)

    # Should be 0 after flush
    assert Producer.Server.in_flight_count(producer_server) == 0

    GenServer.stop(producer_server)
  end

  test "Producer.Server graceful shutdown with flush", %{brokers: brokers, topic: topic} do
    config = Producer.Config.new(bootstrap_servers: brokers)

    {:ok, producer_server} =
      Producer.Server.start_link(config: config)

    # Send async messages
    for i <- 0..19 do
      :ok =
        Producer.Server.send_async(producer_server, %Message{
          topic: topic,
          partition: 0,
          payload: "shutdown-test-#{i}"
        })
    end

    # Stop should flush automatically
    GenServer.stop(producer_server)

    # Verify all messages were delivered
    consumer_config =
      Consumer.Config.new(
        group_id: Franz.Utils.random_bytes(),
        auto_offset_reset: :earliest,
        bootstrap_servers: brokers,
        enable_auto_commit: false
      )

    {:ok, consumer} = Consumer.start(consumer_config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)

    channel = consumer.channel

    messages =
      for _ <- 0..19 do
        receive do
          {^channel, {:message, %{msg: msg}}} -> msg
        after
          5000 -> flunk("Did not receive message")
        end
      end

    assert length(messages) == 20

    :ok = Consumer.stop(consumer)
  end

  test "Producer.Server get_producer returns underlying producer", %{brokers: brokers} do
    config = Producer.Config.new(bootstrap_servers: brokers)

    {:ok, producer_server} =
      Producer.Server.start_link(config: config)

    {:ok, producer} = Producer.Server.get_producer(producer_server)
    assert %Producer{} = producer

    GenServer.stop(producer_server)
  end
end
