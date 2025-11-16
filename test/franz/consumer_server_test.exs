defmodule Franz.ConsumerServerTest do
  use ExUnit.Case, async: true

  alias Franz.{Consumer, Producer, Message}

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

  test "Consumer.Server starts and handles messages", %{brokers: brokers, topic: topic} do
    # Start a test process to receive handled messages
    test_pid = self()

    handler = fn msg ->
      send(test_pid, {:handled, msg})
      :ok
    end

    config =
      Consumer.Config.new(
        group_id: Franz.Utils.random_bytes(),
        auto_offset_reset: :earliest,
        bootstrap_servers: brokers,
        enable_auto_commit: false
      )

    {:ok, consumer_server} =
      Consumer.Server.start_link(
        topics: [topic],
        handler: handler,
        config: config
      )

    # Give consumer time to subscribe and get assignments
    Process.sleep(100)

    # Send test messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..4 do
      {:ok, _receipt} =
        Producer.send(producer, %Message{
          topic: topic,
          partition: 0,
          payload: "test-message-#{i}"
        })
    end

    :ok = Producer.flush(producer)

    # Receive handled messages
    for i <- 0..4 do
      assert_receive {:handled, msg}, 5000
      assert msg.topic == topic
      assert msg.payload == "test-message-#{i}"
    end

    :ok = Producer.stop(producer)
    GenServer.stop(consumer_server)
  end

  test "Consumer.Server pause and resume", %{brokers: brokers, topic: topic} do
    test_pid = self()

    handler = fn msg ->
      send(test_pid, {:handled, msg})
      :ok
    end

    config =
      Consumer.Config.new(
        group_id: Franz.Utils.random_bytes(),
        auto_offset_reset: :earliest,
        bootstrap_servers: brokers,
        enable_auto_commit: false
      )

    {:ok, consumer_server} =
      Consumer.Server.start_link(
        topics: [topic],
        handler: handler,
        config: config
      )

    Process.sleep(100)

    # Send initial messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..2 do
      {:ok, _receipt} =
        Producer.send(producer, %Message{
          topic: topic,
          partition: 0,
          payload: "msg-#{i}"
        })
    end

    :ok = Producer.flush(producer)

    # Receive messages
    for _ <- 0..2 do
      assert_receive {:handled, _msg}, 5000
    end

    # Pause partition
    :ok = Consumer.Server.pause(consumer_server, [{topic, 0}])

    # Send more messages while paused
    for i <- 3..5 do
      {:ok, _receipt} =
        Producer.send(producer, %Message{
          topic: topic,
          partition: 0,
          payload: "paused-msg-#{i}"
        })
    end

    :ok = Producer.flush(producer)

    # Should not receive messages
    refute_receive {:handled, _}, 1000

    # Resume partition
    :ok = Consumer.Server.resume(consumer_server, [{topic, 0}])

    # Should now receive paused messages
    for _ <- 3..5 do
      assert_receive {:handled, _msg}, 5000
    end

    :ok = Producer.stop(producer)
    GenServer.stop(consumer_server)
  end

  test "Consumer.Server lag monitoring", %{brokers: brokers, topic: topic} do
    test_pid = self()
    message_count = :atomics.new(1, [])

    handler = fn msg ->
      count = :atomics.add_get(message_count, 1, 1)
      send(test_pid, {:handled, msg})

      # Commit first 5 messages, don't commit the rest
      if count <= 5 do
        :ok
      else
        {:ok, :no_commit}
      end
    end

    config =
      Consumer.Config.new(
        group_id: Franz.Utils.random_bytes(),
        auto_offset_reset: :earliest,
        bootstrap_servers: brokers,
        enable_auto_commit: false
      )

    {:ok, consumer_server} =
      Consumer.Server.start_link(
        topics: [topic],
        handler: handler,
        config: config,
        auto_commit: true
      )

    Process.sleep(100)

    # Send messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..14 do
      {:ok, _receipt} =
        Producer.send(producer, %Message{
          topic: topic,
          partition: 0,
          payload: "lag-test-#{i}"
        })
    end

    :ok = Producer.flush(producer)

    # Wait for all messages to be consumed
    for _ <- 0..14 do
      assert_receive {:handled, _msg}, 5000
    end

    # Give commits time to complete
    Process.sleep(50)

    # Check lag - should have lag for the uncommitted messages (10 messages)
    {:ok, lag_map} = Consumer.Server.lag(consumer_server)

    total_lag = lag_map |> Map.values() |> Enum.sum()
    assert total_lag >= 10

    :ok = Producer.stop(producer)
    GenServer.stop(consumer_server)
  end

  test "Consumer.Server handles handler errors gracefully", %{brokers: brokers, topic: topic} do
    test_pid = self()

    handler = fn msg ->
      if String.contains?(msg.payload, "error") do
        send(test_pid, {:error_handled, msg})
        {:error, :test_error}
      else
        send(test_pid, {:ok_handled, msg})
        :ok
      end
    end

    config =
      Consumer.Config.new(
        group_id: Franz.Utils.random_bytes(),
        auto_offset_reset: :earliest,
        bootstrap_servers: brokers,
        enable_auto_commit: false
      )

    {:ok, consumer_server} =
      Consumer.Server.start_link(
        topics: [topic],
        handler: handler,
        config: config
      )

    Process.sleep(100)

    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    {:ok, _receipt} =
        Producer.send(producer, %Message{
        topic: topic,
        partition: 0,
        payload: "good-message"
      })

    {:ok, _receipt} =
        Producer.send(producer, %Message{
        topic: topic,
        partition: 0,
        payload: "error-message"
      })

    :ok = Producer.flush(producer)

    # Both messages should be handled
    assert_receive {:ok_handled, _}, 5000
    assert_receive {:error_handled, _}, 5000

    # Consumer server should still be alive
    assert Process.alive?(consumer_server)

    :ok = Producer.stop(producer)
    GenServer.stop(consumer_server)
  end
end
