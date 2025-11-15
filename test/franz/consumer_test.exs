defmodule Franz.ConsumerTest do
  use ExUnit.Case
  doctest Franz

  alias Franz.{Consumer, Producer}

  # Fast consumer config for tests - reduces session timeout from 10s to 3s
  defp test_consumer_config(brokers, group_id \\ nil) do
    Consumer.Config.new(
      group_id: group_id || Franz.Utils.random_bytes(),
      auto_offset_reset: :earliest,
      bootstrap_servers: brokers,
      enable_auto_commit: false,
      session_timeout_ms: 3000,
      heartbeat_interval_ms: 1000,
      max_poll_interval_ms: 10000
    )
  end

  setup do
    brokers = "127.0.0.1:9094"
    topic = Franz.Utils.random_bytes()
    num_partitions = 10

    :ok =
      Franz.create_topic(brokers, %Franz.NewTopic{
        name: topic,
        num_partitions: num_partitions
      })

    # Wait for topic metadata to propagate
    Process.sleep(200)

    on_exit(fn ->
      :ok = Franz.delete_topic(brokers, topic)
    end)

    {:ok, brokers: brokers, topic: topic, num_partitions: num_partitions}
  end

  test "polling for new messages", %{
    brokers: brokers,
    topic: topic,
    num_partitions: num_partitions
  } do
    config = test_consumer_config(brokers, "test")

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])

    {:ok, assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    for [{^topic, partition, :invalid}, n] <- Enum.zip(assignments, 0..(num_partitions - 1)) do
      assert partition == n
    end

    # Send messages after consumer is ready
    spawn(fn ->
      config = Producer.Config.new(bootstrap_servers: brokers)
      {:ok, producer} = Producer.start(config)

      for n <- 0..19 do
        :ok =
          Producer.send(producer, %Franz.Message{
            topic: topic,
            partition: :erlang.phash2(n, num_partitions),
            key: "#{n}",
            payload: "#{n}"
          })
      end

      :ok = Producer.stop(producer)
    end)

    # Receive and commit all messages
    for _ <- 0..19 do
      receive do
        {^channel, {:message, %{msg: msg}}} ->
          :ok = Consumer.commit(consumer, msg)
      end
    end

    {:ok, committed} = Consumer.committed(consumer)

    assert Enum.reduce(committed, 0, fn
             {_, _, {:offset, n}}, acc ->
               acc + n + 1

             {_, _, :invalid}, acc ->
               acc
           end) == 20

    :ok = Consumer.stop(consumer)
  end

  test "consumer unsubscribe", %{brokers: brokers, topic: topic} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)

    {:ok, consumer} = Consumer.unsubscribe(consumer)

    :ok = Consumer.stop(consumer)
  end

  test "consumer assignment", %{brokers: brokers, topic: topic, num_partitions: num_partitions} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)

    {:ok, current_assignment} = Consumer.assignment(consumer)

    assert length(current_assignment) == num_partitions

    for {topic_name, partition, _offset} <- current_assignment do
      assert topic_name == topic
      assert partition in 0..(num_partitions - 1)
    end

    :ok = Consumer.stop(consumer)
  end

  test "consumer subscribe to multiple topics", %{brokers: brokers} do
    topic_a = Franz.Utils.random_bytes()
    topic_b = Franz.Utils.random_bytes()

    :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic_a, num_partitions: 2})
    :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic_b, num_partitions: 2})

    Process.sleep(200)

    on_exit(fn ->
      :ok = Franz.delete_topic(brokers, topic_a)
      :ok = Franz.delete_topic(brokers, topic_b)
    end)

    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic_a, topic_b])

    {:ok, assignments, consumer} = Consumer.receive_assignments(consumer)

    topics_in_assignments =
      assignments
      |> Enum.map(fn {topic, _partition, _offset} -> topic end)
      |> Enum.uniq()
      |> Enum.sort()

    assert topics_in_assignments == Enum.sort([topic_a, topic_b])

    :ok = Consumer.stop(consumer)
  end

  test "consumer receives messages from different partitions", %{
    brokers: brokers,
    topic: topic
  } do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])

    spawn(fn ->
      producer_config = Producer.Config.new(bootstrap_servers: brokers)
      {:ok, producer} = Producer.start(producer_config)

      for i <- 0..4 do
        Producer.send(producer, %Franz.Message{
          topic: topic,
          partition: 0,
          key: "p0-#{i}",
          payload: "partition-0-message-#{i}"
        })
      end

      for i <- 0..4 do
        Producer.send(producer, %Franz.Message{
          topic: topic,
          partition: 1,
          key: "p1-#{i}",
          payload: "partition-1-message-#{i}"
        })
      end

      :ok = Producer.stop(producer)
    end)

    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    messages =
      for _ <- 0..9 do
        receive do
          {^channel, {:message, %{msg: msg}}} -> msg
        after
          5000 -> flunk("Did not receive message")
        end
      end

    partitions =
      messages
      |> Enum.map(& &1.partition)
      |> Enum.uniq()
      |> Enum.sort()

    assert 0 in partitions
    assert 1 in partitions

    :ok = Consumer.stop(consumer)
  end

  test "consumer group rebalancing with multiple consumers", %{
    brokers: brokers,
    topic: topic,
    num_partitions: num_partitions
  } do
    group_id = Franz.Utils.random_bytes()

    config1 = test_consumer_config(brokers, group_id)

    {:ok, consumer1} = Consumer.start(config1)
    {:ok, consumer1} = Consumer.subscribe(consumer1, [topic])
    {:ok, assignments1, consumer1} = Consumer.receive_assignments(consumer1)

    assert length(assignments1) == num_partitions

    config2 = test_consumer_config(brokers, group_id)

    {:ok, consumer2} = Consumer.start(config2)
    {:ok, consumer2} = Consumer.subscribe(consumer2, [topic])

    %{channel: channel1} = consumer1
    %{channel: _channel2} = consumer2

    receive do
      {^channel1, {:pre_rebalance, _}} -> :ok
    after
      5000 -> flunk("Consumer 1 did not receive pre_rebalance")
    end

    receive do
      {^channel1, {:post_rebalance, _}} -> :ok
    after
      5000 -> flunk("Consumer 1 did not receive post_rebalance")
    end

    {:ok, assignments2, consumer2} = Consumer.receive_assignments(consumer2)

    Process.sleep(100)

    receive do
      {^channel1, {:pre_rebalance, _}} -> :ok
    after
      0 -> :ok
    end

    receive do
      {^channel1, {:post_rebalance, _}} -> :ok
    after
      0 -> :ok
    end

    {:ok, new_assignments1} = Consumer.assignment(consumer1)

    total_partitions = length(new_assignments1) + length(assignments2)
    assert total_partitions == num_partitions

    assert length(new_assignments1) > 0
    assert length(assignments2) > 0

    partitions1 =
      new_assignments1
      |> Enum.map(fn {_topic, partition, _offset} -> partition end)
      |> MapSet.new()

    partitions2 =
      assignments2 |> Enum.map(fn {_topic, partition, _offset} -> partition end) |> MapSet.new()

    assert MapSet.intersection(partitions1, partitions2) == MapSet.new()

    :ok = Consumer.stop(consumer1)
    :ok = Consumer.stop(consumer2)
  end

  test "consumer pause and resume", %{brokers: brokers, topic: topic} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    # Send initial messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..4 do
      :ok =
        Producer.send(producer, %Franz.Message{
          topic: topic,
          partition: 0,
          payload: "msg-#{i}"
        })
    end

    # Receive all initial messages
    for _ <- 0..4 do
      receive do
        {^channel, {:message, %{msg: msg}}} ->
          assert msg.partition == 0
      after
        5000 -> flunk("Did not receive initial message")
      end
    end

    # Pause partition 0
    :ok = Consumer.pause(consumer, [{topic, 0}])

    # Give pause time to take effect
    Process.sleep(100)

    # Send more messages while paused
    for i <- 5..9 do
      :ok =
        Producer.send(producer, %Franz.Message{
          topic: topic,
          partition: 0,
          payload: "paused-msg-#{i}"
        })
    end

    # Flush producer to ensure messages are sent
    :ok = Producer.flush(producer)

    # Should not receive new messages from paused partition
    receive do
      {^channel, {:message, %{msg: msg}}} when msg.partition == 0 ->
        flunk("Received message from paused partition: #{msg.payload}")
    after
      2000 -> :ok
    end

    # Resume partition 0
    :ok = Consumer.resume(consumer, [{topic, 0}])

    # Should now receive the messages that were sent while paused
    receive do
      {^channel, {:message, %{msg: msg}}} ->
        assert msg.partition == 0
        assert String.starts_with?(msg.payload, "paused-msg-")
    after
      5000 -> flunk("Did not receive message after resume")
    end

    :ok = Producer.stop(producer)
    :ok = Consumer.stop(consumer)
  end

  test "consumer seek to beginning", %{brokers: brokers, topic: topic} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    # Send messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..4 do
      :ok =
        Producer.send(producer, %Franz.Message{
          topic: topic,
          partition: 0,
          key: "key-#{i}",
          payload: "msg-#{i}"
        })
    end

    # Receive all messages
    messages =
      for _ <- 0..4 do
        receive do
          {^channel, {:message, %{msg: msg}}} -> msg
        after
          5000 -> flunk("Did not receive message")
        end
      end

    first_offset = hd(messages).offset

    # Seek to beginning
    :ok = Consumer.seek(consumer, topic, 0, :beginning)

    # Should receive messages again from beginning
    msg_after_seek =
      receive do
        {^channel, {:message, %{msg: msg}}} -> msg
      after
        5000 -> flunk("Did not receive message after seek")
      end

    assert msg_after_seek.offset == first_offset

    :ok = Producer.stop(producer)
    :ok = Consumer.stop(consumer)
  end

  test "consumer seek to end", %{brokers: brokers, topic: topic} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    # Send initial messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..4 do
      :ok =
        Producer.send(producer, %Franz.Message{
          topic: topic,
          partition: 0,
          payload: "msg-#{i}"
        })
    end

    # Receive all messages
    for _ <- 0..4 do
      receive do
        {^channel, {:message, %{msg: _msg}}} -> :ok
      after
        5000 -> flunk("Did not receive message")
      end
    end

    # Seek to end
    :ok = Consumer.seek(consumer, topic, 0, :end)

    # Should not receive old messages
    receive do
      {^channel, {:message, %{msg: _msg}}} ->
        flunk("Received old message after seek to end")
    after
      1000 -> :ok
    end

    # Send new message
    :ok =
      Producer.send(producer, %Franz.Message{
        topic: topic,
        partition: 0,
        payload: "new-msg"
      })

    # Should receive only new message
    receive do
      {^channel, {:message, %{msg: msg}}} ->
        assert msg.payload == "new-msg"
    after
      5000 -> flunk("Did not receive new message after seek to end")
    end

    :ok = Producer.stop(producer)
    :ok = Consumer.stop(consumer)
  end

  test "consumer seek to specific offset", %{brokers: brokers, topic: topic} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    # Send messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..9 do
      :ok =
        Producer.send(producer, %Franz.Message{
          topic: topic,
          partition: 0,
          key: "key-#{i}",
          payload: "msg-#{i}"
        })
    end

    # Receive all messages and track offsets
    messages =
      for _ <- 0..9 do
        receive do
          {^channel, {:message, %{msg: msg}}} -> msg
        after
          5000 -> flunk("Did not receive message")
        end
      end

    # Get the 5th message's offset
    fifth_msg = Enum.at(messages, 4)
    target_offset = fifth_msg.offset

    # Seek to that specific offset
    :ok = Consumer.seek(consumer, topic, 0, {:offset, target_offset})

    # Should receive messages starting from that offset
    msg_after_seek =
      receive do
        {^channel, {:message, %{msg: msg}}} -> msg
      after
        5000 -> flunk("Did not receive message after seek to offset")
      end

    assert msg_after_seek.offset == target_offset

    :ok = Producer.stop(producer)
    :ok = Consumer.stop(consumer)
  end

  test "consumer position", %{brokers: brokers, topic: topic} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    # Send messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..4 do
      :ok =
        Producer.send(producer, %Franz.Message{
          topic: topic,
          partition: 0,
          payload: "msg-#{i}"
        })
    end

    # Receive messages
    last_msg =
      for _ <- 0..4 do
        receive do
          {^channel, {:message, %{msg: msg}}} -> msg
        after
          5000 -> flunk("Did not receive message")
        end
      end
      |> List.last()

    # Get position
    {:ok, positions} = Consumer.position(consumer)

    # Position should contain partition 0
    partition_0_pos =
      Enum.find(positions, fn {topic_name, partition, _offset} ->
        topic_name == topic && partition == 0
      end)

    assert partition_0_pos != nil

    {^topic, 0, {:offset, position_offset}} = partition_0_pos

    # Position should be the next offset after the last consumed message
    assert position_offset == last_msg.offset + 1

    :ok = Producer.stop(producer)
    :ok = Consumer.stop(consumer)
  end

  test "consumer lag monitoring", %{brokers: brokers, topic: topic} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    # Send some messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..19 do
      :ok =
        Producer.send(producer, %Franz.Message{
          topic: topic,
          partition: 0,
          payload: "lag-test-#{i}"
        })
    end

    :ok = Producer.flush(producer)

    # Wait a bit for messages to be available
    Process.sleep(200)

    # Consume only 10 messages
    for _ <- 0..9 do
      receive do
        {^channel, {:message, %{msg: msg}}} ->
          :ok = Consumer.commit(consumer, msg)
      after
        5000 -> flunk("Did not receive message")
      end
    end

    # Wait for commit to propagate
    Process.sleep(100)

    # Check lag - should have ~10 messages remaining
    {:ok, lag_map} = Consumer.lag(consumer)

    partition_lag = Map.get(lag_map, {topic, 0}, 0)
    assert partition_lag >= 9, "Expected lag >= 9, got #{partition_lag}"

    # Consume remaining messages
    for _ <- 0..9 do
      receive do
        {^channel, {:message, %{msg: msg}}} ->
          :ok = Consumer.commit(consumer, msg)
      after
        5000 -> flunk("Did not receive message")
      end
    end

    # Wait for commit to propagate
    Process.sleep(100)

    # Check lag again - should be 0 or close to 0
    {:ok, lag_map2} = Consumer.lag(consumer)
    partition_lag2 = Map.get(lag_map2, {topic, 0}, 0)
    assert partition_lag2 == 0, "Expected lag 0, got #{partition_lag2}"

    :ok = Producer.stop(producer)
    :ok = Consumer.stop(consumer)
  end

  test "consumer watermarks", %{brokers: brokers, topic: topic} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)

    # Send messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..4 do
      :ok =
        Producer.send(producer, %Franz.Message{
          topic: topic,
          partition: 0,
          payload: "watermark-test-#{i}"
        })
    end

    :ok = Producer.flush(producer)

    # Fetch watermarks
    {:ok, {low, high}} = Consumer.watermarks(consumer, topic, 0)

    # Low should be 0 (or close), high should be at least 5
    assert low >= 0
    assert high >= 5

    :ok = Producer.stop(producer)
    :ok = Consumer.stop(consumer)
  end

  test "consumer receive_batch", %{brokers: brokers, topic: topic} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, _consumer} = Consumer.receive_assignments(consumer)

    # Send messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..19 do
      :ok =
        Producer.send(producer, %Franz.Message{
          topic: topic,
          partition: 0,
          key: "batch-#{i}",
          payload: "batch-test-#{i}"
        })
    end

    :ok = Producer.flush(producer)

    # Receive batch of up to 10 messages
    {:ok, batch1} = Consumer.receive_batch(consumer, max_batch_size: 10, timeout: 2000)

    assert length(batch1) == 10
    assert Enum.all?(batch1, fn msg -> msg.topic == topic end)

    # Commit the batch
    :ok = Consumer.commit_batch(consumer, batch1)

    # Receive remaining messages
    {:ok, batch2} = Consumer.receive_batch(consumer, max_batch_size: 20, timeout: 2000)

    assert length(batch2) == 10

    :ok = Consumer.commit_batch(consumer, batch2)

    :ok = Producer.stop(producer)
    :ok = Consumer.stop(consumer)
  end

  test "consumer receive_batch with min_batch_size", %{brokers: brokers, topic: topic} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, _consumer} = Consumer.receive_assignments(consumer)

    # Send only 5 messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..4 do
      :ok =
        Producer.send(producer, %Franz.Message{
          topic: topic,
          partition: 0,
          payload: "min-batch-#{i}"
        })
    end

    :ok = Producer.flush(producer)

    # Try to receive batch with min_batch_size=5, max=10
    {:ok, batch} =
      Consumer.receive_batch(consumer, max_batch_size: 10, min_batch_size: 5, timeout: 2000)

    assert length(batch) == 5

    :ok = Producer.stop(producer)
    :ok = Consumer.stop(consumer)
  end

  test "consumer receive_batch with empty queue", %{brokers: brokers, topic: topic} do
    config = test_consumer_config(brokers)

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, _assignments, _consumer} = Consumer.receive_assignments(consumer)

    # Don't send any messages
    # Try to receive batch - should timeout and return empty list
    {:ok, batch} = Consumer.receive_batch(consumer, max_batch_size: 10, timeout: 500)

    assert batch == []

    :ok = Consumer.stop(consumer)
  end

  @tag timeout: :infinity
  @tag :performance
  test "produce and consume 1M messages", %{brokers: brokers, topic: topic} do
    num_messages = 1_000_000
    num_partitions = 10

    IO.puts("\n=== Starting 1M message test ===")
    IO.puts("Topic: #{topic}")
    IO.puts("Partitions: #{num_partitions}")
    IO.puts("Messages: #{num_messages}")

    # Start producer with high-throughput config
    config =
      Producer.Config.new(
        bootstrap_servers: brokers,
        acks: :leader,
        compression_type: :lz4,
        linger_ms: 10,
        batch_size: 1_000_000,
        max_in_flight: 10
      )

    {:ok, producer} = Producer.start(config)

    IO.puts("\nProducer config:")
    IO.puts("  acks: :leader")
    IO.puts("  compression: lz4")
    IO.puts("  linger_ms: 10")
    IO.puts("  batch_size: 1MB")
    IO.puts("  max_in_flight: 10")

    # Produce 1M messages using async send
    produce_start = System.monotonic_time(:millisecond)
    IO.puts("\nProducing #{num_messages} messages (async)...")

    for i <- 0..(num_messages - 1) do
      partition = rem(i, num_partitions)

      :ok =
        Producer.send_async(producer, %Franz.Message{
          topic: topic,
          partition: partition,
          key: "key-#{i}",
          payload: "message-#{i}"
        })

      # Print progress every 100k messages
      if rem(i + 1, 100_000) == 0 do
        elapsed = System.monotonic_time(:millisecond) - produce_start
        rate = div((i + 1) * 1000, max(elapsed, 1))
        in_flight = Producer.in_flight_count(producer)
        IO.puts("  Produced #{i + 1} messages (#{rate} msg/sec, #{in_flight} in-flight)")
      end
    end

    # Flush to ensure all messages are sent
    flush_start = System.monotonic_time(:millisecond)
    IO.puts("\nFlushing producer...")
    :ok = Producer.flush(producer, 60_000)
    flush_duration = System.monotonic_time(:millisecond) - flush_start
    IO.puts("  Flushed in #{flush_duration}ms")

    produce_duration = System.monotonic_time(:millisecond) - produce_start
    produce_rate = div(num_messages * 1000, max(produce_duration, 1))

    IO.puts("\n✓ Produced #{num_messages} messages in #{produce_duration}ms")
    IO.puts("  Rate: #{produce_rate} msg/sec")

    # Wait a moment for Kafka to fully persist and index messages
    Process.sleep(1000)

    # Start consumer
    config = test_consumer_config(brokers)
    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])
    {:ok, assignments, consumer} = Consumer.receive_assignments(consumer)
    %{channel: channel} = consumer

    IO.puts("\n✓ Consumer started with #{length(assignments)} partition assignments")

    # Wait for initial rebalance to complete
    Process.sleep(500)

    # Consume all messages
    consume_start = System.monotonic_time(:millisecond)
    IO.puts("\nConsuming #{num_messages} messages...")

    {received_count, last_msg} =
      Stream.unfold({0, nil}, fn {count, _last_msg} ->
        if count >= num_messages do
          nil
        else
          receive do
            {^channel, {:message, %{msg: msg}}} ->
              new_count = count + 1

              # Print progress every 100k messages
              if rem(new_count, 100_000) == 0 do
                elapsed = System.monotonic_time(:millisecond) - consume_start
                rate = div(new_count * 1000, max(elapsed, 1))
                IO.puts("  Consumed #{new_count} messages (#{rate} msg/sec)")
              end

              # Commit periodically to avoid offset loss
              if rem(new_count, 10_000) == 0 do
                :ok = Consumer.commit(consumer, msg)
              end

              {{new_count, msg}, {new_count, msg}}
          after
            30_000 ->
              # Timeout after 30s of no messages
              IO.puts("  Timeout after receiving #{count} messages")
              nil
          end
        end
      end)
      |> Enum.take(num_messages)
      |> List.last()

    consume_duration = System.monotonic_time(:millisecond) - consume_start
    consume_rate = div(received_count * 1000, max(consume_duration, 1))

    IO.puts("\n✓ Consumed #{received_count} messages in #{consume_duration}ms")
    IO.puts("  Rate: #{consume_rate} msg/sec")

    # Commit final message
    if last_msg do
      :ok = Consumer.commit(consumer, last_msg)
    end

    # Verify count
    assert received_count == num_messages

    # Summary
    total_duration = produce_duration + consume_duration
    IO.puts("\n=== Test Summary ===")
    IO.puts("Total time: #{total_duration}ms (#{div(total_duration, 1000)}s)")
    IO.puts("Produce: #{produce_duration}ms @ #{produce_rate} msg/sec")
    IO.puts("Consume: #{consume_duration}ms @ #{consume_rate} msg/sec")
    IO.puts("✓ All #{num_messages} messages produced and consumed successfully")

    :ok = Producer.stop(producer)
    :ok = Consumer.stop(consumer)
  end
end
