defmodule Franz.ConsumerTest do
  use ExUnit.Case
  doctest Franz

  alias Franz.{Consumer, Producer}

  setup do
    brokers = "127.0.0.1:9094"
    topic = Franz.Utils.random_bytes()
    num_partitions = 10

    :ok =
      Franz.create_topic(brokers, %Franz.NewTopic{
        name: topic,
        num_partitions: num_partitions
      })

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
    config =
      Consumer.Config.new(
        group_id: "test",
        auto_offset_reset: :earliest,
        bootstrap_servers: brokers,
        enable_auto_commit: false,
        # Add additional configuration for better stability
        socket_timeout_ms: 30000,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000
      )

    {:ok, consumer} = Consumer.start(config)
    {:ok, consumer} = Consumer.subscribe(consumer, [topic])

    spawn(fn ->
      config = Producer.Config.new(bootstrap_servers: brokers)

      {:ok, producer} = Producer.start(config)

      for n <- 0..99 do
        :ok =
          Producer.send(producer, %Franz.Message{
            topic: topic,
            partition: :erlang.phash2(n, num_partitions),
            key: "#{n}",
            payload: "#{n}"
          })
      end

      Process.sleep(10_000)

      :ok = Producer.stop(producer)
    end)

    {:ok, assignments, consumer} = Consumer.receive_assignments(consumer)

    for [{^topic, partition, :invalid}, n] <- Enum.zip(assignments, 0..(num_partitions - 1)) do
      assert partition == n
    end

    for _ <- 0..99 do
      receive do
        %Franz.Message{} = msg ->
          :ok = Consumer.commit(consumer, msg)
      end
    end

    {:ok, committed} = Consumer.committed(consumer)

    assert Enum.reduce(committed, 0, fn
             {_, _, {:offset, n}}, acc ->
               acc + n + 1

             {_, _, :invalid}, acc ->
               acc
           end) == 100

    :ok = Consumer.stop(consumer)
  end
end
