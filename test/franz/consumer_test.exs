defmodule Franz.ConsumerTest do
  use ExUnit.Case
  doctest Franz

  alias Franz.{Consumer, Producer}

  setup do
    brokers = "localhost:9092"
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
        enable_auto_commit: false
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

      :ok = Producer.stop(producer)
    end)

    {:ok, assignments} = Consumer.receive_assignments(consumer)

    for [{^topic, partition, :invalid}, n] <- Enum.zip(assignments, 0..(num_partitions - 1)) do
      assert partition == n
    end

    for _ <- 0..99 do
      case Consumer.poll(consumer) do
        %Franz.Message{} = msg ->
          :ok = Consumer.commit(consumer, msg)
      end
    end

    {:ok, committed} = Consumer.committed(consumer)

    assert Enum.reduce(committed, 0, fn {_, _, {:offset, n}}, acc ->
             acc + n + 1
           end) == 100

    :ok = Consumer.stop(consumer)
  end
end
