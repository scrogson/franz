defmodule Franz.ConsumerTest do
  use ExUnit.Case
  doctest Franz

  alias Franz.{Consumer, Producer}
  alias Consumer.Config, as: ConsumerConfig
  alias Producer.Config, as: ProducerConfig

  setup do
    brokers = "localhost:9094"

    # topic = Franz.Utils.random_bytes()

    topic = "test"

    Franz.create_topic(brokers, %Franz.NewTopic{name: topic, num_partitions: 8})

    consumer_config =
      ConsumerConfig.new(
        group_id: "test",
        auto_offset_reset: :earliest,
        bootstrap_servers: brokers,
        enable_auto_commit: false,
        topics: [topic]
      )

    producer_config = ProducerConfig.new(bootstrap_servers: brokers)

    on_exit(fn ->
      nil
      # :ok = Franz.delete_topic(brokers, topic)
    end)

    {:ok, consumer_config: consumer_config, producer_config: producer_config, topic: topic}
  end

  test "polling for new messages", %{
    topic: topic,
    consumer_config: consumer_config,
    producer_config: producer_config
  } do
    {:ok, producer} = Producer.start(producer_config)

    for n <- 0..100 do
      Producer.send(producer, %Franz.Message{
        topic: topic,
        partition: 0,
        key: "",
        payload: "#{n}"
      })
    end

    {:ok, consumer} = Consumer.start(consumer_config)
    {:ok, consumer} = Consumer.subscribe(consumer, consumer_config.topics)
    {:ok, consumer, assignments} = Consumer.receive_assignments(consumer)
    IO.puts(inspect(assignments))
    IO.inspect(Consumer.poll(consumer))
    IO.inspect(Consumer.poll(consumer))
    # IO.inspect(Consumer.unsubscribe(consumer))
    poll_for_messages(consumer)
  end

  defp poll_for_messages(consumer) do
    case Consumer.poll(consumer, 100) do
      %Franz.Message{payload: payload, offset: offset} = msg ->
        IO.inspect("Offset: #{offset}; Payload: #{payload}")
        Consumer.commit(consumer, msg)
    end

    poll_for_messages(consumer)
  end
end
