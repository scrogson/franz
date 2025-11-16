if Code.ensure_loaded?(Broadway) do
  defmodule Franz.BroadwayProducerTest do
    use ExUnit.Case, async: false

    alias Franz.{Producer, Message}

  defmodule TestPipeline do
    use Broadway

    def start_link(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      topics = Keyword.fetch!(opts, :topics)
      brokers = Keyword.fetch!(opts, :brokers)
      group_id = Keyword.fetch!(opts, :group_id)

      Broadway.start_link(__MODULE__,
        name: __MODULE__,
        producer: [
          module:
            {Franz.BroadwayProducer,
             [
               topics: topics,
               group_id: group_id,
               bootstrap_servers: brokers
             ]},
          concurrency: 1
        ],
        processors: [
          default: [
            concurrency: 2,
            min_demand: 1,
            max_demand: 5
          ]
        ],
        context: %{test_pid: test_pid}
      )
    end

    @impl true
    def handle_message(_processor, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_processed, message.data})
      message
    end
  end

  defmodule TestBatcher do
    use Broadway

    def start_link(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      topics = Keyword.fetch!(opts, :topics)
      brokers = Keyword.fetch!(opts, :brokers)
      group_id = Keyword.fetch!(opts, :group_id)

      Broadway.start_link(__MODULE__,
        name: __MODULE__,
        producer: [
          module:
            {Franz.BroadwayProducer,
             [
               topics: topics,
               group_id: group_id,
               bootstrap_servers: brokers
             ]},
          concurrency: 1
        ],
        processors: [
          default: [
            concurrency: 2,
            min_demand: 1,
            max_demand: 10
          ]
        ],
        batchers: [
          default: [
            concurrency: 1,
            batch_size: 5,
            batch_timeout: 1000
          ]
        ],
        context: %{test_pid: test_pid}
      )
    end

    @impl true
    def handle_message(_processor, message, _context) do
      message
    end

    @impl true
    def handle_batch(_batcher, messages, _batch_info, %{test_pid: test_pid}) do
      send(test_pid, {:batch_processed, Enum.map(messages, & &1.data)})
      messages
    end
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
    Process.sleep(200)

    on_exit(fn ->
      :ok = Franz.delete_topic(brokers, topic)
    end)

    {:ok, brokers: brokers, topic: topic}
  end

  test "BroadwayProducer processes messages", %{brokers: brokers, topic: topic} do
    test_pid = self()
    group_id = Franz.Utils.random_bytes()

    {:ok, pipeline} =
      TestPipeline.start_link(
        test_pid: test_pid,
        topics: [topic],
        brokers: brokers,
        group_id: group_id
      )

    # Give Broadway time to start and subscribe
    Process.sleep(500)

    # Produce test messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..9 do
      {:ok, _receipt} =
        Producer.send(producer, %Message{
          topic: topic,
          partition: rem(i, 3),
          payload: "broadway-test-#{i}"
        })
    end

    :ok = Producer.stop(producer)

    # Receive all processed messages
    messages =
      for _ <- 0..9 do
        assert_receive {:message_processed, msg}, 10_000
        msg
      end

    assert length(messages) == 10
    assert Enum.all?(messages, &String.starts_with?(&1.payload, "broadway-test-"))

    Broadway.stop(pipeline)
  end

  test "BroadwayProducer handles batching", %{brokers: brokers, topic: topic} do
    test_pid = self()
    group_id = Franz.Utils.random_bytes()

    {:ok, pipeline} =
      TestBatcher.start_link(
        test_pid: test_pid,
        topics: [topic],
        brokers: brokers,
        group_id: group_id
      )

    # Give Broadway time to start and subscribe
    Process.sleep(500)

    # Produce test messages
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    for i <- 0..14 do
      {:ok, _receipt} =
        Producer.send(producer, %Message{
          topic: topic,
          partition: 0,
          payload: "batch-test-#{i}"
        })
    end

    :ok = Producer.stop(producer)

    # Receive batches
    batches =
      for _ <- 0..2 do
        assert_receive {:batch_processed, batch}, 10_000
        batch
      end

    # Should have 3 batches (5 + 5 + 5)
    all_messages = List.flatten(batches)
    assert length(all_messages) == 15
    assert Enum.all?(all_messages, &String.starts_with?(&1.payload, "batch-test-"))

    Broadway.stop(pipeline)
  end

  test "BroadwayProducer handles metadata", %{brokers: brokers, topic: topic} do
    test_pid = self()
    group_id = Franz.Utils.random_bytes()

    {:ok, pipeline} =
      TestPipeline.start_link(
        test_pid: test_pid,
        topics: [topic],
        brokers: brokers,
        group_id: group_id
      )

    # Give Broadway time to start and subscribe
    Process.sleep(500)

    # Produce message with headers
    {:ok, producer} = Producer.start(Producer.Config.new(bootstrap_servers: brokers))

    {:ok, _receipt} =
        Producer.send(producer, %Message{
        topic: topic,
        partition: 0,
        key: "test-key",
        payload: "test-payload",
        headers: [{"header1", "value1"}, {"header2", "value2"}]
      })

    :ok = Producer.stop(producer)

    # Verify message with metadata
    assert_receive {:message_processed, msg}, 10_000

    assert msg.topic == topic
    assert msg.partition == 0
    assert msg.key == "test-key"
    assert msg.payload == "test-payload"
    assert msg.headers == [{"header1", "value1"}, {"header2", "value2"}]

    Broadway.stop(pipeline)
  end
  end
end
