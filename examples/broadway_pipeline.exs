# Broadway Pipeline Example
#
# This example demonstrates how to use Franz.BroadwayProducer to build
# a production-ready Kafka message processing pipeline with Broadway.
#
# Prerequisites:
# - Add {:broadway, "~> 1.0"} to your mix.exs dependencies
# - Add {:jason, "~> 1.4"} for JSON encoding/decoding
#
# To run this example:
# 1. Start Kafka/Redpanda: docker compose up -d
# 2. Create a test topic: mix run -e 'Franz.create_topic("localhost:9092", %Franz.NewTopic{name: "events", num_partitions: 3})'
# 3. Run the pipeline: mix run examples/broadway_pipeline.exs
# 4. Produce messages: mix run examples/produce_events.exs

unless Code.ensure_loaded?(Broadway) do
  IO.puts("""
  Error: Broadway is not available.

  Please add Broadway to your dependencies in mix.exs:

    {:broadway, "~> 1.0"}
    {:jason, "~> 1.4"}  # for JSON encoding/decoding

  Then run: mix deps.get
  """)

  System.halt(1)
end

defmodule EventProcessor do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {Franz.BroadwayProducer,
           [
             topics: ["events"],
             group_id: "event-processor-group",
             bootstrap_servers: "localhost:9092"
           ]},
        concurrency: 1
      ],
      processors: [
        default: [
          concurrency: 10,
          min_demand: 5,
          max_demand: 10
        ]
      ],
      batchers: [
        database: [
          concurrency: 5,
          batch_size: 100,
          batch_timeout: 1000
        ],
        analytics: [
          concurrency: 3,
          batch_size: 50,
          batch_timeout: 500
        ]
      ]
    )
  end

  @impl true
  def handle_message(_processor, %Message{data: kafka_msg} = message, _context) do
    # Process individual message
    IO.puts("Processing message: #{inspect(kafka_msg.payload)}")

    # Parse and validate message
    case Jason.decode(kafka_msg.payload) do
      {:ok, %{"type" => "user_signup"} = event} ->
        # Route to database batcher
        Message.put_batcher(message, :database)
        |> Message.put_data(%{type: :user_signup, data: event})

      {:ok, %{"type" => "page_view"} = event} ->
        # Route to analytics batcher
        Message.put_batcher(message, :analytics)
        |> Message.put_data(%{type: :page_view, data: event})

      {:ok, _other} ->
        # No batching, process individually
        message

      {:error, _} ->
        # Mark as failed, will not be committed
        Message.failed(message, "invalid_json")
    end
  end

  @impl true
  def handle_batch(:database, messages, _batch_info, _context) do
    # Process batch of database writes
    IO.puts("Writing batch of #{length(messages)} events to database")

    # Simulate database write
    events = Enum.map(messages, & &1.data)
    IO.inspect(events, label: "Database batch")

    messages
  end

  def handle_batch(:analytics, messages, _batch_info, _context) do
    # Process batch of analytics events
    IO.puts("Sending batch of #{length(messages)} events to analytics")

    # Simulate analytics API call
    events = Enum.map(messages, & &1.data)
    IO.inspect(events, label: "Analytics batch")

    messages
  end
end

# Start the pipeline
{:ok, _pid} = EventProcessor.start_link([])

IO.puts("\n=== Event Processing Pipeline Started ===")
IO.puts("Consuming from: events")
IO.puts("Group ID: event-processor-group")
IO.puts("Processors: 10 concurrent")
IO.puts("Batchers: database (100 msgs), analytics (50 msgs)")
IO.puts("\nPress Ctrl+C to stop\n")

# Keep the process running
Process.sleep(:infinity)
