# Franz

> A high-performance Kafka client library for Elixir powered by Rust NIFs

Franz is a modern Kafka client for Elixir that leverages the power of [librdkafka] via Rust NIFs to provide blazing-fast performance with the ergonomics of idiomatic Elixir.

## Features

- 🚀 **High Performance**: Rust NIFs with async I/O for maximum throughput
- 🎯 **Idiomatic Elixir**: Clean, composable API that feels natural in Elixir
- 🔄 **GenServer Wrappers**: Drop-in components for OTP supervision trees
- 🎭 **Broadway Integration**: First-class Broadway support for data pipelines
- 📊 **Telemetry**: Built-in instrumentation for observability
- ⚡ **Async Operations**: Fire-and-forget or synchronous sends
- 🎚️ **Advanced Features**: Consumer lag monitoring, offset management, partition control
- 🛡️ **Production Ready**: Comprehensive test coverage and battle-tested libraries

## Installation

Add `franz` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:franz, "~> 0.1.0"}
  ]
end
```

For Broadway integration, also add:

```elixir
{:broadway, "~> 1.0"}
```

## Quick Start

### Producing Messages

```elixir
alias Franz.{Producer, Message}

# Start a producer
{:ok, producer} = Producer.start(
  Producer.Config.new(
    bootstrap_servers: "localhost:9092",
    acks: :leader,
    compression_type: :lz4
  )
)

# Send a message (synchronous) - returns delivery receipt
{:ok, receipt} = Producer.send(producer, %Message{
  topic: "events",
  partition: 0,
  key: "user-123",
  payload: "Hello, Kafka!"
})

# receipt contains metadata: topic, partition, offset, timestamp
IO.inspect(receipt)
# %Franz.DeliveryReceipt{topic: "events", partition: 0, offset: 123, timestamp: 1234567890}

# Send async (fire-and-forget)
:ok = Producer.send_async(producer, %Message{
  topic: "events",
  partition: 0,
  payload: "Fast message"
})

# Ensure delivery
:ok = Producer.flush(producer)

# Cleanup
:ok = Producer.stop(producer)
```

### Consuming Messages

```elixir
alias Franz.{Consumer, Message}

# Start a consumer
{:ok, consumer} = Consumer.start(
  Consumer.Config.new(
    group_id: "my-app",
    bootstrap_servers: "localhost:9092",
    auto_offset_reset: :earliest
  )
)

# Subscribe to topics
{:ok, consumer} = Consumer.subscribe(consumer, ["events"])

# Wait for assignment
{:ok, assignments, consumer} = Consumer.receive_assignments(consumer)

# Consume messages
channel = consumer.channel

receive do
  {^channel, {:message, %{msg: msg}}} ->
    IO.inspect(msg.payload)
    :ok = Consumer.commit(consumer, msg)
end
```

## GenServer Wrappers

For easy integration with OTP supervision trees:

### Producer Server

```elixir
defmodule MyApp.EventProducer do
  def start_link(_opts) do
    config = Franz.Producer.Config.new(
      bootstrap_servers: "localhost:9092",
      acks: :leader,
      compression_type: :lz4
    )

    Franz.Producer.Server.start_link(
      name: __MODULE__,
      config: config
    )
  end

  def send_event(event_data) do
    message = %Franz.Message{
      topic: "events",
      payload: Jason.encode!(event_data)
    }

    Franz.Producer.Server.send_async(__MODULE__, message)
  end
end

# Add to your supervision tree
children = [
  MyApp.EventProducer
]
```

### Consumer Server

```elixir
defmodule MyApp.EventConsumer do
  def start_link(_opts) do
    config = Franz.Consumer.Config.new(
      group_id: "my-app-consumers",
      bootstrap_servers: "localhost:9092",
      auto_offset_reset: :earliest,
      enable_auto_commit: false
    )

    Franz.Consumer.Server.start_link(
      name: __MODULE__,
      topics: ["events"],
      handler: &handle_message/1,
      config: config
    )
  end

  def handle_message(message) do
    IO.puts("Received: #{message.payload}")
    :ok  # Commits offset
  end
end
```

## Broadway Integration

Franz provides a Broadway producer for building powerful data processing pipelines:

```elixir
defmodule MyApp.EventProcessor do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {Franz.BroadwayProducer, [
          topics: ["events"],
          group_id: "event-processor",
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
        ]
      ]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    # Route to different batchers based on message type
    case message.data.payload do
      "db:" <> _ -> Broadway.Message.put_batcher(message, :database)
      _ -> message
    end
  end

  @impl true
  def handle_batch(:database, messages, _batch_info, _context) do
    # Process batch of messages
    IO.puts("Writing #{length(messages)} messages to database")
    messages
  end
end
```

## Advanced Features

### Consumer Lag Monitoring

```elixir
{:ok, lag_map} = Consumer.lag(consumer)
total_lag = lag_map |> Map.values() |> Enum.sum()
IO.puts("Total consumer lag: #{total_lag} messages")
```

### Batch Processing

```elixir
# Receive batch of messages
{:ok, messages} = Consumer.receive_batch(consumer,
  max_batch_size: 100,
  min_batch_size: 10,
  timeout: 5000
)

# Process batch...

# Commit batch
:ok = Consumer.commit_batch(consumer, messages)
```

### Pause/Resume Partitions

```elixir
# Pause consumption
:ok = Consumer.pause(consumer, [{"events", 0}, {"events", 1}])

# Resume consumption
:ok = Consumer.resume(consumer, [{"events", 0}, {"events", 1}])
```

### Seek to Offset

```elixir
# Seek to specific offset
:ok = Consumer.seek(consumer, [{"events", 0, 1000}])

# Seek to beginning
:ok = Consumer.seek_to_beginning(consumer, [{"events", 0}])

# Seek to end
:ok = Consumer.seek_to_end(consumer, [{"events", 0}])
```

## Configuration

Franz provides both traditional keyword list configuration and fluent builder-style APIs.

### Producer Configuration

**Keyword list style:**
```elixir
Producer.Config.new(
  bootstrap_servers: "localhost:9092",
  acks: :all,
  compression_type: :lz4,
  linger_ms: 10,
  batch_size: 1_000_000,
  max_in_flight: 10
)
```

**Fluent builder style:**
```elixir
alias Franz.Producer.Config

Config.new()
|> Config.bootstrap_servers("localhost:9092")
|> Config.acks(:leader)                    # :none | :leader | :all
|> Config.compression_type(:lz4)           # :none | :gzip | :snappy | :lz4 | :zstd
|> Config.linger_ms(10)                    # Batch delay in ms
|> Config.batch_size(1_000_000)            # Max batch size in bytes
|> Config.max_in_flight(10)                # Max unacknowledged requests
```

### Consumer Configuration

**Keyword list style:**
```elixir
Consumer.Config.new(
  group_id: "my-app",
  bootstrap_servers: "localhost:9092",
  auto_offset_reset: :earliest,
  enable_auto_commit: false
)
```

**Fluent builder style:**
```elixir
alias Franz.Consumer.Config

Config.new()
|> Config.group_id("my-app")
|> Config.bootstrap_servers("localhost:9092")
|> Config.auto_offset_reset(:earliest)     # :earliest | :latest | :error
|> Config.enable_auto_commit(false)
|> Config.topics(["events", "notifications"])
```

## Telemetry

Franz emits telemetry events for monitoring:

### Producer Events

- `[:franz, :producer, :send]` - Synchronous send completion
- `[:franz, :producer, :send_async]` - Async send initiated
- `[:franz, :producer, :flush]` - Flush completion

### Consumer Events

- `[:franz, :consumer, :lag]` - Lag monitoring
- `[:franz, :consumer, :receive_batch]` - Batch reception

Example handler:

```elixir
:telemetry.attach(
  "franz-metrics",
  [:franz, :producer, :send],
  fn event, measurements, metadata, _config ->
    Logger.info("Message sent: #{inspect(measurements)}")
  end,
  nil
)
```

## Performance Tips

1. **Use async sends** for high throughput:
   ```elixir
   Producer.send_async(producer, message)
   ```

2. **Enable compression** to reduce network bandwidth:
   ```elixir
   Producer.Config.new(compression_type: :lz4)
   ```

3. **Tune batching** for your workload:
   ```elixir
   Producer.Config.new(linger_ms: 10, batch_size: 1_000_000)
   ```

4. **Use Broadway** for concurrent processing:
   ```elixir
   processors: [default: [concurrency: 50]]
   ```

5. **Monitor lag** to detect backpressure:
   ```elixir
   {:ok, lag} = Consumer.lag(consumer)
   ```

## Testing

```bash
# Start Kafka
docker compose up -d

# Run tests
mix test

# Run with performance tests
mix test --include performance
```

## Examples

See the `examples/` directory for complete working examples:

- `examples/broadway_pipeline.exs` - Broadway data processing pipeline
- `examples/produce_events.exs` - Event production example

## Architecture

Franz uses a hybrid Elixir/Rust architecture:

- **Elixir Layer**: Public API, OTP integration, process coordination
- **Rust NIF Layer**: High-performance Kafka operations via rdkafka
- **Async I/O**: Tokio runtime for concurrent message handling
- **Zero-copy**: Efficient message passing between Rust and Elixir

## License

Copyright (c) 2025 Sonny Scroggin

Licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Links

- [Documentation](https://hexdocs.pm/franz)
- [librdkafka](https://github.com/edenhill/librdkafka)
- [rdkafka](https://github.com/fede1024/rust-rdkafka)
- [Broadway](https://github.com/dashbitco/broadway)

[librdkafka]: https://github.com/edenhill/librdkafka
[rdkafka]: https://github.com/fede1024/rust-rdkafka
