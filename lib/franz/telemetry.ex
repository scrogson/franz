defmodule Franz.Telemetry do
  @moduledoc """
  Telemetry events emitted by Franz.

  Franz emits telemetry events for monitoring, metrics, and observability.
  All events include measurements (durations, counts) and metadata (topics, partitions, etc.).

  ## Event Naming Convention

  Events follow the pattern: `[:franz, component, operation]` or `[:franz, component, operation, :error]`

  ## Producer Events

  ### `[:franz, :producer, :start]`

  Emitted when a producer is started.

  **Measurements:**
  - `:duration` - Time taken to start the producer (native time units)

  **Metadata:**
  - `:bootstrap_servers` - Kafka bootstrap servers
  - `:security_enabled` - Boolean indicating if security is configured

  ### `[:franz, :producer, :start, :error]`

  Emitted when producer start fails.

  **Measurements:**
  - `:duration` - Time taken before failure (native time units)

  **Metadata:**
  - `:bootstrap_servers` - Kafka bootstrap servers
  - `:security_enabled` - Boolean indicating if security is configured
  - `:error` - The error that occurred

  ### `[:franz, :producer, :send]`

  Emitted when a message is successfully sent.

  **Measurements:**
  - `:duration` - Time taken to send the message (native time units)

  **Metadata:**
  - `:topic` - Topic name (requested)
  - `:partition` - Partition number (requested)
  - `:has_key` - Boolean indicating if message has a key
  - `:has_payload` - Boolean indicating if message has a payload
  - `:headers_count` - Number of headers
  - `:delivered_partition` - Actual partition where message was written
  - `:delivered_offset` - Offset assigned to the message in Kafka

  ### `[:franz, :producer, :send, :error]`

  Emitted when message send fails.

  **Measurements:**
  - `:duration` - Time taken before failure (native time units)

  **Metadata:**
  - `:topic` - Topic name
  - `:partition` - Partition number
  - `:has_key` - Boolean indicating if message has a key
  - `:has_payload` - Boolean indicating if message has a payload
  - `:headers_count` - Number of headers
  - `:error` - The error that occurred

  ### `[:franz, :producer, :flush]`

  Emitted when producer flush completes successfully.

  **Measurements:**
  - `:duration` - Time taken to flush (native time units)

  **Metadata:**
  - `:timeout_ms` - Configured timeout in milliseconds

  ### `[:franz, :producer, :flush, :error]`

  Emitted when producer flush fails.

  **Measurements:**
  - `:duration` - Time taken before failure (native time units)

  **Metadata:**
  - `:timeout_ms` - Configured timeout in milliseconds
  - `:error` - The error that occurred

  ## Consumer Events

  ### `[:franz, :consumer, :start]`

  Emitted when a consumer is started.

  **Measurements:**
  - `:duration` - Time taken to start the consumer (native time units)

  **Metadata:**
  - `:group_id` - Consumer group ID
  - `:bootstrap_servers` - Kafka bootstrap servers
  - `:auto_offset_reset` - Auto offset reset strategy
  - `:security_enabled` - Boolean indicating if security is configured

  ### `[:franz, :consumer, :subscribe]`

  Emitted when consumer subscribes to topics.

  **Measurements:**
  - `:duration` - Time taken to subscribe (native time units)

  **Metadata:**
  - `:topics` - List of topic names
  - `:topic_count` - Number of topics

  ### `[:franz, :consumer, :subscribe, :error]`

  Emitted when consumer subscribe fails.

  **Measurements:**
  - `:duration` - Time taken before failure (native time units)

  **Metadata:**
  - `:topics` - List of topic names
  - `:topic_count` - Number of topics
  - `:error` - The error that occurred

  ### `[:franz, :consumer, :message]`

  Emitted when a message is received from Kafka.

  **Measurements:**
  - (none - this is an event counter)

  **Metadata:**
  - `:topic` - Topic name
  - `:partition` - Partition number
  - `:offset` - Message offset
  - `:has_key` - Boolean indicating if message has a key
  - `:has_payload` - Boolean indicating if message has a payload
  - `:headers_count` - Number of headers

  ### `[:franz, :consumer, :commit]`

  Emitted when an offset is successfully committed.

  **Measurements:**
  - `:duration` - Time taken to commit (native time units)

  **Metadata:**
  - `:topic` - Topic name
  - `:partition` - Partition number
  - `:offset` - Committed offset

  ### `[:franz, :consumer, :commit, :error]`

  Emitted when offset commit fails.

  **Measurements:**
  - `:duration` - Time taken before failure (native time units)

  **Metadata:**
  - `:topic` - Topic name
  - `:partition` - Partition number
  - `:offset` - Attempted offset
  - `:error` - The error that occurred

  ### `[:franz, :consumer, :rebalance, :pre]`

  Emitted before a consumer group rebalance begins.

  **Measurements:**
  - (none - this is an event counter)

  **Metadata:**
  - (none)

  ### `[:franz, :consumer, :rebalance, :post]`

  Emitted after a consumer group rebalance completes.

  **Measurements:**
  - (none - this is an event counter)

  **Metadata:**
  - `:partition_count` - Number of partitions assigned (when applicable)

  ## Admin Events

  ### `[:franz, :admin, :create_topic]`

  Emitted when a topic is successfully created.

  **Measurements:**
  - `:duration` - Time taken to create the topic (native time units)

  **Metadata:**
  - `:topic` - Topic name
  - `:num_partitions` - Number of partitions
  - `:replication` - Replication factor
  - `:bootstrap_servers` - Kafka bootstrap servers

  ### `[:franz, :admin, :create_topic, :error]`

  Emitted when topic creation fails.

  **Measurements:**
  - `:duration` - Time taken before failure (native time units)

  **Metadata:**
  - `:topic` - Topic name
  - `:num_partitions` - Number of partitions
  - `:replication` - Replication factor
  - `:bootstrap_servers` - Kafka bootstrap servers
  - `:error` - The error that occurred

  ### `[:franz, :admin, :delete_topic]`

  Emitted when a topic is successfully deleted.

  **Measurements:**
  - `:duration` - Time taken to delete the topic (native time units)

  **Metadata:**
  - `:topic` - Topic name
  - `:bootstrap_servers` - Kafka bootstrap servers

  ### `[:franz, :admin, :delete_topic, :error]`

  Emitted when topic deletion fails.

  **Measurements:**
  - `:duration` - Time taken before failure (native time units)

  **Metadata:**
  - `:topic` - Topic name
  - `:bootstrap_servers` - Kafka bootstrap servers
  - `:error` - The error that occurred

  ## Example: Attaching Handlers

  To observe Franz telemetry events, attach handlers using `:telemetry.attach/4` or `:telemetry.attach_many/4`.

  ### Logging Example

      :telemetry.attach_many(
        "franz-logger",
        [
          [:franz, :producer, :send],
          [:franz, :consumer, :message],
          [:franz, :consumer, :commit]
        ],
        fn event_name, measurements, metadata, _config ->
          duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)
          IO.inspect({event_name, duration_ms, metadata})
        end,
        nil
      )

  ### Prometheus/StatsD Integration Example

      defmodule MyApp.FranzTelemetry do
        def setup do
          events = [
            [:franz, :producer, :send],
            [:franz, :producer, :send, :error],
            [:franz, :consumer, :message],
            [:franz, :consumer, :commit],
            [:franz, :consumer, :commit, :error]
          ]

          :telemetry.attach_many(
            "my-app-franz-metrics",
            events,
            &handle_event/4,
            nil
          )
        end

        def handle_event([:franz, :producer, :send], measurements, metadata, _config) do
          # Increment counter
          :telemetry.execute([:my_app, :kafka, :messages, :sent], %{count: 1})

          # Record latency histogram
          duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)
          :telemetry.execute([:my_app, :kafka, :latency], %{duration: duration_ms}, %{
            operation: "send",
            topic: metadata.topic
          })
        end

        def handle_event([:franz, :consumer, :message], _measurements, metadata, _config) do
          # Increment counter per topic/partition
          :telemetry.execute([:my_app, :kafka, :messages, :received], %{count: 1}, %{
            topic: metadata.topic,
            partition: metadata.partition
          })
        end

        def handle_event([:franz, :producer, :send, :error], measurements, metadata, _config) do
          # Increment error counter
          :telemetry.execute([:my_app, :kafka, :errors], %{count: 1}, %{
            operation: "producer_send",
            topic: metadata.topic,
            error_type: metadata.error.type
          })
        end

        def handle_event(_event, _measurements, _metadata, _config), do: :ok
      end

  ### TelemetryMetrics Integration Example

  Franz works seamlessly with [Telemetry.Metrics](https://hexdocs.pm/telemetry_metrics):

      defmodule MyApp.Telemetry do
        import Telemetry.Metrics

        def metrics do
          [
            # Producer metrics
            counter("franz.producer.send.count"),
            counter("franz.producer.send.error.count"),
            distribution("franz.producer.send.duration",
              unit: {:native, :millisecond},
              tags: [:topic, :partition]
            ),

            # Consumer metrics
            counter("franz.consumer.message.count", tags: [:topic, :partition]),
            counter("franz.consumer.commit.count"),
            counter("franz.consumer.commit.error.count"),
            distribution("franz.consumer.commit.duration",
              unit: {:native, :millisecond}
            ),

            # Rebalance metrics
            counter("franz.consumer.rebalance.pre.count"),
            counter("franz.consumer.rebalance.post.count"),

            # Admin metrics
            distribution("franz.admin.create_topic.duration",
              unit: {:native, :millisecond}
            ),
            distribution("franz.admin.delete_topic.duration",
              unit: {:native, :millisecond}
            )
          ]
        end
      end
  """
end
