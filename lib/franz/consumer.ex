defmodule Franz.Consumer do
  defstruct ref: nil, channel: nil

  alias Franz.{Consumer, Error, Message, Native}
  alias Consumer.Config
  require Logger

  @type t :: %Consumer{
          ref: reference(),
          channel: reference()
        }

  @doc """
  Start a Kafka consumer with the provided configuration.
  """
  @spec start(Config.t()) :: {:ok, Consumer.t()} | {:error, Error.t()}
  def start(config) do
    start_time = System.monotonic_time()

    # Start the streaming task - it returns a channel sender reference
    channel_sender = Native.consumer_stream(config)
    result = {:ok, %Consumer{ref: channel_sender, channel: channel_sender}}

    duration = System.monotonic_time() - start_time

    :telemetry.execute(
      [:franz, :consumer, :start],
      %{duration: duration},
      %{
        group_id: config.group_id,
        bootstrap_servers: config.bootstrap_servers,
        auto_offset_reset: config.auto_offset_reset,
        security_enabled: not is_nil(config.security)
      }
    )

    result
  end

  @doc """
  Subscribe to a list of topics.
  """
  @spec subscribe(Consumer.t(), [String.t()]) :: {:ok, Consumer.t()} | {:error, Error.t()}
  def subscribe(%Consumer{channel: channel} = consumer, topics) when is_list(topics) do
    start_time = System.monotonic_time()
    :ok = Native.consumer_subscribe(channel, topics)

    # Wait for response from the streaming task - messages are tagged with channel ref
    result =
      receive do
        {^channel, :ok} ->
          {:ok, consumer}

        {^channel, {:error, %{reason: reason}}} ->
          Error.wrap({:error, reason})
      after
        10_000 ->
          Error.wrap({:error, :timeout})
      end

    duration = System.monotonic_time() - start_time

    metadata = %{topics: topics, topic_count: length(topics)}

    case result do
      {:ok, _} ->
        :telemetry.execute([:franz, :consumer, :subscribe], %{duration: duration}, metadata)

      {:error, error} ->
        :telemetry.execute(
          [:franz, :consumer, :subscribe, :error],
          %{duration: duration},
          Map.put(metadata, :error, error)
        )
    end

    result
  end

  @doc """
  Get current partition assignments.
  """
  @spec assignment(Consumer.t()) :: {:ok, list()} | {:error, Error.t()}
  def assignment(%Consumer{channel: channel}) do
    :ok = Native.consumer_assignment(channel)

    receive do
      {^channel, {:assignments, %{assignments: assignments}}} ->
        {:ok, assignments}

      {^channel, {:error, %{reason: reason}}} ->
        Error.wrap({:error, reason})

      other ->
        Error.wrap({:error, {:unexpected_message, other}})
    after
      10_000 ->
        Error.wrap({:error, :timeout})
    end
  end

  @doc """
  Unsubscribe from the current subscribed topics.
  """
  @spec unsubscribe(Consumer.t()) :: {:ok, Consumer.t()} | {:error, Error.t()}
  def unsubscribe(%Consumer{channel: channel} = consumer) do
    :ok = Native.consumer_unsubscribe(channel)

    receive do
      {^channel, :ok} ->
        {:ok, consumer}

      {^channel, {:error, %{reason: reason}}} ->
        Error.wrap({:error, reason})
    after
      10_000 ->
        Error.wrap({:error, :timeout})
    end
  end

  @spec receive_assignments(Consumer.t()) :: {:ok, list(), Consumer.t()} | {:error, Error.t()}
  def receive_assignments(%Consumer{channel: channel} = consumer) do
    receive do
      {^channel, {:pre_rebalance, _}} ->
        receive_assignments(consumer)

      {^channel, {:post_rebalance, %{action: {:assign, %{partitions: assignments}}}}} ->
        {:ok, assignments, consumer}
    after
      100 ->
        receive_assignments(consumer)
    end
  end

  @doc """
  Commit a topic partition offset.
  """
  @spec commit(Consumer.t(), Message.t()) :: :ok | {:error, Error.t()}
  def commit(%Consumer{channel: channel}, %Message{} = msg) do
    %Message{topic: topic, partition: partition, offset: offset} = msg
    start_time = System.monotonic_time()
    :ok = Native.consumer_commit(channel, {topic, partition, offset})

    result =
      receive do
        {^channel, :ok} -> :ok
        {^channel, {:error, %{reason: reason}}} -> Error.wrap({:error, reason})
      after
        5_000 ->
          Error.wrap({:error, :timeout})
      end

    duration = System.monotonic_time() - start_time

    metadata = %{
      topic: topic,
      partition: partition,
      offset: offset
    }

    case result do
      :ok ->
        :telemetry.execute([:franz, :consumer, :commit], %{duration: duration}, metadata)

      {:error, error} ->
        :telemetry.execute(
          [:franz, :consumer, :commit, :error],
          %{duration: duration},
          Map.put(metadata, :error, error)
        )
    end

    result
  end

  @doc """
  Retrieve committed offsets for topics and partitions.
  """
  @spec committed(Consumer.t(), number()) :: {:ok, list()} | {:error, Error.t()}
  def committed(%Consumer{channel: channel}, timeout \\ 100) do
    :ok = Native.consumer_committed(channel, timeout)

    receive do
      {^channel, {:committed, %{offsets: offsets}}} ->
        {:ok, offsets}

      {^channel, {:error, %{reason: reason}}} ->
        Error.wrap({:error, reason})
    after
      # Add buffer time to the rdkafka timeout for the Elixir receive
      timeout + 5_000 ->
        Error.wrap({:error, :timeout})
    end
  end

  @doc """
  Pause consumption from specific partitions.

  Pauses the consumer from fetching new messages from the specified partitions.
  Useful for backpressure handling or selective consumption.

  ## Example

      partitions = [{"my-topic", 0}, {"my-topic", 1}]
      :ok = Consumer.pause(consumer, partitions)
  """
  @spec pause(Consumer.t(), [{String.t(), integer()}]) :: :ok | {:error, Error.t()}
  def pause(%Consumer{channel: channel}, partitions) when is_list(partitions) do
    start_time = System.monotonic_time()
    :ok = Native.consumer_pause(channel, partitions)

    result =
      receive do
        {^channel, :ok} -> :ok
        {^channel, {:error, %{reason: reason}}} -> Error.wrap({:error, reason})
      after
        5_000 ->
          Error.wrap({:error, :timeout})
      end

    duration = System.monotonic_time() - start_time

    metadata = %{partition_count: length(partitions)}

    case result do
      :ok ->
        :telemetry.execute([:franz, :consumer, :pause], %{duration: duration}, metadata)

      {:error, error} ->
        :telemetry.execute(
          [:franz, :consumer, :pause, :error],
          %{duration: duration},
          Map.put(metadata, :error, error)
        )
    end

    result
  end

  @doc """
  Resume consumption from paused partitions.

  Resumes the consumer from fetching messages from previously paused partitions.

  ## Example

      partitions = [{"my-topic", 0}, {"my-topic", 1}]
      :ok = Consumer.resume(consumer, partitions)
  """
  @spec resume(Consumer.t(), [{String.t(), integer()}]) :: :ok | {:error, Error.t()}
  def resume(%Consumer{channel: channel}, partitions) when is_list(partitions) do
    start_time = System.monotonic_time()
    :ok = Native.consumer_resume(channel, partitions)

    result =
      receive do
        {^channel, :ok} -> :ok
        {^channel, {:error, %{reason: reason}}} -> Error.wrap({:error, reason})
      after
        5_000 ->
          Error.wrap({:error, :timeout})
      end

    duration = System.monotonic_time() - start_time

    metadata = %{partition_count: length(partitions)}

    case result do
      :ok ->
        :telemetry.execute([:franz, :consumer, :resume], %{duration: duration}, metadata)

      {:error, error} ->
        :telemetry.execute(
          [:franz, :consumer, :resume, :error],
          %{duration: duration},
          Map.put(metadata, :error, error)
        )
    end

    result
  end

  @doc """
  Seek to a specific offset in a partition.

  Moves the consumer's position to the specified offset. The next message fetched
  will be from this offset.

  ## Offset Types

  - `:beginning` - Seek to the beginning of the partition
  - `:end` - Seek to the end of the partition (next new message)
  - `{:offset, n}` - Seek to specific offset `n`

  ## Example

      # Seek to beginning
      :ok = Consumer.seek(consumer, "my-topic", 0, :beginning)

      # Seek to specific offset
      :ok = Consumer.seek(consumer, "my-topic", 0, {:offset, 1000})

      # Seek to end
      :ok = Consumer.seek(consumer, "my-topic", 0, :end)
  """
  @spec seek(Consumer.t(), String.t(), integer(), :beginning | :end | {:offset, integer()}) ::
          :ok | {:error, Error.t()}
  def seek(%Consumer{channel: channel}, topic, partition, offset)
      when is_binary(topic) and is_integer(partition) do
    start_time = System.monotonic_time()
    :ok = Native.consumer_seek(channel, topic, partition, offset)

    result =
      receive do
        {^channel, :ok} -> :ok
        {^channel, {:error, %{reason: reason}}} -> Error.wrap({:error, reason})
      after
        10_000 ->
          Error.wrap({:error, :timeout})
      end

    duration = System.monotonic_time() - start_time

    metadata = %{
      topic: topic,
      partition: partition,
      offset: offset
    }

    case result do
      :ok ->
        :telemetry.execute([:franz, :consumer, :seek], %{duration: duration}, metadata)

      {:error, error} ->
        :telemetry.execute(
          [:franz, :consumer, :seek, :error],
          %{duration: duration},
          Map.put(metadata, :error, error)
        )
    end

    result
  end

  @doc """
  Get the current position (offset) for assigned partitions.

  Returns the current offset for each partition that the consumer is reading from.
  This is the offset of the *next* message that will be consumed.

  ## Example

      {:ok, positions} = Consumer.position(consumer)
      # => {:ok, [{"my-topic", 0, {:offset, 1234}}, {"my-topic", 1, {:offset, 5678}}]}
  """
  @spec position(Consumer.t()) :: {:ok, list()} | {:error, Error.t()}
  def position(%Consumer{channel: channel}) do
    start_time = System.monotonic_time()
    :ok = Native.consumer_position(channel, [])

    result =
      receive do
        {^channel, {:position, %{positions: positions}}} ->
          {:ok, positions}

        {^channel, {:error, %{reason: reason}}} ->
          Error.wrap({:error, reason})
      after
        5_000 ->
          Error.wrap({:error, :timeout})
      end

    duration = System.monotonic_time() - start_time

    case result do
      {:ok, positions} ->
        :telemetry.execute(
          [:franz, :consumer, :position],
          %{duration: duration},
          %{partition_count: length(positions)}
        )

      {:error, error} ->
        :telemetry.execute(
          [:franz, :consumer, :position, :error],
          %{duration: duration},
          %{error: error}
        )
    end

    result
  end

  @doc """
  Fetch the low and high water marks for a specific partition.

  Water marks indicate the range of available offsets:
  - Low water mark: oldest available offset
  - High water mark: next offset to be written (latest offset + 1)

  ## Example

      {:ok, {low, high}} = Consumer.watermarks(consumer, "my-topic", 0)
      # => {:ok, {0, 1234}}
  """
  @spec watermarks(Consumer.t(), String.t(), integer(), timeout :: non_neg_integer()) ::
          {:ok, {low :: integer(), high :: integer()}} | {:error, Error.t()}
  def watermarks(%Consumer{channel: channel}, topic, partition, timeout \\ 5000)
      when is_binary(topic) and is_integer(partition) do
    :ok = Native.consumer_watermarks(channel, topic, partition, timeout)

    receive do
      {^channel, {:watermarks, %{low: low, high: high}}} ->
        {:ok, {low, high}}

      {^channel, {:error, %{reason: reason}}} ->
        Error.wrap({:error, reason})
    after
      timeout + 1_000 ->
        Error.wrap({:error, :timeout})
    end
  end

  @doc """
  Calculate consumer lag for all assigned partitions.

  Lag is the difference between the high water mark (latest available offset)
  and the consumer's last committed offset. Returns lag per partition.

  This is essential for monitoring consumer health and identifying backlog issues.

  ## Returns

  A map of `{topic, partition}` => `lag` where lag is the number of messages
  behind the high water mark based on committed offsets.

  ## Example

      {:ok, lag_map} = Consumer.lag(consumer)
      # => {:ok, %{
      #      {"my-topic", 0} => 1234,  # 1234 messages behind
      #      {"my-topic", 1} => 0,      # caught up
      #      {"my-topic", 2} => 5678    # 5678 messages behind
      #    }}

      # Total lag across all partitions
      total_lag = lag_map |> Map.values() |> Enum.sum()
  """
  @spec lag(Consumer.t(), timeout :: non_neg_integer()) ::
          {:ok, %{optional({String.t(), integer()}) => integer()}} | {:error, Error.t()}
  def lag(%Consumer{} = consumer, timeout \\ 5000) do
    start_time = System.monotonic_time()

    # Use committed offsets instead of position for accurate lag calculation
    # Position can be ahead due to prefetching
    with {:ok, committed_offsets} <- committed(consumer, timeout),
         {:ok, lag_map} <- calculate_lag_from_committed(consumer, committed_offsets, timeout) do
      duration = System.monotonic_time() - start_time

      # Emit telemetry with lag metrics
      total_lag = lag_map |> Map.values() |> Enum.sum()
      max_lag = lag_map |> Map.values() |> Enum.max(fn -> 0 end)

      :telemetry.execute(
        [:franz, :consumer, :lag],
        %{duration: duration},
        %{
          total_lag: total_lag,
          max_lag: max_lag,
          partition_count: map_size(lag_map)
        }
      )

      {:ok, lag_map}
    end
  end

  defp calculate_lag_from_committed(consumer, committed_offsets, timeout) do
    # Fetch watermarks for each partition and calculate lag based on committed offsets
    # Skip partitions with invalid offsets (not yet consumed/committed)
    results =
      Enum.flat_map(committed_offsets, fn
        {topic, partition, {:offset, committed_offset}} ->
          case watermarks(consumer, topic, partition, timeout) do
            {:ok, {_low, high}} ->
              # Lag = high water mark - (committed offset + 1)
              # The +1 is because committed offset is the last processed message,
              # and we want to know how many messages are after that
              lag = max(0, high - (committed_offset + 1))
              [{{topic, partition}, lag}]

            {:error, _} = error ->
              [error]
          end

        {_topic, _partition, :invalid} ->
          # Skip partitions with invalid offsets (not yet committed)
          []
      end)

    # Check if any errors occurred
    case Enum.find(results, &match?({:error, _}, &1)) do
      nil -> {:ok, Map.new(results)}
      error -> error
    end
  end

  @doc """
  Receive a batch of messages from the consumer.

  This is more efficient than receiving messages one at a time, as it allows you to:
  - Process multiple messages together
  - Amortize the cost of commits over multiple messages
  - Implement batched processing with backpressure

  ## Options

  - `:max_batch_size` - Maximum number of messages to collect (required)
  - `:timeout` - Maximum time to wait for the batch in milliseconds (default: 5000)
  - `:min_batch_size` - Minimum batch size before returning early (default: 1)

  ## Returns

  `{:ok, messages}` where `messages` is a list of `Franz.Message` structs.
  Returns an empty list if no messages are available within the timeout.

  ## Example

      # Collect up to 100 messages, wait max 1 second
      {:ok, batch} = Consumer.receive_batch(consumer,
        max_batch_size: 100,
        timeout: 1000,
        min_batch_size: 10
      )

      # Process batch
      Enum.each(batch, fn msg ->
        process_message(msg)
      end)

      # Commit the last message in batch
      if last_msg = List.last(batch) do
        Consumer.commit(consumer, last_msg)
      end
  """
  @spec receive_batch(Consumer.t(), keyword()) :: {:ok, [Message.t()]}
  def receive_batch(%Consumer{channel: channel}, opts) do
    max_batch_size = Keyword.fetch!(opts, :max_batch_size)
    timeout = Keyword.get(opts, :timeout, 5000)
    min_batch_size = Keyword.get(opts, :min_batch_size, 1)

    start_time = System.monotonic_time(:millisecond)
    end_time = start_time + timeout

    messages = collect_batch(channel, max_batch_size, min_batch_size, end_time, [])

    duration = System.monotonic_time(:millisecond) - start_time

    :telemetry.execute(
      [:franz, :consumer, :receive_batch],
      %{duration: duration},
      %{
        batch_size: length(messages),
        max_batch_size: max_batch_size,
        min_batch_size: min_batch_size
      }
    )

    {:ok, messages}
  end

  defp collect_batch(channel, max_remaining, min_batch_size, end_time, acc)
       when max_remaining > 0 do
    current_time = System.monotonic_time(:millisecond)
    remaining_timeout = max(0, end_time - current_time)

    # If we have enough messages and timeout is approaching, return early
    batch_size = length(acc)

    if batch_size >= min_batch_size and remaining_timeout < 100 do
      Enum.reverse(acc)
    else
      receive do
        {^channel, {:message, %{msg: msg}}} ->
          new_acc = [msg | acc]

          # If we've hit max batch size, return immediately
          if length(new_acc) >= max_remaining + length(acc) do
            Enum.reverse(new_acc)
          else
            collect_batch(channel, max_remaining - 1, min_batch_size, end_time, new_acc)
          end
      after
        remaining_timeout ->
          # Timeout - return what we have
          Enum.reverse(acc)
      end
    end
  end

  defp collect_batch(_channel, _max_remaining, _min_batch_size, _end_time, acc) do
    # Max batch size reached
    Enum.reverse(acc)
  end

  @doc """
  Commit the last offset in a batch of messages.

  This is a convenience function for batch processing that commits the offset
  of the last message, implicitly committing all previous messages in the batch.

  ## Example

      {:ok, batch} = Consumer.receive_batch(consumer, max_batch_size: 100)

      # Process all messages
      results = Enum.map(batch, &process_message/1)

      # Commit the entire batch
      :ok = Consumer.commit_batch(consumer, batch)
  """
  @spec commit_batch(Consumer.t(), [Message.t()]) :: :ok | {:error, Error.t()}
  def commit_batch(_consumer, []), do: :ok

  def commit_batch(consumer, messages) when is_list(messages) do
    last_msg = List.last(messages)
    commit(consumer, last_msg)
  end

  @doc """
  Stop a Kafka consumer.
  The task will automatically stop when the channel is dropped or the process exits.
  """
  @spec stop(Consumer.t()) :: :ok | {:error, Error.t()}
  def stop(%Consumer{}) do
    # With the task-based approach, the consumer task will automatically stop
    # when the channel is dropped or the process exits
    :ok
  end

  @doc """
  Handle incoming messages from the consumer.
  This is a helper to pattern match on different event types.

  Emits telemetry events for messages and rebalances.
  """
  def handle_event(event) do
    result =
      case event do
        {:message, %{msg: msg}} ->
          {:message, msg}

        {:pre_rebalance, %{action: action}} ->
          {:pre_rebalance, action}

        {:post_rebalance, %{action: action}} ->
          {:post_rebalance, action}

        {:error, %{reason: reason}} ->
          {:error, reason}

        other ->
          {:unknown, other}
      end

    # Emit telemetry for different event types
    case result do
      {:message, msg} ->
        :telemetry.execute(
          [:franz, :consumer, :message],
          %{},
          %{
            topic: msg.topic,
            partition: msg.partition,
            offset: msg.offset,
            has_key: not is_nil(msg.key),
            has_payload: not is_nil(msg.payload),
            headers_count: length(msg.headers)
          }
        )

      {:pre_rebalance, _action} ->
        :telemetry.execute([:franz, :consumer, :rebalance, :pre], %{}, %{})

      {:post_rebalance, action} ->
        metadata =
          case action do
            {:assign, %{partitions: partitions}} ->
              %{partition_count: length(partitions)}

            _ ->
              %{}
          end

        :telemetry.execute([:franz, :consumer, :rebalance, :post], %{}, metadata)

      _ ->
        :ok
    end

    result
  end
end
