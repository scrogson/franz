defmodule Franz.Producer do
  alias Franz.{Error, Message, Native, Producer}
  alias Producer.Config

  defstruct ref: nil

  @type t :: %Producer{
          ref: reference()
        }

  @spec start(Config.t()) :: {:ok, Producer.t()} | {:error, Error.t()}
  def start(config) do
    start_time = System.monotonic_time()

    result =
      case Native.producer_start(config) do
        {:ok, ref} ->
          {:ok, %Producer{ref: ref}}

        {:error, error} ->
          Error.wrap({:error, error})
      end

    duration = System.monotonic_time() - start_time

    metadata = %{
      bootstrap_servers: config.bootstrap_servers,
      security_enabled: not is_nil(config.security)
    }

    case result do
      {:ok, _} ->
        :telemetry.execute([:franz, :producer, :start], %{duration: duration}, metadata)

      {:error, error} ->
        :telemetry.execute(
          [:franz, :producer, :start, :error],
          %{duration: duration},
          Map.put(metadata, :error, error)
        )
    end

    result
  end

  @doc """
  Send a message to Kafka and wait for delivery confirmation.

  This is the safest option but has lower throughput due to waiting for acks.
  For high-throughput scenarios, use `send_async/2` instead.
  """
  @spec send(Producer.t(), Message.t()) :: :ok | {:error, Error.t()}
  def send(%Producer{ref: ref}, %Message{} = msg) do
    start_time = System.monotonic_time()
    task_ref = Native.producer_send(ref, msg)

    result =
      receive do
        {^task_ref, {:ok, _}} ->
          :ok

        {^task_ref, {:error, reason}} ->
          Error.wrap({:error, reason})
      after
        10_000 ->
          Error.wrap({:error, :timeout})
      end

    duration = System.monotonic_time() - start_time

    metadata = %{
      topic: msg.topic,
      partition: msg.partition,
      has_key: not is_nil(msg.key),
      has_payload: not is_nil(msg.payload),
      headers_count: length(msg.headers)
    }

    case result do
      :ok ->
        :telemetry.execute([:franz, :producer, :send], %{duration: duration}, metadata)

      {:error, error} ->
        :telemetry.execute(
          [:franz, :producer, :send, :error],
          %{duration: duration},
          Map.put(metadata, :error, error)
        )
    end

    result
  end

  @doc """
  Send a message asynchronously without waiting for delivery confirmation.

  This provides maximum throughput by enqueueing messages without blocking.
  Messages are buffered according to `linger_ms` and `batch_size` configuration.

  **Important:** This does NOT guarantee delivery. Use `flush/1` to ensure
  all messages are sent, and configure `acks` appropriately in Producer.Config.

  ## Examples

      # High-throughput fire-and-forget
      config = Producer.Config.new(
        bootstrap_servers: "localhost:9092",
        acks: :none,
        compression_type: :lz4,
        linger_ms: 10
      )

      {:ok, producer} = Producer.start(config)

      # Send 1M messages without waiting
      for i <- 1..1_000_000 do
        :ok = Producer.send_async(producer, %Message{
          topic: "events",
          payload: "message-\#{i}"
        })
      end

      # Ensure all messages are delivered
      :ok = Producer.flush(producer)
  """
  @spec send_async(Producer.t(), Message.t()) :: :ok | {:error, Error.t()}
  def send_async(%Producer{ref: ref}, %Message{} = msg) do
    start_time = System.monotonic_time()
    task_ref = Native.producer_send_async(ref, msg)

    result =
      receive do
        {^task_ref, {:ok, _}} ->
          :ok

        {^task_ref, {:error, reason}} ->
          Error.wrap({:error, reason})
      after
        100 ->
          # Short timeout since the task returns immediately after spawning
          Error.wrap({:error, :timeout})
      end

    duration = System.monotonic_time() - start_time

    metadata = %{
      topic: msg.topic,
      partition: msg.partition,
      has_key: not is_nil(msg.key),
      has_payload: not is_nil(msg.payload),
      headers_count: length(msg.headers)
    }

    case result do
      :ok ->
        :telemetry.execute([:franz, :producer, :send_async], %{duration: duration}, metadata)

      {:error, error} ->
        :telemetry.execute(
          [:franz, :producer, :send_async, :error],
          %{duration: duration},
          Map.put(metadata, :error, error)
        )
    end

    result
  end

  @doc """
  Get the number of messages waiting to be sent or acknowledged.

  Useful for monitoring producer queue depth and implementing backpressure.

  ## Example

      count = Producer.in_flight_count(producer)
      if count > 10_000 do
        # Too many pending messages, slow down
        Process.sleep(10)
      end
  """
  @spec in_flight_count(Producer.t()) :: non_neg_integer()
  def in_flight_count(%Producer{ref: ref}) do
    Native.producer_in_flight_count(ref)
  end

  @doc """
  Flush all pending messages from the producer.

  This blocks until all outstanding messages are delivered or the timeout expires.
  Should be called before stopping the producer to ensure all messages are sent.

  ## Options
    * `timeout_ms` - Maximum time to wait in milliseconds (default: 10000)
  """
  @spec flush(Producer.t(), non_neg_integer()) :: :ok | {:error, Error.t()}
  def flush(%Producer{ref: ref}, timeout_ms \\ 10_000) do
    start_time = System.monotonic_time()
    task_ref = Native.producer_flush(ref, timeout_ms)

    result =
      receive do
        {^task_ref, {:ok, _}} ->
          :ok

        {^task_ref, {:error, reason}} ->
          Error.wrap({:error, reason})
      after
        # Add buffer to the timeout
        timeout_ms + 5_000 ->
          Error.wrap({:error, :timeout})
      end

    duration = System.monotonic_time() - start_time

    metadata = %{timeout_ms: timeout_ms}

    case result do
      :ok ->
        :telemetry.execute([:franz, :producer, :flush], %{duration: duration}, metadata)

      {:error, error} ->
        :telemetry.execute(
          [:franz, :producer, :flush, :error],
          %{duration: duration},
          Map.put(metadata, :error, error)
        )
    end

    result
  end

  @spec stop(Producer.t()) :: :ok | {:error, Error.t()}
  def stop(%Producer{ref: ref}) do
    Native.producer_stop(ref)
  end
end
