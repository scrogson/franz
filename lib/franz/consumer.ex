defmodule Franz.Consumer do
  defstruct ref: nil, channel: nil

  alias Franz.{Consumer, Message, Native}
  alias Consumer.Config
  require Logger

  @type error :: any()

  @type t :: %Consumer{
          ref: reference(),
          channel: reference()
        }

  @doc """
  Start a Kafka consumer with the provided configuration.
  """
  @spec start(Config.t()) :: {:ok, Consumer.t()} | {:error, error()}
  def start(config) do
    # Start the streaming task - it returns a channel sender reference
    channel_sender = Native.consumer_stream(config)
    {:ok, %Consumer{ref: channel_sender, channel: channel_sender}}
  end

  @doc """
  Subscribe to a list of topics.
  """
  @spec subscribe(Consumer.t(), [String.t()]) :: {:ok, Consumer.t()} | {:error, error()}
  def subscribe(%Consumer{channel: channel} = consumer, topics) when is_list(topics) do
    :ok = Native.consumer_subscribe(channel, topics)

    # Wait for response from the streaming task - messages are tagged with channel ref
    receive do
      {^channel, :ok} ->
        {:ok, consumer}

      {^channel, {:error, %{reason: reason}}} ->
        {:error, reason}
    end
  end

  @doc """
  Get current partition assignments.
  """
  @spec assignment(Consumer.t()) :: {:ok, list()} | {:error, error()}
  def assignment(%Consumer{channel: channel}) do
    :ok = Native.consumer_assignment(channel)

    receive do
      {^channel, {:assignments, %{assignments: assignments}}} ->
        {:ok, assignments}

      {^channel, {:error, %{reason: reason}}} ->
        {:error, reason}

      other ->
        {:error, {:unexpected_message, other}}
    end
  end

  @doc """
  Unsubscribe from the current subscribed topics.
  """
  @spec unsubscribe(Consumer.t()) :: {:ok, Consumer.t()} | {:error, error()}
  def unsubscribe(%Consumer{channel: channel} = consumer) do
    :ok = Native.consumer_unsubscribe(channel)

    receive do
      {^channel, :ok} ->
        {:ok, consumer}

      {^channel, {:error, %{reason: reason}}} ->
        {:error, reason}
    end
  end

  @spec receive_assignments(Consumer.t()) :: {:ok, list(), Consumer.t()} | {:error, error()}
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
  @spec commit(Consumer.t(), Message.t()) :: :ok | {:error, error()}
  def commit(%Consumer{channel: channel}, %Message{} = msg) do
    %Message{topic: topic, partition: partition, offset: offset} = msg
    :ok = Native.consumer_commit(channel, {topic, partition, offset})

    receive do
      {^channel, :ok} -> :ok
      {^channel, {:error, %{reason: reason}}} -> {:error, reason}
    end
  end

  @doc """
  Retrieve committed offsets for topics and partitions.
  """
  @spec committed(Consumer.t(), number()) :: {:ok, list()} | {:error, term()}
  def committed(%Consumer{channel: channel}, timeout \\ 100) do
    :ok = Native.consumer_committed(channel, timeout)

    receive do
      {^channel, {:committed, %{offsets: offsets}}} ->
        {:ok, offsets}

      {^channel, {:error, %{reason: reason}}} ->
        {:error, reason}
    end
  end

  @doc """
  Stop a Kafka consumer.
  The task will automatically stop when the channel is dropped or the process exits.
  """
  @spec stop(Consumer.t()) :: :ok | {:error, error()}
  def stop(%Consumer{}) do
    # With the task-based approach, the consumer task will automatically stop
    # when the channel is dropped or the process exits
    :ok
  end

  @doc """
  Handle incoming messages from the consumer.
  This is a helper to pattern match on different event types.
  """
  def handle_event(event) do
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
  end
end
