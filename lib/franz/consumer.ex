defmodule Franz.Consumer do
  defstruct ref: nil

  alias Franz.{Consumer, Message, Native}
  alias Consumer.Config
  require Logger

  @type error :: any()

  @type t :: %Consumer{
          ref: reference()
        }

  @doc """
  Start a Kafka consumer with the provided configuration.
  """
  @spec start(Config.t()) :: {:ok, Consumer.t()} | {:error, error()}
  def start(config) do
    case Native.consumer_start(config) do
      {:ok, ref} ->
        {:ok, %Consumer{ref: ref}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Subscribe to a list of topics.
  """
  @spec subscribe(Consumer.t(), [String.t()]) :: {:ok, Consumer.t()} | {:error, error()}
  def subscribe(%Consumer{ref: ref}, topics) when is_list(topics) do
    {:ok, ^ref} = Native.consumer_subscribe(ref, topics)

    receive do
      :ok ->
        {:ok, %Consumer{ref: ref}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Unsubscribe from the current subscribed topics.
  """
  @spec unsubscribe(Consumer.t()) :: {:ok, Consumer.t()} | {:error, error()}
  def unsubscribe(%Consumer{ref: ref}) do
    {:ok, ^ref} = Native.consumer_unsubscribe(ref)

    receive do
      :ok ->
        {:ok, %Consumer{ref: ref}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec receive_assignments(Consumer.t()) :: {:ok, Consumer.t()} | {:error, error()}
  def receive_assignments(%Consumer{ref: ref} = consumer) do
    {:ok, ^ref} = Native.consumer_poll(ref, 100)

    receive do
      {:pre_rebalance, _} ->
        receive_assignments(consumer)

      {:post_rebalance, assignments} ->
        {:ok, consumer, assignments}
    after
      250 ->
        receive_assignments(consumer)
    end
  end

  @doc """
  Poll for a message.
  """
  @spec poll(Consumer.t()) :: {:ok, Message.t()} | :none
  def poll(%Consumer{ref: ref} = consumer, timeout \\ 250) do
    {:ok, ^ref} = Native.consumer_poll(ref, timeout)

    receive do
      :poll_ready ->
        :poll_ready

      %Message{} = msg ->
        msg
    after
      timeout ->
        poll(consumer, timeout)
    end
  end

  @doc """
  Commit a topic partition.
  """
  @spec commit(Consumer.t(), Message.t()) :: {:ok, Consumer.t()} | :none
  def commit(%Consumer{ref: ref} = consumer, %Message{} = msg) do
    %Message{topic: topic, partition: partition, offset: offset} = msg
    {:ok, ^ref} = Native.consumer_commit(ref, {topic, partition, offset})

    receive do
      {:ok, :commited} -> :ok
    end
  end

  @doc """
  Pause a given topic partition.
  """
  # @spec pause(Consumer.t(), TopicPartion.t())
  def pause(%Consumer{ref: ref}, tpl) do
    Native.consumer_pause(ref, tpl)
  end

  @doc """
  Resume a given topic partition.
  """
  # @spec resume(Consumer.t(), TopicPartion.t())
  def resume(%Consumer{ref: ref}, tpl) do
    Native.consumer_resume(ref, tpl)
  end

  @doc """
  Stop a Kafka client
  """
  @spec stop(Consumer.t()) :: :ok | {:error, error()}
  def stop(%Consumer{ref: ref}) do
    case Native.consumer_stop(ref) do
      :ok ->
        :ok

      {:error, error} ->
        {:error, error}
    end
  end
end
