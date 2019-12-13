defmodule Franz.Consumer do
  defstruct ref: nil

  alias Franz.{Consumer, Message, Native}
  alias Consumer.Config

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

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Poll for a message.
  """
  @spec poll(Consumer.t()) :: {:ok, Message.t()} | :none
  def poll(%Consumer{ref: ref}, timeout \\ 250) do
    case Native.consumer_poll(ref, timeout) do
      {:ok, ^ref} ->
        receive do
          %Message{} = msg ->
            {:ok, msg}

          nil ->
            {:ok, nil}

          {:error, _reason} = error ->
            error
        end

      :error ->
        :error
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
