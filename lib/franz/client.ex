defmodule Franz.Client do
  alias Franz.{Client, Config, Message, Native}

  defstruct ref: nil

  @type error :: any()

  @type t :: %Client{
          ref: reference()
        }

  @doc """
  Start a Kafka client with the provided configuration.
  """
  @spec start(Config.t()) :: {:ok, Client.t()} | {:error, error()}
  def start(config) do
    case Native.client_start(config) do
      {:ok, ref} ->
        {:ok, %Client{ref: ref}}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Poll for a message.
  """
  @spec poll(Client.t()) :: {:ok, Message.t()} | :none
  def poll(%Client{ref: ref}, timeout \\ 250) do
    case Native.client_poll(ref, timeout) do
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
  # @spec pause(Client.t(), TopicPartion.t())
  def pause(%Client{ref: ref}, tpl) do
    Native.client_pause(ref, tpl)
  end

  @doc """
  Resume a given topic partition.
  """
  # @spec resume(Client.t(), TopicPartion.t())
  def resume(%Client{ref: ref}, tpl) do
    Native.client_resume(ref, tpl)
  end

  @doc """
  Stop a Kafka client
  """
  @spec stop(Client.t()) :: :ok | {:error, error()}
  def stop(%Client{ref: ref}) do
    case Native.client_stop(ref) do
      :ok ->
        :ok

      {:error, error} ->
        {:error, error}
    end
  end
end
