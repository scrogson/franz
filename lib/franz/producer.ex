defmodule Franz.Producer do
  alias Franz.{Message, Native, Producer}
  alias Producer.Config

  defstruct ref: nil

  @type t :: %Producer{
          ref: reference()
        }

  @spec start(Config.t()) :: {:ok, Producer.t()} | {:error, term()}
  def start(config) do
    case Native.producer_start(config) do
      {:ok, ref} ->
        {:ok, %Producer{ref: ref}}

      {:error, error} ->
        {:error, error}
    end
  end

  def send(%Producer{ref: ref}, %Message{} = msg) do
    {:ok, ^ref} = Native.producer_send(ref, msg)

    receive do
      :ok ->
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  @spec stop(Producer.t()) :: :ok | {:error, term()}
  def stop(%Producer{ref: ref}) do
    Native.producer_stop(ref)
  end
end
