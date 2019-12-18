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

  def send(%Producer{ref: ref}, %Message{} = message) do
    case Native.producer_send(ref, message) do
      {:ok, ^ref} ->
        receive do
          {:error, _reason} = error ->
            error

          any ->
            {:ok, any}
        end

      :error ->
        :error
    end
  end
end
