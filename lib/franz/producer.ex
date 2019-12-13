defmodule Franz.Producer do
  alias Franz.Producer

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

  def poll(%Producer{ref: ref}, timeout \\ 100) do
    case Native.producer_poll(ref, timeout) do
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
