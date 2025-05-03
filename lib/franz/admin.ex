defmodule Franz.Admin do
  defstruct ref: nil

  alias Franz.{Admin, Native}
  alias Admin.Config

  @type error :: any()

  @type t :: %Admin{
          ref: reference()
        }

  @doc """
  start a kafka admin client with the provided configuration.
  """
  @spec start(Config.t()) :: {:ok, Admin.t()} | {:error, error()}
  def start(config) do
    case Native.admin_start(config) do
      {:ok, ref} ->
        {:ok, %Admin{ref: ref}}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  stop a kafka client
  """
  @spec stop(Admin.t()) :: :ok | {:error, error()}
  def stop(%Admin{ref: ref}) do
    case Native.admin_stop(ref) do
      :ok ->
        :ok

      {:error, error} ->
        {:error, error}
    end
  end
end
