defmodule Franz.Config do
  @moduledoc """
  Client configuration.
  """

  alias Franz.Config

  # TODO: convert these to atoms and make type-safe on the NIF side.
  @type auto_offset_reset :: :earliest | :beginning | :latest | :end | :error
  @type brokers :: String.t()
  @type group_id :: nil | String.t()
  @type topic :: String.t()
  @type topics :: [topic()]

  defstruct auto_offset_reset: "beginning",
            brokers: "",
            enable_auto_commit: true,
            group_id: nil,
            topics: []

  @type t :: %Config{
          auto_offset_reset: auto_offset_reset(),
          brokers: brokers(),
          enable_auto_commit: boolean(),
          group_id: group_id(),
          topics: topics()
        }

  def new(opts \\ []) do
    opts = Keyword.put_new(opts, :group_id, generate_group_id())

    struct(Config, opts)
  end

  defp generate_group_id do
    8
    |> :crypto.strong_rand_bytes()
    |> Base.encode64()
  end
end
