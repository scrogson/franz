defmodule Franz.Consumer.Config do
  @moduledoc """
  Consumer configuration.
  """

  alias Franz.Consumer.Config

  @type auto_offset_reset ::
          :smallest | :earliest | :beginning | :largest | :latest | :end | :error
  @type bootstrap_servers :: String.t()
  @type group_id :: nil | String.t()
  @type topic :: String.t()
  @type topics :: [topic()]

  defstruct auto_offset_reset: :beginning,
            bootstrap_servers: "",
            enable_auto_commit: true,
            group_id: nil,
            topics: []

  @type t :: %Config{
          auto_offset_reset: auto_offset_reset(),
          bootstrap_servers: bootstrap_servers(),
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
