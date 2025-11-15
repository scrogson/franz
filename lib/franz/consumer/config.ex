defmodule Franz.Consumer.Config do
  @moduledoc """
  Consumer configuration.
  """

  alias Franz.SecurityConfig

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
            topics: [],
            security: nil

  @type t :: %__MODULE__{
          auto_offset_reset: auto_offset_reset(),
          bootstrap_servers: bootstrap_servers(),
          enable_auto_commit: boolean(),
          group_id: group_id(),
          topics: topics(),
          security: SecurityConfig.t() | nil
        }

  def new(opts \\ []) do
    opts = Keyword.put_new(opts, :group_id, Franz.Utils.random_bytes())

    # Convert security keyword list to SecurityConfig struct if present
    opts =
      case Keyword.get(opts, :security) do
        nil -> opts
        sec when is_list(sec) -> Keyword.put(opts, :security, SecurityConfig.new(sec))
        %SecurityConfig{} = _sec -> opts
        _ -> opts
      end

    struct(__MODULE__, opts)
  end
end
