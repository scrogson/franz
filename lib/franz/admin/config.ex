defmodule Franz.Admin.Config do
  alias Franz.SecurityConfig

  defstruct bootstrap_servers: "",
            security: nil

  @type t :: %__MODULE__{
          bootstrap_servers: String.t(),
          security: SecurityConfig.t() | nil
        }

  def new(opts \\ []) do
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
