defmodule Franz.Admin.Config do
  @moduledoc """
  Admin client configuration.
  """

  alias Franz.SecurityConfig

  defstruct bootstrap_servers: "",
            security: nil

  @type t :: %__MODULE__{
          bootstrap_servers: String.t(),
          security: SecurityConfig.t() | nil
        }

  @doc """
  Create a new admin configuration.

  ## Options

  - `:bootstrap_servers` - Kafka broker addresses (required)
  - `:security` - Security configuration (optional)

  ## Examples

      # Basic admin client
      Config.new(bootstrap_servers: "localhost:9092")

      # With security
      Config.new(
        bootstrap_servers: "localhost:9092",
        security: SecurityConfig.new(
          protocol: :sasl_ssl,
          sasl_mechanism: :plain,
          sasl_username: "admin",
          sasl_password: "secret"
        )
      )
  """
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

  @doc """
  Set the Kafka broker addresses (fluent builder).

  ## Example

      Config.new()
      |> Config.bootstrap_servers("localhost:9092")
  """
  @spec bootstrap_servers(t(), String.t()) :: t()
  def bootstrap_servers(%__MODULE__{} = config, servers) when is_binary(servers) do
    %{config | bootstrap_servers: servers}
  end

  @doc """
  Set the security configuration (fluent builder).

  ## Example

      Config.new()
      |> Config.security(SecurityConfig.new(
        protocol: :sasl_ssl,
        sasl_mechanism: :plain,
        sasl_username: "admin",
        sasl_password: "secret"
      ))
  """
  @spec security(t(), SecurityConfig.t() | nil) :: t()
  def security(%__MODULE__{} = config, sec) when is_nil(sec) or is_struct(sec, SecurityConfig) do
    %{config | security: sec}
  end
end
