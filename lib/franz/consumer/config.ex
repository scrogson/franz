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

  @doc """
  Create a new consumer configuration.

  ## Options

  - `:group_id` - Consumer group ID (default: random)
  - `:bootstrap_servers` - Kafka broker addresses (required)
  - `:auto_offset_reset` - Where to start consuming (default: `:beginning`)
    - `:smallest | :earliest | :beginning` - Start from earliest offset
    - `:largest | :latest | :end` - Start from latest offset
    - `:error` - Error if no committed offset
  - `:enable_auto_commit` - Enable auto-commit (default: true)
  - `:topics` - List of topics to subscribe to (default: [])
  - `:security` - Security configuration (optional)

  ## Examples

      # Basic consumer
      Config.new(
        group_id: "my-app",
        bootstrap_servers: "localhost:9092"
      )

      # Consumer starting from latest
      Config.new(
        group_id: "my-app",
        bootstrap_servers: "localhost:9092",
        auto_offset_reset: :latest,
        enable_auto_commit: false
      )
  """
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

  @doc """
  Set the consumer group ID (fluent builder).

  ## Example

      Config.new()
      |> Config.group_id("my-app-consumers")
  """
  @spec group_id(t(), String.t()) :: t()
  def group_id(%__MODULE__{} = config, id) when is_binary(id) do
    %{config | group_id: id}
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
  Set where to start consuming from when no committed offset exists (fluent builder).

  ## Options

  - `:smallest | :earliest | :beginning` - Start from earliest offset
  - `:largest | :latest | :end` - Start from latest offset
  - `:error` - Error if no committed offset

  ## Example

      Config.new()
      |> Config.auto_offset_reset(:earliest)
  """
  @spec auto_offset_reset(t(), auto_offset_reset()) :: t()
  def auto_offset_reset(%__MODULE__{} = config, reset)
      when reset in [:smallest, :earliest, :beginning, :largest, :latest, :end, :error] do
    %{config | auto_offset_reset: reset}
  end

  @doc """
  Enable or disable automatic offset commits (fluent builder).

  ## Example

      Config.new()
      |> Config.enable_auto_commit(false)
  """
  @spec enable_auto_commit(t(), boolean()) :: t()
  def enable_auto_commit(%__MODULE__{} = config, enabled) when is_boolean(enabled) do
    %{config | enable_auto_commit: enabled}
  end

  @doc """
  Set the topics to subscribe to (fluent builder).

  ## Example

      Config.new()
      |> Config.topics(["events", "notifications"])
  """
  @spec topics(t(), [String.t()]) :: t()
  def topics(%__MODULE__{} = config, topic_list) when is_list(topic_list) do
    %{config | topics: topic_list}
  end

  @doc """
  Set the security configuration (fluent builder).

  ## Example

      Config.new()
      |> Config.security(SecurityConfig.new(
        protocol: :sasl_ssl,
        sasl_mechanism: :plain,
        sasl_username: "user",
        sasl_password: "pass"
      ))
  """
  @spec security(t(), SecurityConfig.t() | nil) :: t()
  def security(%__MODULE__{} = config, sec) when is_nil(sec) or is_struct(sec, SecurityConfig) do
    %{config | security: sec}
  end
end
