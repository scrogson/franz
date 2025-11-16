defmodule Franz.Producer.Config do
  alias Franz.SecurityConfig

  defstruct bootstrap_servers: "",
            security: nil,
            acks: :all,
            compression_type: :none,
            linger_ms: 0,
            batch_size: 16384,
            max_in_flight: 5

  @type acks :: :none | :leader | :all
  @type compression :: :none | :gzip | :snappy | :lz4 | :zstd

  @type t :: %__MODULE__{
          bootstrap_servers: String.t(),
          security: SecurityConfig.t() | nil,
          acks: acks(),
          compression_type: compression(),
          linger_ms: non_neg_integer(),
          batch_size: pos_integer(),
          max_in_flight: pos_integer()
        }

  @doc """
  Create a new producer configuration.

  ## Options

  - `:bootstrap_servers` - Kafka broker addresses (required)
  - `:security` - Security configuration (SecurityConfig struct or keyword list)
  - `:acks` - Acknowledgement level (default: `:all`)
    - `:none` (0) - Fire and forget, no acks
    - `:leader` (1) - Wait for leader ack only
    - `:all` (-1) - Wait for all in-sync replicas
  - `:compression_type` - Message compression (default: `:none`)
    - `:none`, `:gzip`, `:snappy`, `:lz4`, `:zstd`
  - `:linger_ms` - Time to wait before sending batch (default: 0)
  - `:batch_size` - Maximum batch size in bytes (default: 16384)
  - `:max_in_flight` - Maximum unacknowledged requests (default: 5)

  ## Examples

      # High-throughput config with compression and batching
      Config.new(
        bootstrap_servers: "localhost:9092",
        acks: :leader,
        compression_type: :lz4,
        linger_ms: 10,
        batch_size: 1_000_000,
        max_in_flight: 10
      )

      # Maximum reliability config
      Config.new(
        bootstrap_servers: "localhost:9092",
        acks: :all,
        max_in_flight: 1
      )

      # Maximum throughput config (fire-and-forget)
      Config.new(
        bootstrap_servers: "localhost:9092",
        acks: :none,
        compression_type: :lz4,
        linger_ms: 100,
        batch_size: 10_000_000
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
  Set the acknowledgement level (fluent builder).

  ## Options

  - `:none` - Fire and forget, no acks
  - `:leader` - Wait for leader ack only
  - `:all` - Wait for all in-sync replicas

  ## Example

      Config.new()
      |> Config.acks(:leader)
  """
  @spec acks(t(), acks()) :: t()
  def acks(%__MODULE__{} = config, level) when level in [:none, :leader, :all] do
    %{config | acks: level}
  end

  @doc """
  Set the compression type (fluent builder).

  ## Options

  - `:none`, `:gzip`, `:snappy`, `:lz4`, `:zstd`

  ## Example

      Config.new()
      |> Config.compression_type(:lz4)
  """
  @spec compression_type(t(), compression()) :: t()
  def compression_type(%__MODULE__{} = config, type)
      when type in [:none, :gzip, :snappy, :lz4, :zstd] do
    %{config | compression_type: type}
  end

  @doc """
  Set the linger time in milliseconds (fluent builder).

  Time to wait before sending a batch, allowing messages to accumulate.

  ## Example

      Config.new()
      |> Config.linger_ms(10)
  """
  @spec linger_ms(t(), non_neg_integer()) :: t()
  def linger_ms(%__MODULE__{} = config, ms) when is_integer(ms) and ms >= 0 do
    %{config | linger_ms: ms}
  end

  @doc """
  Set the maximum batch size in bytes (fluent builder).

  ## Example

      Config.new()
      |> Config.batch_size(1_000_000)
  """
  @spec batch_size(t(), pos_integer()) :: t()
  def batch_size(%__MODULE__{} = config, size) when is_integer(size) and size > 0 do
    %{config | batch_size: size}
  end

  @doc """
  Set the maximum number of unacknowledged requests (fluent builder).

  ## Example

      Config.new()
      |> Config.max_in_flight(10)
  """
  @spec max_in_flight(t(), pos_integer()) :: t()
  def max_in_flight(%__MODULE__{} = config, max) when is_integer(max) and max > 0 do
    %{config | max_in_flight: max}
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
