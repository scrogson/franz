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
end
