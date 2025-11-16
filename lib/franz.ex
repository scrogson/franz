defmodule Franz do
  alias Franz.{Admin, Error, Native}

  @type bootstrap_servers :: String.t()
  @type topic :: String.t()
  @type topic_result :: :ok | {:error, Error.t()}

  defmodule BrokerMetadata do
    @moduledoc """
    Metadata about a Kafka broker.
    """
    defstruct id: 0, host: "", port: 0

    @type t :: %__MODULE__{
            id: integer(),
            host: String.t(),
            port: integer()
          }
  end

  defmodule ClusterMetadata do
    @moduledoc """
    Metadata about the Kafka cluster.
    """
    defstruct cluster_id: "",
              controller_id: 0,
              broker_count: 0,
              brokers: []

    @type t :: %__MODULE__{
            cluster_id: String.t(),
            controller_id: integer(),
            broker_count: integer(),
            brokers: [BrokerMetadata.t()]
          }
  end

  defmodule TopicReplication do
    @moduledoc """
    Replication configuration for a new topic.

    Can be either:
    - `{:fixed, factor}` - All partitions have the same replication factor
    - `{:variable, assignments}` - Each partition has specific replica assignments
      where assignments is a list of lists, with each inner list containing broker IDs

    ## Examples

        # Fixed replication factor of 3 for all partitions
        {:fixed, %{factor: 3}}

        # Variable assignment: partition 0 on brokers [1, 2, 3], partition 1 on [2, 3, 4]
        {:variable, %{assignments: [[1, 2, 3], [2, 3, 4]]}}
    """

    @type t ::
      {:fixed, %{factor: pos_integer()}}
      | {:variable, %{assignments: [[pos_integer()]]}}

    @doc """
    Create a fixed replication configuration.
    """
    @spec fixed(pos_integer()) :: t()
    def fixed(factor) when is_integer(factor) and factor > 0 do
      {:fixed, %{factor: factor}}
    end

    @doc """
    Create a variable replication configuration with specific broker assignments per partition.
    """
    @spec variable([[pos_integer()]]) :: t()
    def variable(assignments) when is_list(assignments) do
      {:variable, %{assignments: assignments}}
    end
  end

  defmodule NewTopic do
    @moduledoc """
    Configuration for creating a new Kafka topic.

    ## Fields

    - `name` - The topic name
    - `num_partitions` - Number of partitions (default: 1)
    - `replication` - Replication configuration (default: `{:fixed, %{factor: 1}}`)
    - `config` - Additional topic configuration as key-value pairs

    ## Examples

        # Simple topic with default settings
        %Franz.NewTopic{name: "events"}

        # Topic with custom partitions and replication
        %Franz.NewTopic{
          name: "logs",
          num_partitions: 10,
          replication: Franz.TopicReplication.fixed(3)
        }

        # Topic with variable replica assignment
        %Franz.NewTopic{
          name: "metrics",
          num_partitions: 2,
          replication: Franz.TopicReplication.variable([[1, 2], [2, 3]])
        }
    """

    defstruct name: "",
              num_partitions: 1,
              replication: {:fixed, %{factor: 1}},
              config: []

    @type t :: %Franz.NewTopic{
            name: Franz.topic(),
            num_partitions: pos_integer(),
            replication: Franz.TopicReplication.t(),
            config: [{String.t(), String.t()}]
          }
  end

  @doc """
  Creates a new topic according to the provided `%NewTopic{}` specification.
  """
  @spec create_topic(bootstrap_servers(), Franz.NewTopic.t()) :: topic_result()
  def create_topic(bootstrap_servers, %Franz.NewTopic{} = topic) do
    start_time = System.monotonic_time()

    result =
      case create_topics(bootstrap_servers, [topic]) do
        [{:ok, _}] -> :ok
        [{:error, {_, error}}] -> Error.wrap({:error, error})
      end

    duration = System.monotonic_time() - start_time

    metadata = %{
      topic: topic.name,
      num_partitions: topic.num_partitions,
      replication: topic.replication,
      bootstrap_servers: bootstrap_servers
    }

    case result do
      :ok ->
        :telemetry.execute([:franz, :admin, :create_topic], %{duration: duration}, metadata)

      {:error, error} ->
        :telemetry.execute(
          [:franz, :admin, :create_topic, :error],
          %{duration: duration},
          Map.put(metadata, :error, error)
        )
    end

    result
  end

  @doc """
  Creates new topics according to the provided `%NewTopic{}` specifications.

  Note that while the API supports creating multiple topics at once, it is not
  transactional. Creation of some topics may succeed while others fail. Be sure
  to check the result of each individual operation.
  """
  @spec create_topics(bootstrap_servers(), [Franz.NewTopic.t()]) :: [topic_result()]
  def create_topics(bootstrap_servers, topics) when is_list(topics) do
    config = %Admin.Config{bootstrap_servers: bootstrap_servers}
    {:ok, admin_ref} = Native.admin_start(config)
    task_ref = Native.create_topics(admin_ref, topics)

    receive do
      {^task_ref, {:ok, results}} -> results
      {^task_ref, {:error, error}} -> {:error, error}
    after
      30_000 ->
        {:error, :timeout}
    end
  end

  @doc """
  Delete a named topic.
  """
  @spec delete_topic(bootstrap_servers(), topic()) :: topic_result()
  def delete_topic(bootstrap_servers, topic) when is_binary(topic) do
    start_time = System.monotonic_time()

    result =
      case delete_topics(bootstrap_servers, [topic]) do
        [{:ok, _}] -> :ok
        [{:error, {_, error}}] -> Error.wrap({:error, error})
      end

    duration = System.monotonic_time() - start_time

    metadata = %{
      topic: topic,
      bootstrap_servers: bootstrap_servers
    }

    case result do
      :ok ->
        :telemetry.execute([:franz, :admin, :delete_topic], %{duration: duration}, metadata)

      {:error, error} ->
        :telemetry.execute(
          [:franz, :admin, :delete_topic, :error],
          %{duration: duration},
          Map.put(metadata, :error, error)
        )
    end

    result
  end

  @doc """
  Deletes the named topics.

  Note that while the API supports deleting multiple topics at once, it is not
  transactional. Deletion of some topics may succeed while others fail. Be sure
  to check the result of each individual operation.
  """
  @spec delete_topics(bootstrap_servers(), [topic()]) :: [topic_result()]
  def delete_topics(bootstrap_servers, topics) when is_list(topics) do
    config = %Admin.Config{bootstrap_servers: bootstrap_servers}
    {:ok, admin_ref} = Native.admin_start(config)
    task_ref = Native.delete_topics(admin_ref, topics)

    receive do
      {^task_ref, {:ok, results}} -> results
      {^task_ref, {:error, error}} -> {:error, error}
    after
      30_000 ->
        {:error, :timeout}
    end
  end

  @doc """
  List all topics in the Kafka cluster.

  ## Options

  - `timeout_ms` - Timeout in milliseconds (default: 5000)

  ## Examples

      {:ok, topics} = Franz.list_topics("localhost:9092")
      # ["events", "logs", "metrics"]
  """
  @spec list_topics(bootstrap_servers(), timeout: pos_integer()) ::
          {:ok, [String.t()]} | {:error, Error.t()}
  def list_topics(bootstrap_servers, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout, 5000)
    config = %Admin.Config{bootstrap_servers: bootstrap_servers}
    {:ok, admin_ref} = Native.admin_start(config)
    task_ref = Native.list_topics(admin_ref, timeout_ms)

    receive do
      {^task_ref, {:ok, topics}} -> {:ok, topics}
      {^task_ref, {:error, error}} -> Error.wrap({:error, error})
    after
      timeout_ms + 1000 ->
        Error.wrap({:error, :timeout})
    end
  end

  @doc """
  Get metadata about the Kafka cluster including brokers and controller.

  ## Options

  - `timeout_ms` - Timeout in milliseconds (default: 5000)

  ## Examples

      {:ok, metadata} = Franz.describe_cluster("localhost:9092")
      IO.inspect(metadata.broker_count)
      # 3
      IO.inspect(metadata.controller_id)
      # 1
  """
  @spec describe_cluster(bootstrap_servers(), timeout: pos_integer()) ::
          {:ok, ClusterMetadata.t()} | {:error, Error.t()}
  def describe_cluster(bootstrap_servers, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout, 5000)
    config = %Admin.Config{bootstrap_servers: bootstrap_servers}
    {:ok, admin_ref} = Native.admin_start(config)
    task_ref = Native.describe_cluster(admin_ref, timeout_ms)

    receive do
      {^task_ref, {:ok, metadata}} -> {:ok, metadata}
      {^task_ref, {:error, error}} -> Error.wrap({:error, error})
    after
      timeout_ms + 1000 ->
        Error.wrap({:error, :timeout})
    end
  end
end
