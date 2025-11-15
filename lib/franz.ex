defmodule Franz do
  alias Franz.{Admin, Error, Native}

  @type bootstrap_servers :: String.t()
  @type topic :: String.t()
  @type topic_result :: :ok | {:error, Error.t()}

  defmodule NewTopic do
    defstruct name: "",
              num_partitions: 1,
              # TODO: use TopicReplication enum for Fixed(i32)
              # | Variable(Assignment)
              replication: 1,
              config: []

    @type t :: %Franz.NewTopic{
            name: Franz.topic(),
            num_partitions: pos_integer(),
            replication: pos_integer(),
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
end
