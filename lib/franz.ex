defmodule Franz do
  alias Franz.{Admin, Native}

  @type bootstrap_servers :: String.t()
  @type topic :: String.t()
  @type reason :: String.t()
  @type topic_result :: {:ok, topic()} | {:error, topic(), reason()}

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
    case create_topics(bootstrap_servers, [topic]) do
      [{:ok, _}] -> :ok
      [{:error, {_, error}}] -> {:error, error}
    end
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
    {:ok, ref} = Native.admin_start(config)
    {:ok, ^ref} = Native.create_topics(ref, topics)

    receive do
      {:ok, results} -> results
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Delete a named topic.
  """
  @spec delete_topic(bootstrap_servers(), topic()) :: topic_result()
  def delete_topic(bootstrap_servers, topic) when is_binary(topic) do
    case delete_topics(bootstrap_servers, [topic]) do
      [{:ok, _}] -> :ok
      [{:error, {_, error}}] -> {:error, error}
    end
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
    {:ok, ref} = Native.admin_start(config)
    {:ok, ^ref} = Native.delete_topics(ref, topics)

    receive do
      {:ok, results} -> results
      {:error, error} -> {:error, error}
    end
  end
end
