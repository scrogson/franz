defmodule Franz do
  alias Franz.{Admin, Native}

  @type bootstrap_servers :: String.t()
  @type topic :: String.t()
  @type reason :: String.t()
  @type create_topics_result :: {:ok, topic()} | {:error, topic(), reason()}

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
  Create a new topic.
  """
  @spec create_topic(bootstrap_servers(), Franz.NewTopic.t()) :: create_topics_result()
  def create_topic(bootstrap_servers, %Franz.NewTopic{} = topic) do
    [result] = create_topics(bootstrap_servers, [topic])
    result
  end

  @doc """
  Create multiple new topics.
  """
  @spec create_topics(bootstrap_servers(), [Franz.NewTopic.t()]) :: [create_topics_result()]
  def create_topics(bootstrap_servers, topics) when is_list(topics) do
    config = %Admin.Config{bootstrap_servers: bootstrap_servers}
    {:ok, ref} = Native.admin_start(config)
    {:ok, ^ref} = Native.create_topics(ref, topics)

    receive do
      result -> result
    end
  end

  @doc """
  Delete a new topic.
  """
  @spec delete_topic(bootstrap_servers(), topic()) :: create_topics_result()
  def delete_topic(bootstrap_servers, topic) when is_binary(topic) do
    [result] = delete_topics(bootstrap_servers, [topic])
    result
  end

  @doc """
  Delete multiple new topics.
  """
  @spec delete_topics(bootstrap_servers(), [topic()]) :: [create_topics_result()]
  def delete_topics(bootstrap_servers, topics) when is_list(topics) do
    config = %Admin.Config{bootstrap_servers: bootstrap_servers}
    {:ok, ref} = Native.admin_start(config)
    {:ok, ^ref} = Native.delete_topics(ref, topics)

    receive do
      result -> result
    end
  end
end
