defmodule Franz do
  alias Franz.{Admin, Native}

  defmodule NewTopic do
    defstruct name: "",
              num_partitions: 0,
              # TODO: use TopicReplication enum for Fixed(i32)
              # | Variable(Assignment)
              replication: 1,
              config: []
  end

  def create_topic(bootstrap_servers, %Franz.NewTopic{} = topic),
    do: create_topics(bootstrap_servers, [topic])

  def create_topics(bootstrap_servers, topics) when is_list(topics) do
    config = %Admin.Config{bootstrap_servers: bootstrap_servers}
    {:ok, ref} = Native.admin_start(config)

    case Native.create_topics(ref, topics) do
      {:ok, ^ref} ->
        receive do
          {:ok, something} ->
            {:ok, something}

          other ->
            other
        end

      error ->
        error
    end
  end
end
