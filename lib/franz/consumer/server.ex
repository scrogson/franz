defmodule Franz.Consumer.Server do
  @moduledoc """
  A GenServer wrapper for Franz.Consumer that provides automatic message handling,
  reconnection, and supervision tree integration.

  This module handles the complexity of managing a consumer lifecycle, receiving
  messages, and calling user-defined handlers. It's designed to work seamlessly
  with OTP supervision trees.

  ## Features

  - Automatic subscription on startup
  - Configurable message handler callbacks
  - Automatic message commits (optional)
  - Rebalance event handling
  - Graceful shutdown with optional flush
  - Process-based message consumption

  ## Example

      defmodule MyApp.EventConsumer do
        def start_link(opts) do
          config = Franz.Consumer.Config.new(
            group_id: "my-app-consumers",
            bootstrap_servers: "localhost:9092",
            auto_offset_reset: :earliest,
            enable_auto_commit: false
          )

          Franz.Consumer.Server.start_link(
            name: __MODULE__,
            topics: ["events", "notifications"],
            handler: &handle_message/1,
            config: config
          )
        end

        def handle_message(message) do
          IO.puts("Received: \#{message.payload}")
          # Return :ok to commit, {:ok, :no_commit} to skip commit
          :ok
        end
      end

      # Add to supervision tree
      children = [
        MyApp.EventConsumer
      ]

  ## Handler Return Values

  The handler function can return:
  - `:ok` - Message processed successfully, commit the offset
  - `{:ok, :no_commit}` - Processed successfully, but don't commit
  - `{:error, reason}` - Processing failed, don't commit, log error
  """

  use GenServer
  require Logger

  alias Franz.{Consumer, Error, Message}

  @type handler :: (Message.t() -> :ok | {:ok, :no_commit} | {:error, term()})

  @type option ::
          {:name, atom()}
          | {:topics, [String.t()]}
          | {:handler, handler()}
          | {:config, Consumer.Config.t()}
          | {:auto_commit, boolean()}

  ## Client API

  @doc """
  Starts a consumer server.

  ## Options

  - `:name` - Optional name for the GenServer (default: no name)
  - `:topics` - List of topics to subscribe to (required)
  - `:handler` - Function to handle messages (required)
  - `:config` - Franz.Consumer.Config struct (required)
  - `:auto_commit` - Whether to auto-commit after successful handling (default: true)

  ## Examples

      {:ok, pid} = Franz.Consumer.Server.start_link(
        name: MyConsumer,
        topics: ["events"],
        handler: &handle_message/1,
        config: consumer_config
      )
  """
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {gen_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, gen_opts)
  end

  @doc """
  Pauses consumption from specific partitions.

  ## Examples

      Franz.Consumer.Server.pause(MyConsumer, [{"events", 0}, {"events", 1}])
  """
  @spec pause(GenServer.server(), [{String.t(), integer()}]) :: :ok | {:error, Error.t()}
  def pause(server, partitions) do
    GenServer.call(server, {:pause, partitions})
  end

  @doc """
  Resumes consumption from paused partitions.

  ## Examples

      Franz.Consumer.Server.resume(MyConsumer, [{"events", 0}, {"events", 1}])
  """
  @spec resume(GenServer.server(), [{String.t(), integer()}]) :: :ok | {:error, Error.t()}
  def resume(server, partitions) do
    GenServer.call(server, {:resume, partitions})
  end

  @doc """
  Gets consumer lag for all assigned partitions.

  ## Examples

      {:ok, lag_map} = Franz.Consumer.Server.lag(MyConsumer)
  """
  @spec lag(GenServer.server()) :: {:ok, map()} | {:error, Error.t()}
  def lag(server) do
    GenServer.call(server, :lag)
  end

  @doc """
  Gets the underlying consumer struct (for advanced operations).

  ## Examples

      {:ok, consumer} = Franz.Consumer.Server.get_consumer(MyConsumer)
      {:ok, positions} = Consumer.position(consumer)
  """
  @spec get_consumer(GenServer.server()) :: {:ok, Consumer.t()}
  def get_consumer(server) do
    GenServer.call(server, :get_consumer)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    topics = Keyword.fetch!(opts, :topics)
    handler = Keyword.fetch!(opts, :handler)
    config = Keyword.fetch!(opts, :config)
    auto_commit = Keyword.get(opts, :auto_commit, true)

    # Start consumer
    case Consumer.start(config) do
      {:ok, consumer} ->
        state = %{
          consumer: consumer,
          topics: topics,
          handler: handler,
          auto_commit: auto_commit,
          config: config
        }

        Logger.info("Consumer.Server started")

        {:ok, state, {:continue, :subscribe}}

      {:error, reason} ->
        Logger.error("Failed to start consumer: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl true
  def handle_continue(:subscribe, state) do
    # Subscribe to topics and wait for assignment
    case Consumer.subscribe(state.consumer, state.topics) do
      {:ok, consumer} ->
        # Wait for initial assignment
        {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)

        updated_state = %{state | consumer: consumer}

        Logger.info("Consumer.Server subscribed to topics: #{inspect(state.topics)}")

        {:noreply, updated_state}

      {:error, reason} ->
        Logger.error("Failed to subscribe to topics: #{inspect(reason)}")
        {:stop, reason, state}
    end
  end

  @impl true
  def handle_call({:pause, partitions}, _from, state) do
    result = Consumer.pause(state.consumer, partitions)
    {:reply, result, state}
  end

  def handle_call({:resume, partitions}, _from, state) do
    result = Consumer.resume(state.consumer, partitions)
    {:reply, result, state}
  end

  def handle_call(:lag, _from, state) do
    result = Consumer.lag(state.consumer)
    {:reply, result, state}
  end

  def handle_call(:get_consumer, _from, state) do
    {:reply, {:ok, state.consumer}, state}
  end

  @impl true
  def handle_info({channel, {:message, %{msg: msg}}}, state)
      when channel == state.consumer.channel do
    # Call user handler
    case apply_handler(state.handler, msg) do
      :ok ->
        if state.auto_commit do
          case Consumer.commit(state.consumer, msg) do
            :ok ->
              :ok

            {:error, error} ->
              Logger.warning(
                "Failed to commit message offset #{msg.offset} for #{msg.topic}:#{msg.partition}: #{inspect(error)}"
              )
          end
        end

      {:ok, :no_commit} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Handler error for message #{msg.topic}:#{msg.partition} offset #{msg.offset}: #{inspect(reason)}"
        )
    end

    {:noreply, state}
  end

  def handle_info({channel, {:pre_rebalance, action}}, state)
      when channel == state.consumer.channel do
    Logger.info("Pre-rebalance: #{inspect(action)}")
    {:noreply, state}
  end

  def handle_info({channel, {:post_rebalance, action}}, state)
      when channel == state.consumer.channel do
    Logger.info("Post-rebalance: #{inspect(action)}")
    {:noreply, state}
  end

  def handle_info({channel, {:error, %{reason: reason}}}, state)
      when channel == state.consumer.channel do
    Logger.error("Consumer error: #{inspect(reason)}")
    {:noreply, state}
  end

  @impl true
  def terminate(reason, state) do
    Logger.info("Consumer.Server terminating: #{inspect(reason)}")
    :ok = Consumer.stop(state.consumer)
  end

  ## Private Functions

  defp apply_handler(handler, message) do
    try do
      handler.(message)
    rescue
      e ->
        Logger.error("Handler raised exception: #{Exception.format(:error, e, __STACKTRACE__)}")
        {:error, {:handler_exception, e}}
    end
  end
end
