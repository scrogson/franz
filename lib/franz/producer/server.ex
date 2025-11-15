defmodule Franz.Producer.Server do
  @moduledoc """
  A GenServer wrapper for Franz.Producer that provides a managed producer instance
  with supervision tree integration and a simple API.

  This module handles the complexity of managing a producer lifecycle and provides
  both synchronous and asynchronous send operations. It's designed to work seamlessly
  with OTP supervision trees.

  ## Features

  - Automatic producer startup
  - GenServer-based API
  - Synchronous and asynchronous sends
  - Graceful shutdown with flush
  - Supervision tree integration
  - In-flight message tracking

  ## Example

      defmodule MyApp.EventProducer do
        def start_link(opts) do
          config = Franz.Producer.Config.new(
            bootstrap_servers: "localhost:9092",
            acks: :leader,
            compression_type: :lz4
          )

          Franz.Producer.Server.start_link(
            name: __MODULE__,
            config: config
          )
        end

        def send_event(event_data) do
          message = %Franz.Message{
            topic: "events",
            payload: Jason.encode!(event_data)
          }

          Franz.Producer.Server.send_async(__MODULE__, message)
        end
      end

      # Add to supervision tree
      children = [
        MyApp.EventProducer
      ]

  ## Synchronous vs Asynchronous Sends

  - `send/3` - Waits for delivery confirmation (slower, guaranteed delivery)
  - `send_async/3` - Returns immediately (faster, eventual delivery)
  - `flush/2` - Ensures all pending messages are delivered

  """

  use GenServer
  require Logger

  alias Franz.{Producer, Error, Message}

  @type option ::
          {:name, atom()}
          | {:config, Producer.Config.t()}

  ## Client API

  @doc """
  Starts a producer server.

  ## Options

  - `:name` - Optional name for the GenServer (default: no name)
  - `:config` - Franz.Producer.Config struct (required)

  ## Examples

      {:ok, pid} = Franz.Producer.Server.start_link(
        name: MyProducer,
        config: producer_config
      )
  """
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {gen_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, gen_opts)
  end

  @doc """
  Sends a message synchronously (waits for delivery confirmation).

  This blocks until the message is delivered or an error occurs.
  For high-throughput scenarios, use `send_async/3` instead.

  ## Examples

      :ok = Franz.Producer.Server.send(MyProducer, message)
  """
  @spec send(GenServer.server(), Message.t(), timeout()) :: :ok | {:error, Error.t()}
  def send(server, message, timeout \\ 10_000) do
    GenServer.call(server, {:send, message}, timeout)
  end

  @doc """
  Sends a message asynchronously (returns immediately).

  This provides maximum throughput by not waiting for delivery confirmation.
  Messages are buffered and delivered in the background. Use `flush/2` to
  ensure all messages are delivered.

  ## Examples

      :ok = Franz.Producer.Server.send_async(MyProducer, message)
  """
  @spec send_async(GenServer.server(), Message.t()) :: :ok | {:error, Error.t()}
  def send_async(server, message) do
    GenServer.call(server, {:send_async, message})
  end

  @doc """
  Flushes all pending messages from the producer.

  This blocks until all outstanding messages are delivered or the timeout expires.
  Should be called before stopping the producer to ensure no message loss.

  ## Examples

      :ok = Franz.Producer.Server.flush(MyProducer)
      :ok = Franz.Producer.Server.flush(MyProducer, 30_000)
  """
  @spec flush(GenServer.server(), timeout()) :: :ok | {:error, Error.t()}
  def flush(server, timeout \\ 10_000) do
    GenServer.call(server, {:flush, timeout}, timeout + 5_000)
  end

  @doc """
  Gets the number of messages waiting to be sent or acknowledged.

  Useful for monitoring producer queue depth and implementing backpressure.

  ## Examples

      count = Franz.Producer.Server.in_flight_count(MyProducer)
  """
  @spec in_flight_count(GenServer.server()) :: non_neg_integer()
  def in_flight_count(server) do
    GenServer.call(server, :in_flight_count)
  end

  @doc """
  Gets the underlying producer struct (for advanced operations).

  ## Examples

      {:ok, producer} = Franz.Producer.Server.get_producer(MyProducer)
  """
  @spec get_producer(GenServer.server()) :: {:ok, Producer.t()}
  def get_producer(server) do
    GenServer.call(server, :get_producer)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)

    case Producer.start(config) do
      {:ok, producer} ->
        Logger.info("Producer.Server started")

        state = %{
          producer: producer,
          config: config
        }

        {:ok, state}

      {:error, reason} ->
        Logger.error("Failed to start producer: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:send, message}, _from, state) do
    result = Producer.send(state.producer, message)
    {:reply, result, state}
  end

  def handle_call({:send_async, message}, _from, state) do
    result = Producer.send_async(state.producer, message)
    {:reply, result, state}
  end

  def handle_call({:flush, timeout}, _from, state) do
    result = Producer.flush(state.producer, timeout)
    {:reply, result, state}
  end

  def handle_call(:in_flight_count, _from, state) do
    count = Producer.in_flight_count(state.producer)
    {:reply, count, state}
  end

  def handle_call(:get_producer, _from, state) do
    {:reply, {:ok, state.producer}, state}
  end

  @impl true
  def terminate(reason, state) do
    Logger.info("Producer.Server terminating: #{inspect(reason)}")

    # Flush any pending messages before shutdown
    case Producer.flush(state.producer, 10_000) do
      :ok ->
        Logger.info("Producer flushed successfully before shutdown")

      {:error, error} ->
        Logger.warning("Failed to flush producer before shutdown: #{inspect(error)}")
    end

    :ok = Producer.stop(state.producer)
    :ok
  end
end
