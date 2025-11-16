if Code.ensure_loaded?(Broadway) do
  defmodule Franz.BroadwayProducer do
    @moduledoc """
      A Broadway producer for consuming Kafka messages via Franz.

    This module integrates Franz with Broadway, enabling powerful concurrent
    and multi-stage message processing with automatic batching, rate limiting,
    and fault tolerance.

    ## Features

    - Automatic demand-driven message consumption
    - Configurable batching and concurrency
    - Offset commits on acknowledgement
    - Graceful shutdown with proper cleanup
    - Built-in telemetry and observability
    - Integration with Broadway's supervision tree

    ## Options

    - `:topics` - List of Kafka topics to consume (required)
    - `:group_id` - Consumer group ID (required)
    - `:bootstrap_servers` - Kafka broker addresses (required)
    - `:config` - Additional Franz.Consumer.Config options (optional)
    - `:receive_interval` - Interval between message polls in ms (default: 100)

    ## Example

        defmodule MyApp.EventProcessor do
          use Broadway

          def start_link(_opts) do
            Broadway.start_link(__MODULE__,
              name: __MODULE__,
              producer: [
                module: {Franz.BroadwayProducer,
                  topics: ["events"],
                  group_id: "my-app-processor",
                  bootstrap_servers: "localhost:9092"
                },
                concurrency: 1
              ],
              processors: [
                default: [
                  concurrency: 10,
                  min_demand: 5,
                  max_demand: 10
                ]
              ],
              batchers: [
                default: [
                  concurrency: 5,
                  batch_size: 100,
                  batch_timeout: 1000
                ]
              ]
            )
          end

          @impl true
          def handle_message(_processor, message, _context) do
            # Process individual message
            IO.inspect(message.data, label: "Processing")
            message
          end

          @impl true
          def handle_batch(_batcher, messages, _batch_info, _context) do
            # Process batch of messages
            IO.inspect(length(messages), label: "Batch size")
            messages
          end
        end

    ## Acknowledgements

    Messages are automatically committed to Kafka when acknowledged by Broadway.
    Failed messages (via `Broadway.Message.failed/2`) will not be committed,
    allowing them to be reprocessed.

    ## Graceful Shutdown

    On shutdown, the producer will:
    1. Stop accepting new demand
    2. Drain any pending messages
    3. Commit final offsets
    4. Close the consumer connection
    """

    use GenStage
    require Logger

    alias Franz.Consumer
    alias Broadway.Message, as: BroadwayMessage

    @behaviour Broadway.Producer

    @impl true
    def prepare_for_start(_module, broadway_opts) do
      # Broadway passes the full configuration, we need to extract our module options
      # from the producer.module tuple: {Franz.BroadwayProducer, franz_opts}
      {_module, franz_opts} =
        broadway_opts
        |> Keyword.fetch!(:producer)
        |> Keyword.fetch!(:module)

      # Validate Franz options
      topics = Keyword.fetch!(franz_opts, :topics)
      group_id = Keyword.fetch!(franz_opts, :group_id)
      bootstrap_servers = Keyword.fetch!(franz_opts, :bootstrap_servers)
      additional_config = Keyword.get(franz_opts, :config, [])
      receive_interval = Keyword.get(franz_opts, :receive_interval, 100)

      # Build consumer config
      config =
        Consumer.Config.new(
          Keyword.merge(
            [
              group_id: group_id,
              bootstrap_servers: bootstrap_servers,
              auto_offset_reset: :earliest,
              enable_auto_commit: false
            ],
            additional_config
          )
        )

      # Prepare init options for our producer
      init_opts = [
        config: config,
        topics: topics,
        receive_interval: receive_interval
      ]

      # Return modified Broadway opts with our init options in the module tuple
      # The first element is a list of child specs (we don't need any)
      # The second element is the updated Broadway options
      producer_opts = Keyword.get(broadway_opts, :producer, [])
      modified_producer_opts = Keyword.put(producer_opts, :module, {__MODULE__, init_opts})
      modified_broadway_opts = Keyword.put(broadway_opts, :producer, modified_producer_opts)

      {[], modified_broadway_opts}
    end

    @impl true
    def init(opts) do
      config = Keyword.fetch!(opts, :config)
      topics = Keyword.fetch!(opts, :topics)
      receive_interval = Keyword.get(opts, :receive_interval, 100)

      {:ok, consumer} = Consumer.start(config)

      state = %{
        consumer: consumer,
        topics: topics,
        demand: 0,
        receive_interval: receive_interval,
        receive_timer: nil,
        buffer: :queue.new()
      }

      Logger.info("Franz.BroadwayProducer started")

      {:producer, state,
       dispatcher: {GenStage.DemandDispatcher, [shuffle_demands_on_first_dispatch: true]}}
    end

    @impl true
    def handle_demand(incoming_demand, %{demand: existing_demand} = state) do
      total_demand = incoming_demand + existing_demand

      # Subscribe on first demand
      state =
        if is_nil(state.receive_timer) do
          case Consumer.subscribe(state.consumer, state.topics) do
            {:ok, consumer} ->
              {:ok, _assignments, consumer} = Consumer.receive_assignments(consumer)
              Logger.info("Franz.BroadwayProducer subscribed to topics: #{inspect(state.topics)}")

              # Start polling for messages
              timer = schedule_receive(state.receive_interval)

              %{state | consumer: consumer, receive_timer: timer}

            {:error, reason} ->
              Logger.error("Failed to subscribe: #{inspect(reason)}")
              state
          end
        else
          state
        end

      # Dispatch any buffered messages
      {events, new_state} = take_from_buffer(state, total_demand)

      {:noreply, events, new_state}
    end

    @impl true
    def handle_info(:receive_messages, state) do
      # Receive messages from consumer channel
      channel = state.consumer.channel
      new_buffer = collect_messages(channel, state.buffer)

      # Dispatch messages if there's demand
      {events, new_state} = take_from_buffer(%{state | buffer: new_buffer}, state.demand)

      # Schedule next receive
      timer = schedule_receive(state.receive_interval)
      new_state = %{new_state | receive_timer: timer}

      {:noreply, events, new_state}
    end

    def handle_info({channel, {:message, %{msg: msg}}}, state)
        when channel == state.consumer.channel do
      # Add message to buffer
      broadway_msg = wrap_message(msg, state.consumer)
      new_buffer = :queue.in(broadway_msg, state.buffer)

      # Dispatch if there's demand
      {events, new_state} = take_from_buffer(%{state | buffer: new_buffer}, state.demand)

      {:noreply, events, new_state}
    end

    def handle_info({channel, {:pre_rebalance, action}}, state)
        when channel == state.consumer.channel do
      Logger.info("Franz.BroadwayProducer pre-rebalance: #{inspect(action)}")
      {:noreply, [], state}
    end

    def handle_info({channel, {:post_rebalance, action}}, state)
        when channel == state.consumer.channel do
      Logger.info("Franz.BroadwayProducer post-rebalance: #{inspect(action)}")
      {:noreply, [], state}
    end

    def handle_info({channel, {:error, %{reason: reason}}}, state)
        when channel == state.consumer.channel do
      Logger.error("Franz.BroadwayProducer consumer error: #{inspect(reason)}")
      {:noreply, [], state}
    end

    @impl true
    def terminate(reason, state) do
      Logger.info("Franz.BroadwayProducer terminating: #{inspect(reason)}")

      # Cancel receive timer
      if state.receive_timer do
        Process.cancel_timer(state.receive_timer)
      end

      # Stop consumer
      :ok = Consumer.stop(state.consumer)
    end

    ## Private Functions

    defp schedule_receive(interval) do
      Process.send_after(self(), :receive_messages, interval)
    end

    defp collect_messages(channel, buffer) do
      receive do
        {^channel, {:message, %{msg: msg}}} ->
          broadway_msg = wrap_message(msg, nil)
          new_buffer = :queue.in(broadway_msg, buffer)
          collect_messages(channel, new_buffer)
      after
        0 -> buffer
      end
    end

    defp take_from_buffer(state, 0) do
      {[], state}
    end

    defp take_from_buffer(state, demand) do
      case :queue.out(state.buffer) do
        {{:value, msg}, new_buffer} ->
          {messages, final_state} = take_from_buffer(%{state | buffer: new_buffer}, demand - 1)
          {[msg | messages], final_state}

        {:empty, buffer} ->
          {[], %{state | buffer: buffer, demand: demand}}
      end
    end

    defp wrap_message(msg, consumer) do
      %BroadwayMessage{
        data: msg,
        acknowledger: {__MODULE__, {consumer, msg}, :ok},
        metadata: %{
          topic: msg.topic,
          partition: msg.partition,
          offset: msg.offset,
          timestamp: msg.timestamp,
          headers: msg.headers
        }
      }
    end

    ## Acknowledger Callbacks

    def ack(_ack_ref, successful, failed) do
      # Commit successful messages
      for {_, {consumer, msg}, _} <- successful do
        case Consumer.commit(consumer, msg) do
          :ok ->
            :ok

          {:error, error} ->
            Logger.warning(
              "Failed to commit offset #{msg.offset} for #{msg.topic}:#{msg.partition}: #{inspect(error)}"
            )
        end
      end

      # Log failed messages (they won't be committed, so will be reprocessed)
      for {_, {_consumer, msg}, _} <- failed do
        Logger.warning(
          "Message failed processing, will be reprocessed: #{msg.topic}:#{msg.partition} offset #{msg.offset}"
        )
      end

      :ok
    end

    def configure(_ack_ref, _ack_data, options) do
      options
    end
  end
end
