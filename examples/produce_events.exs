# Produce Events Example
#
# This script produces sample events to the "events" topic for testing
# the Broadway pipeline example.
#
# Usage: mix run examples/produce_events.exs

alias Franz.{Producer, Message}

# Start producer
{:ok, producer} =
  Producer.start(
    Producer.Config.new(
      bootstrap_servers: "localhost:9092",
      acks: :leader,
      compression_type: :lz4
    )
  )

IO.puts("=== Producing Events ===\n")

# Produce user signup events
for i <- 1..10 do
  event = %{
    type: "user_signup",
    user_id: "user_#{i}",
    email: "user#{i}@example.com",
    timestamp: System.system_time(:second)
  }

  payload = Jason.encode!(event)

  :ok =
    Producer.send_async(producer, %Message{
      topic: "events",
      partition: rem(i, 3),
      key: "user_#{i}",
      payload: payload
    })

  IO.puts("Produced user signup: user_#{i}")
end

# Produce page view events
for i <- 1..20 do
  event = %{
    type: "page_view",
    user_id: "user_#{rem(i, 10) + 1}",
    page: "/page_#{rem(i, 5)}",
    timestamp: System.system_time(:second)
  }

  payload = Jason.encode!(event)

  :ok =
    Producer.send_async(producer, %Message{
      topic: "events",
      partition: rem(i, 3),
      payload: payload
    })

  IO.puts("Produced page view: #{event.page}")
end

# Flush to ensure delivery
:ok = Producer.flush(producer)

IO.puts("\n=== Produced 30 events successfully ===")

Producer.stop(producer)
