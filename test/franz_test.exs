defmodule FranzTest do
  use ExUnit.Case
  doctest Franz

  setup do
    {:ok, brokers: "localhost:9092"}
  end

  test "creating/deleting topic", %{brokers: brokers} do
    topic = Franz.Utils.random_bytes()

    assert :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic})

    # Wait for the Broker to commit the new topic
    Process.sleep(100)

    assert :ok = Franz.delete_topic(brokers, topic)
  end

  test "creating/deleting multiple topics", %{brokers: brokers} do
    topic_a = Franz.Utils.random_bytes()
    topic_b = Franz.Utils.random_bytes()

    assert [{:ok, ^topic_a}, {:ok, ^topic_b}] =
             Franz.create_topics(brokers, [
               %Franz.NewTopic{name: topic_a},
               %Franz.NewTopic{name: topic_b}
             ])

    # Wait for the Broker to commit the new topic
    Process.sleep(100)

    assert [{:ok, ^topic_a}, {:ok, ^topic_b}] =
             Franz.delete_topics(brokers, [
               topic_a,
               topic_b
             ])
  end
end
