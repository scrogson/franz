defmodule Franz.AdminTest do
  use ExUnit.Case, async: true
  import Franz.TestHelpers

  setup do
    {:ok, brokers: "127.0.0.1:9094"}
  end

  describe "list_topics/2" do
    test "lists topics in the cluster", %{brokers: brokers} do
      # Create a test topic
      topic = Franz.Utils.random_bytes()
      :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic})
      wait_for_topic(brokers, topic)

      on_exit(fn ->
        :ok = Franz.delete_topic(brokers, topic)
      end)

      # List topics should include our test topic
      assert {:ok, topics} = Franz.list_topics(brokers)
      assert is_list(topics)
      assert topic in topics
    end

    test "returns list when no topics exist or brokers available", %{brokers: brokers} do
      # Even if there are no user topics, internal topics may exist
      assert {:ok, topics} = Franz.list_topics(brokers)
      assert is_list(topics)
    end

    test "respects timeout option", %{brokers: brokers} do
      assert {:ok, _topics} = Franz.list_topics(brokers, timeout: 10_000)
    end

    test "returns error with invalid brokers" do
      # Should timeout or return error with invalid broker
      assert {:error, _} = Franz.list_topics("invalid:9999", timeout: 1000)
    end
  end

  describe "describe_cluster/2" do
    test "returns cluster metadata", %{brokers: brokers} do
      assert {:ok, metadata} = Franz.describe_cluster(brokers)

      assert %Franz.ClusterMetadata{} = metadata
      assert is_binary(metadata.cluster_id)
      assert is_integer(metadata.broker_count)
      assert metadata.broker_count > 0
      assert is_list(metadata.brokers)
      assert length(metadata.brokers) == metadata.broker_count
    end

    test "returns broker details", %{brokers: brokers} do
      assert {:ok, metadata} = Franz.describe_cluster(brokers)

      broker = hd(metadata.brokers)
      assert %Franz.BrokerMetadata{} = broker
      assert is_integer(broker.id)
      assert is_binary(broker.host)
      assert is_integer(broker.port)
      assert broker.port > 0
    end

    test "respects timeout option", %{brokers: brokers} do
      assert {:ok, _metadata} = Franz.describe_cluster(brokers, timeout: 10_000)
    end

    test "returns error with invalid brokers" do
      assert {:error, _} = Franz.describe_cluster("invalid:9999", timeout: 1000)
    end
  end

  describe "create_topics/2" do
    test "creates multiple topics at once", %{brokers: brokers} do
      topic1 = Franz.Utils.random_bytes()
      topic2 = Franz.Utils.random_bytes()

      on_exit(fn ->
        Franz.delete_topic(brokers, topic1)
        Franz.delete_topic(brokers, topic2)
      end)

      topics = [
        %Franz.NewTopic{name: topic1, num_partitions: 1},
        %Franz.NewTopic{name: topic2, num_partitions: 2}
      ]

      results = Franz.create_topics(brokers, topics)
      assert length(results) == 2
      assert Enum.all?(results, fn result -> match?({:ok, _}, result) end)
    end

    test "returns individual results for each topic", %{brokers: brokers} do
      existing_topic = Franz.Utils.random_bytes()
      new_topic = Franz.Utils.random_bytes()

      # Create first topic
      :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: existing_topic})
      wait_for_topic(brokers, existing_topic)

      on_exit(fn ->
        Franz.delete_topic(brokers, existing_topic)
        Franz.delete_topic(brokers, new_topic)
      end)

      # Try to create both (one will fail)
      results =
        Franz.create_topics(brokers, [
          %Franz.NewTopic{name: existing_topic},
          %Franz.NewTopic{name: new_topic}
        ])

      assert length(results) == 2
      # First should fail (already exists)
      assert match?({:error, _}, hd(results))
      # Second should succeed
      assert match?({:ok, _}, Enum.at(results, 1))
    end
  end

  describe "delete_topics/2" do
    test "deletes multiple topics at once", %{brokers: brokers} do
      topic1 = Franz.Utils.random_bytes()
      topic2 = Franz.Utils.random_bytes()

      # Create both topics
      :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic1})
      :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic2})
      wait_for_topics(brokers, [topic1, topic2])

      # Delete both at once
      results = Franz.delete_topics(brokers, [topic1, topic2])
      assert length(results) == 2
      assert Enum.all?(results, fn result -> match?({:ok, _}, result) end)
    end

    test "returns individual results for each topic", %{brokers: brokers} do
      existing_topic = Franz.Utils.random_bytes()
      non_existent_topic = Franz.Utils.random_bytes()

      # Create only one topic
      :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: existing_topic})
      wait_for_topic(brokers, existing_topic)

      # Try to delete both
      results = Franz.delete_topics(brokers, [existing_topic, non_existent_topic])

      assert length(results) == 2
      # First should succeed
      assert match?({:ok, _}, hd(results))
      # Second should fail (doesn't exist)
      assert match?({:error, _}, Enum.at(results, 1))
    end
  end

  describe "create_partitions/2" do
    test "adds partitions to existing topic", %{brokers: brokers} do
      topic = Franz.Utils.random_bytes()

      # Create topic with 1 partition
      :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic, num_partitions: 1})
      wait_for_topic(brokers, topic)

      on_exit(fn ->
        Franz.delete_topic(brokers, topic)
      end)

      # Add partitions to make it 5 total
      assert :ok =
               Franz.create_partitions(brokers, %Franz.NewPartitions{name: topic, total_count: 5})

      wait_for_topic(brokers, topic)

      # Verify by listing topics (we can't easily check partition count without consumer metadata)
      assert {:ok, topics} = Franz.list_topics(brokers)
      assert topic in topics
    end

    test "returns error when topic doesn't exist", %{brokers: brokers} do
      non_existent_topic = Franz.Utils.random_bytes()

      assert {:error, %Franz.Error{}} =
               Franz.create_partitions(brokers, %Franz.NewPartitions{
                 name: non_existent_topic,
                 total_count: 5
               })
    end

    test "returns error when reducing partition count", %{brokers: brokers} do
      topic = Franz.Utils.random_bytes()

      # Create topic with 5 partitions
      :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic, num_partitions: 5})
      wait_for_topic(brokers, topic)

      on_exit(fn ->
        Franz.delete_topic(brokers, topic)
      end)

      # Try to reduce to 3 (should fail - Kafka doesn't allow reducing partitions)
      assert {:error, %Franz.Error{}} =
               Franz.create_partitions(brokers, %Franz.NewPartitions{name: topic, total_count: 3})
    end
  end

  describe "create_partitions_batch/2" do
    test "adds partitions to multiple topics at once", %{brokers: brokers} do
      topic1 = Franz.Utils.random_bytes()
      topic2 = Franz.Utils.random_bytes()

      # Create both topics with 1 partition each
      :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic1, num_partitions: 1})
      :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: topic2, num_partitions: 1})
      wait_for_topics(brokers, [topic1, topic2])

      on_exit(fn ->
        Franz.delete_topic(brokers, topic1)
        Franz.delete_topic(brokers, topic2)
      end)

      # Add partitions to both
      results =
        Franz.create_partitions_batch(brokers, [
          %Franz.NewPartitions{name: topic1, total_count: 3},
          %Franz.NewPartitions{name: topic2, total_count: 4}
        ])

      assert length(results) == 2
      assert Enum.all?(results, fn result -> match?({:ok, _}, result) end)
    end

    test "returns individual results for each topic", %{brokers: brokers} do
      existing_topic = Franz.Utils.random_bytes()
      non_existent_topic = Franz.Utils.random_bytes()

      # Create only one topic
      :ok = Franz.create_topic(brokers, %Franz.NewTopic{name: existing_topic, num_partitions: 1})
      wait_for_topic(brokers, existing_topic)

      on_exit(fn ->
        Franz.delete_topic(brokers, existing_topic)
      end)

      # Try to add partitions to both
      results =
        Franz.create_partitions_batch(brokers, [
          %Franz.NewPartitions{name: existing_topic, total_count: 3},
          %Franz.NewPartitions{name: non_existent_topic, total_count: 5}
        ])

      assert length(results) == 2
      # First should succeed
      assert match?({:ok, _}, hd(results))
      # Second should fail (doesn't exist)
      assert match?({:error, _}, Enum.at(results, 1))
    end
  end
end
