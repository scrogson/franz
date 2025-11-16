defmodule Franz.TestHelpers do
  @moduledoc """
  Helper functions for Franz tests to reduce test time.
  """

  @doc """
  Wait for a topic to be available by polling list_topics.
  This is faster than a fixed sleep after creating topics.
  """
  def wait_for_topic(brokers, topic, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5000)
    interval = Keyword.get(opts, :interval, 50)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_wait_for_topic(brokers, topic, deadline, interval)
  end

  defp do_wait_for_topic(brokers, topic, deadline, interval) do
    case Franz.list_topics(brokers, timeout: 1000) do
      {:ok, topics} when is_list(topics) ->
        if topic in topics do
          :ok
        else
          if System.monotonic_time(:millisecond) < deadline do
            Process.sleep(interval)
            do_wait_for_topic(brokers, topic, deadline, interval)
          else
            {:error, :timeout}
          end
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Wait for multiple topics to be available.
  """
  def wait_for_topics(brokers, topics, opts \\ []) when is_list(topics) do
    Enum.reduce_while(topics, :ok, fn topic, :ok ->
      case wait_for_topic(brokers, topic, opts) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end
