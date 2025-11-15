defmodule Franz.Error do
  @moduledoc """
  Structured error types for Franz Kafka operations.
  """

  defexception [:type, :message, :details]

  @type error_type ::
          :timeout
          | :network_error
          | :authentication_error
          | :authorization_error
          | :broker_not_available
          | :topic_not_found
          | :topic_already_exists
          | :partition_not_found
          | :offset_out_of_range
          | :group_coordinator_not_available
          | :invalid_config
          | :producer_error
          | :consumer_error
          | :unknown_error

  @type t :: %__MODULE__{
          type: error_type(),
          message: String.t(),
          details: map()
        }

  @doc """
  Creates a new Franz error.
  """
  @spec new(error_type(), String.t(), map()) :: t()
  def new(type, message, details \\ %{}) do
    %__MODULE__{
      type: type,
      message: message,
      details: details
    }
  end

  @doc """
  Parses a Kafka error string into a structured error.
  """
  @spec from_kafka_error(String.t()) :: t()
  def from_kafka_error(error_string) when is_binary(error_string) do
    cond do
      String.contains?(error_string, "TopicAlreadyExists") ->
        new(:topic_already_exists, "Topic already exists", %{raw: error_string})

      String.contains?(error_string, "UnknownTopicOrPartition") ->
        new(:topic_not_found, "Unknown topic or partition", %{raw: error_string})

      String.contains?(error_string, "BrokerNotAvailable") ->
        new(:broker_not_available, "Broker not available", %{raw: error_string})

      String.contains?(error_string, "Authentication") ->
        new(:authentication_error, "Authentication failed", %{raw: error_string})

      String.contains?(error_string, "Authorization") ->
        new(:authorization_error, "Authorization failed", %{raw: error_string})

      String.contains?(error_string, "GroupCoordinator") ->
        new(:group_coordinator_not_available, "Group coordinator not available", %{
          raw: error_string
        })

      String.contains?(error_string, "OffsetOutOfRange") ->
        new(:offset_out_of_range, "Offset out of range", %{raw: error_string})

      String.contains?(error_string, "Network") or String.contains?(error_string, "Timeout") ->
        new(:network_error, "Network error", %{raw: error_string})

      true ->
        new(:unknown_error, error_string, %{raw: error_string})
    end
  end

  def from_kafka_error(error) do
    new(:unknown_error, "Unknown error", %{raw: error})
  end

  @impl true
  def message(%__MODULE__{type: type, message: msg, details: details}) do
    base = "[#{type}] #{msg}"

    if map_size(details) > 0 and not Map.has_key?(details, :raw) do
      base <> " - " <> inspect(details)
    else
      base
    end
  end

  @doc """
  Wraps an error tuple with a structured Franz.Error.
  """
  @spec wrap({:error, term()}) :: {:error, t()}
  def wrap({:error, %__MODULE__{} = error}), do: {:error, error}

  def wrap({:error, error}) when is_binary(error) do
    {:error, from_kafka_error(error)}
  end

  def wrap({:error, :timeout}) do
    {:error, new(:timeout, "Operation timed out")}
  end

  def wrap({:error, {:unexpected_message, msg}}) do
    {:error, new(:unknown_error, "Unexpected message", %{message: msg})}
  end

  def wrap({:error, error}) do
    {:error, new(:unknown_error, "Unknown error", %{error: error})}
  end

  def wrap(other), do: other
end
