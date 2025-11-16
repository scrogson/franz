defmodule Franz.DeliveryReceipt do
  @moduledoc """
  Receipt returned from a successful message delivery to Kafka.

  Contains metadata about where the message was written, providing confirmation
  of successful delivery and the exact location in the Kafka log.

  ## Fields

  - `:topic` - The topic name where the message was written
  - `:partition` - The partition number where the message was written
  - `:offset` - The offset assigned to the message within the partition
  - `:timestamp` - The message timestamp in milliseconds since epoch (may be nil)

  ## Usage

  The receipt is returned by `Franz.Producer.send/2`:

      {:ok, receipt} = Producer.send(producer, message)
      IO.inspect(receipt)
      # %Franz.DeliveryReceipt{
      #   topic: "events",
      #   partition: 0,
      #   offset: 123456,
      #   timestamp: 1234567890000
      # }

  ## Use Cases

  - **Idempotent producers**: Store the offset to avoid duplicate sends
  - **Exactly-once semantics**: Track which messages have been successfully delivered
  - **Debugging**: Verify messages are going to the expected partition
  - **Monitoring**: Track offset growth rate per partition
  - **Consumer coordination**: Pass offsets to consumers for targeted reads
  """

  defstruct topic: "",
            partition: 0,
            offset: 0,
            timestamp: nil

  @type t :: %__MODULE__{
          topic: String.t(),
          partition: non_neg_integer(),
          offset: non_neg_integer(),
          timestamp: integer() | nil
        }
end
