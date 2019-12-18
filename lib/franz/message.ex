defmodule Franz.Message do
  defstruct payload: nil,
            key: nil,
            topic: "",
            timestamp: 0,
            partition: 0,
            offset: 0,
            headers: []

  @type t :: %Franz.Message{
          payload: nil | binary(),
          key: nil | binary(),
          topic: String.t(),
          timestamp: number(),
          partition: number(),
          offset: number(),
          headers: [{String.t(), String.t()}]
        }
end
