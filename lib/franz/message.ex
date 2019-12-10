defmodule Franz.Message do
  defstruct [
    payload: nil,
    key: nil,
    topic: "",
    #timestamp: 0,
    #partition: 0,
    #offset: 0,
    #headers: [],
  ]
end
