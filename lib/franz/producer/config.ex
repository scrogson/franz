defmodule Franz.Producer.Config do
  defstruct bootstrap_servers: ""

  def new(opts \\ []) do
    struct(__MODULE__, opts)
  end
end
