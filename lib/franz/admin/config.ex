defmodule Franz.Admin.Config do
  defstruct bootstrap_servers: ""

  @type t :: %__MODULE__{}

  def new(opts \\ []) do
    struct(__MODULE__, opts)
  end
end
