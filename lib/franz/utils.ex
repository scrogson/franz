defmodule Franz.Utils do
  def random_bytes(n \\ 8) do
    n
    |> :crypto.strong_rand_bytes()
    |> Base.encode16(case: :lower)
  end
end
