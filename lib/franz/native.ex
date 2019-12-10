defmodule Franz.Native do
  @moduledoc false

  use Rustler, otp_app: :franz

  # Client NIFs
  def client_start(_config), do: err()
  def client_poll(_ref, _timeout), do: err()
  def client_pause(_ref, _tpl), do: err()
  def client_resume(_ref, _tpl), do: err()
  def client_stop(_ref), do: err()

  # Runtime NIFs
  def runtime_start, do: err()

  defp err, do: :erlang.nif_error(:franz_nif_not_loaded)
end
