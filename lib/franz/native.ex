defmodule Franz.Native do
  @moduledoc false

  use Rustler, otp_app: :franz

  # Admin Client NIFs
  def admin_start(_config), do: err()
  def admin_stop(_ref), do: err()
  def create_topics(_ref, _new_topics), do: err()
  def delete_topics(_ref, _topics), do: err()
  def create_partitions(_ref, _new_partitions), do: err()
  def describe_broker(_ref, _broker), do: err()
  def describe_group(_ref, _group), do: err()
  def describe_topic(_ref, _topic), do: err()
  def alter_configs(_ref, _alter_config), do: err()

  # Consumer NIFs
  def consumer_start(_config), do: err()
  def consumer_assign(_ref, _timeout), do: err()
  def consumer_subscribe(_ref, _topics), do: err()
  def consumer_unsubscribe(_ref), do: err()
  def consumer_assignment(_ref), do: err()
  def consumer_poll(_ref), do: err()
  def consumer_commit(_ref, {_topic, _partition, _offset}), do: err()
  def consumer_committed(_ref, _timeout), do: err()
  def consumer_pause(_ref, _tpl), do: err()
  def consumer_resume(_ref, _tpl), do: err()
  def consumer_stop(_ref), do: err()

  # Producer NIFs
  def producer_start(_config), do: err()
  def producer_send(_ref, _message), do: err()
  def producer_stop(_ref), do: err()

  defp err, do: :erlang.nif_error(:franz_nif_not_loaded)
end
