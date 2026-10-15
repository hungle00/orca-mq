defmodule OrcaMQ do
  @moduledoc """
  Public API for OrcaMQ.
  Provides functions to publish messages, fetch them for consumers, and acknowledge them.
  """

  alias OrcaMQ.Consumer
  alias OrcaMQ.ConsumerGroup
  alias OrcaMQ.StreamServer

  @doc """
  Publishes a message to a stream.
  """
  @spec publish(String.t(), term()) :: {:ok, String.t()} | {:error, term()}
  def publish(stream, message) do
    StreamServer.publish(stream, message)
  end

  @doc """
  Fetches a batch of messages for a consumer.
  Starts the consumer group if it does not exist.
  """
  @spec fetch(String.t(), String.t(), pid(), pos_integer(), keyword()) ::
          [OrcaMQ.MessageStore.t()] | {:error, term()}
  def fetch(stream, group_name, consumer_pid, count \\ 10, opts \\ [])
      when is_binary(stream) and is_binary(group_name) and is_pid(consumer_pid) and count > 0 do
    with {:ok, _group_pid} <- ensure_consumer_group_started(stream, group_name, opts) do
      ConsumerGroup.fetch_messages(stream, group_name, consumer_pid, count)
    end
  end

  @doc """
  Subscribes a consumer to a stream with the given group name and handler function.
  Starts the consumer group if it does not exist, then starts a consumer worker.
  """
  @spec subscribe(String.t(), String.t(), (term() -> :ok | {:error, term()}), keyword()) ::
          {:ok, pid()} | {:error, term()}
  def subscribe(stream, group_name, handler_fn, opts \\ [])
      when is_binary(stream) and is_binary(group_name) and is_function(handler_fn, 1) do
    with {:ok, _group_pid} <- ensure_consumer_group_started(stream, group_name, opts) do
      consumer_opts =
        [
          stream: stream,
          group_name: group_name,
          handler_fn: handler_fn
        ]
        |> Keyword.merge(opts)

      Consumer.start_link(consumer_opts)
    end
  end

  @doc """
  Acknowledges a message that has been successfully processed by the consumer group.
  """
  @spec ack(String.t(), String.t(), String.t()) :: :ok | {:error, term()}
  def ack(stream, group_name, message_id)
      when is_binary(stream) and is_binary(group_name) and is_binary(message_id) do
    ConsumerGroup.ack(stream, group_name, message_id)
  end

  # Starts the ConsumerGroup if it is not already registered.
  defp ensure_consumer_group_started(stream, group_name, opts) do
    case GenServer.whereis(ConsumerGroup.via_tuple(stream, group_name)) do
      nil ->
        # If it is not running, start it under the dynamic supervisor.
        group_opts = [stream: stream, group_name: group_name] ++ Keyword.take(opts, [:start_id])
        DynamicSupervisor.start_child(OrcaMQ.GroupSupervisor, {ConsumerGroup, group_opts})

      pid ->
        {:ok, pid}
    end
  end
end
