defmodule OrcaMQ.Consumer do
  @moduledoc """
  GenServer worker that receives pushed messages, invokes the handler, and acknowledges
  messages on success.
  """

  use GenServer
  require Logger

  alias OrcaMQ.ConsumerGroup

  @default_prefetch 10

  defstruct [:stream, :group_name, :handler_fn]

  @doc "Starts a consumer process."
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    stream = Keyword.fetch!(opts, :stream)
    group_name = Keyword.fetch!(opts, :group_name)
    handler_fn = Keyword.fetch!(opts, :handler_fn)
    prefetch = Keyword.get(opts, :prefetch, Keyword.get(opts, :batch_size, @default_prefetch))

    if not (is_integer(prefetch) and prefetch > 0) do
      {:stop, {:invalid_prefetch, prefetch}}
    else
      state = %__MODULE__{stream: stream, group_name: group_name, handler_fn: handler_fn}

      case ConsumerGroup.register_consumer(stream, group_name, self(), prefetch) do
        :ok -> {:ok, state}
        {:error, reason} -> {:stop, reason}
      end
    end
  end

  @impl true
  def handle_info({:deliver, message}, state) do
    process_message(message, state)
    {:noreply, state}
  end

  defp process_message(message, state) do
    case state.handler_fn.(message) do
      :ok ->
        ConsumerGroup.ack(state.stream, state.group_name, message.id)

      {:ok, _result} ->
        ConsumerGroup.ack(state.stream, state.group_name, message.id)

      {:error, reason} ->
        Logger.error("Consumer handler failed for msg #{message.id}: #{inspect(reason)}")
        handle_failure(state, message)

      other ->
        Logger.warning("Consumer handler returned unexpected value: #{inspect(other)}")
        handle_failure(state, message)
    end
  end

  defp handle_failure(state, message) do
    case ConsumerGroup.nack(state.stream, state.group_name, message.id) do
      :ok ->
        Logger.info("Message #{message.id} NACKed for retry.")

      {:ok, :dead_lettered} ->
        Logger.warning(
          "Message #{message.id} exhausted retries and was moved to the dead-letter stream."
        )

      {:error, :not_pending} ->
        Logger.warning("Message #{message.id} was not pending; cannot NACK.")

      {:error, reason} ->
        Logger.error("Failed to NACK message #{message.id}: #{inspect(reason)}")
    end
  end
end
