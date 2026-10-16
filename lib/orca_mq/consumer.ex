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

      other ->
        Logger.warning("Consumer handler returned unexpected value: #{inspect(other)}")
    end
  end
end
