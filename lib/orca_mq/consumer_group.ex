defmodule OrcaMQ.ConsumerGroup do
  @moduledoc """
  Manages a consumer group, its delivery offset, pending messages, and registered consumers.
  """

  use GenServer
  require Logger

  alias OrcaMQ.MessageStore

  defstruct [
    :stream,
    :group_name,
    :last_delivered_id,
    pending: %{},
    consumers: %{}
  ]

  @type t :: %__MODULE__{
          stream: String.t(),
          group_name: String.t(),
          last_delivered_id: String.t() | nil,
          pending: %{binary() => {pid() | nil, integer()}},
          consumers: %{pid() => %{prefetch: pos_integer(), monitor_ref: reference()}}
        }

  @spec start_link(keyword()) :: :ignore | {:error, any()} | {:ok, pid()}
  @doc "Starts the ConsumerGroup GenServer."
  def start_link(opts) do
    stream = Keyword.fetch!(opts, :stream)
    group_name = Keyword.fetch!(opts, :group_name)
    GenServer.start_link(__MODULE__, opts, name: via_tuple(stream, group_name))
  end

  @doc "Registers an internal consumer with its maximum number of unacknowledged messages."
  def register_consumer(stream, group_name, consumer_pid, prefetch)
      when is_pid(consumer_pid) and is_integer(prefetch) and prefetch > 0 do
    call_group(stream, group_name, {:register_consumer, consumer_pid, prefetch})
  end

  def fetch_messages(stream, group_name, consumer_pid, count \\ 1) do
    call_group(stream, group_name, {:fetch_messages, consumer_pid, count})
  end

  def ack(stream, group_name, message_id) do
    call_group(stream, group_name, {:ack, message_id})
  end

  @doc "Returns the current consumer group state, including its offset and pending entries list."
  @spec info(String.t(), String.t()) :: map()
  def info(stream, group_name) do
    GenServer.call(via_tuple(stream, group_name), :info)
  end

  def via_tuple(stream, group_name) do
    {:via, Registry, {OrcaMQ.GroupRegistry, {stream, group_name}}}
  end

  defp call_group(stream, group_name, request) do
    case GenServer.whereis(via_tuple(stream, group_name)) do
      nil -> {:error, :group_not_found}
      pid -> GenServer.call(pid, request)
    end
  end

  @impl true
  def init(opts) do
    stream = Keyword.fetch!(opts, :stream)
    group_name = Keyword.fetch!(opts, :group_name)
    {:ok, _registry_pid} = Registry.register(OrcaMQ.StreamRegistry, stream, nil)

    state = %__MODULE__{
      stream: stream,
      group_name: group_name,
      last_delivered_id: Keyword.get(opts, :start_id, "0-0")
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:register_consumer, consumer_pid, prefetch}, _from, state) do
    consumers =
      case Map.get(state.consumers, consumer_pid) do
        nil ->
          Map.put(state.consumers, consumer_pid, %{
            prefetch: prefetch,
            monitor_ref: Process.monitor(consumer_pid)
          })

        consumer ->
          Map.put(state.consumers, consumer_pid, %{consumer | prefetch: prefetch})
      end

    state = %{state | consumers: consumers} |> dispatch_available()
    {:reply, :ok, state}
  end

  def handle_call({:fetch_messages, consumer_pid, count}, _from, state) do
    {messages, state} = fetch_new_messages(state, consumer_pid, count, false)
    {:reply, messages, state}
  end

  def handle_call({:ack, message_id}, _from, state) do
    case Map.pop(state.pending, message_id) do
      {nil, _pending} ->
        {:reply, {:error, :not_pending}, state}

      {_entry, pending} ->
        state = %{state | pending: pending} |> dispatch_available()
        {:reply, :ok, state}
    end
  end

  def handle_call(:info, _from, state) do
    info_map = %{
      stream: state.stream,
      group_name: state.group_name,
      last_delivered_id: state.last_delivered_id,
      pending_count: map_size(state.pending),
      pending_entries: state.pending,
      consumer_count: map_size(state.consumers)
    }

    {:reply, info_map, state}
  end

  @impl true
  def handle_info(:messages_available, state) do
    flush_messages_available()
    {:noreply, dispatch_available(state)}
  end

  def handle_info({:DOWN, monitor_ref, :process, consumer_pid, _reason}, state) do
    case Map.get(state.consumers, consumer_pid) do
      %{monitor_ref: ^monitor_ref} ->
        pending =
          Map.new(state.pending, fn
            {message_id, {^consumer_pid, delivered_at}} -> {message_id, {nil, delivered_at}}
            entry -> entry
          end)

        state = %{state | consumers: Map.delete(state.consumers, consumer_pid), pending: pending}
        {:noreply, dispatch_available(state)}

      _ ->
        {:noreply, state}
    end
  end

  defp flush_messages_available do
    receive do
      :messages_available -> flush_messages_available()
    after
      0 -> :ok
    end
  end

  defp dispatch_available(state) do
    Enum.reduce(Map.keys(state.consumers), state, fn consumer_pid, acc ->
      dispatch_to_consumer(acc, consumer_pid)
    end)
  end

  defp dispatch_to_consumer(state, consumer_pid) do
    consumer = Map.fetch!(state.consumers, consumer_pid)
    in_flight = Enum.count(state.pending, fn {_id, {owner, _time}} -> owner == consumer_pid end)
    available = max(consumer.prefetch - in_flight, 0)
    {state, available} = redeliver_pending(state, consumer_pid, available)

    min_refill = max(div(consumer.prefetch, 2), 1)

    if available >= min_refill do
      {_messages, state} = fetch_new_messages(state, consumer_pid, available, true)
      state
    else
      state
    end
  end

  defp redeliver_pending(state, _consumer_pid, 0), do: {state, 0}

  defp redeliver_pending(state, consumer_pid, available) do
    state.pending
    |> Enum.filter(fn {_id, {owner, _time}} -> is_nil(owner) end)
    |> Enum.sort_by(fn {id, _entry} -> stream_id_key(id) end)
    |> Enum.take(available)
    |> Enum.reduce({state, available}, fn {message_id, {_owner, delivered_at}}, {acc, slots} ->
      case MessageStore.get(acc.stream, message_id) do
        {:ok, message} ->
          send(consumer_pid, {:deliver, message})
          pending = Map.put(acc.pending, message_id, {consumer_pid, delivered_at})
          {%{acc | pending: pending}, slots - 1}

        {:error, reason} ->
          Logger.error("Could not redeliver pending message #{message_id}: #{inspect(reason)}")
          {acc, slots}
      end
    end)
  end

  defp fetch_new_messages(state, consumer_pid, count, push?) do
    messages = MessageStore.list(state.stream, after_id: state.last_delivered_id, limit: count)
    now = System.system_time(:millisecond)

    {pending, last_delivered_id} =
      Enum.reduce(messages, {state.pending, state.last_delivered_id}, fn message,
                                                                         {pending, _last_id} ->
        if push?, do: send(consumer_pid, {:deliver, message})
        {Map.put(pending, message.id, {consumer_pid, now}), message.id}
      end)

    {messages, %{state | pending: pending, last_delivered_id: last_delivered_id}}
  end

  defp stream_id_key(id) when is_integer(id), do: {0, id, 0}

  defp stream_id_key(id) when is_binary(id) do
    case String.split(id, "-", parts: 2) do
      [timestamp, sequence] ->
        with {timestamp, ""} <- Integer.parse(timestamp),
             {sequence, ""} <- Integer.parse(sequence) do
          {0, timestamp, sequence}
        else
          _ -> {1, id}
        end

      _ ->
        {1, id}
    end
  end
end
