defmodule OrcaMQ.ConsumerGroup do
  @moduledoc """
  Manages a consumer group, its delivery offset, pending messages, and registered consumers.
  """

  use GenServer
  require Logger

  @default_max_retries 3
  @default_max_consumers 100
  @default_retry_backoff_ms 100
  @default_max_retry_backoff_ms 5_000

  alias OrcaMQ.MessageStore
  alias OrcaMQ.StreamServer

  defstruct [
    :stream,
    :group_name,
    :last_delivered_id,
    :max_retries,
    :max_consumers,
    :retry_backoff_ms,
    :max_retry_backoff_ms,
    :retry_timer,
    pending: %{},
    consumers: %{}
  ]

  @type t :: %__MODULE__{
          stream: String.t(),
          group_name: String.t(),
          last_delivered_id: String.t() | nil,
          max_retries: integer(),
          pending: %{
            binary() => %{
              consumer_pid: pid() | nil,
              delivered_at: integer(),
              retry_count: non_neg_integer(),
              retry_at: integer() | nil
            }
          },
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

  def nack(stream, group_name, message_id) do
    call_group(stream, group_name, {:nack, message_id})
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
  @spec init(keyword()) :: {:ok, OrcaMQ.ConsumerGroup.t()}
  def init(opts) do
    stream = Keyword.fetch!(opts, :stream)
    group_name = Keyword.fetch!(opts, :group_name)
    max_retries = Keyword.get(opts, :max_retries, @default_max_retries)
    max_consumers = Keyword.get(opts, :max_consumers, @default_max_consumers)
    retry_backoff_ms = Keyword.get(opts, :retry_backoff_ms, @default_retry_backoff_ms)
    max_retry_backoff_ms = Keyword.get(opts, :max_retry_backoff_ms, @default_max_retry_backoff_ms)

    if is_integer(max_retries) and max_retries >= 0 and is_integer(max_consumers) and
         max_consumers > 0 and is_integer(retry_backoff_ms) and retry_backoff_ms > 0 and
         is_integer(max_retry_backoff_ms) and max_retry_backoff_ms >= retry_backoff_ms do
      {:ok, _registry_pid} = Registry.register(OrcaMQ.StreamRegistry, stream, nil)

      state = %__MODULE__{
        stream: stream,
        group_name: group_name,
        last_delivered_id: Keyword.get(opts, :start_id, "0-0"),
        max_retries: max_retries,
        max_consumers: max_consumers,
        retry_backoff_ms: retry_backoff_ms,
        max_retry_backoff_ms: max_retry_backoff_ms
      }

      {:ok, state}
    else
      {:stop, {:invalid_consumer_group_options, opts}}
    end
  end

  @impl true
  def handle_call({:register_consumer, consumer_pid, prefetch}, _from, state) do
    state = prune_dead_consumers(state)

    if not Map.has_key?(state.consumers, consumer_pid) and
         map_size(state.consumers) >= state.max_consumers do
      {:reply, {:error, :max_consumers_reached}, state}
    else
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

  def handle_call({:nack, message_id}, _from, state) do
    case Map.get(state.pending, message_id) do
      nil ->
        {:reply, {:error, :not_pending}, state}

      metadata ->
        handle_nack(state, message_id, metadata)
    end
  end

  def handle_call(:info, _from, state) do
    info_map = %{
      stream: state.stream,
      group_name: state.group_name,
      last_delivered_id: state.last_delivered_id,
      pending_count: map_size(state.pending),
      pending_entries: state.pending,
      consumer_count: map_size(state.consumers),
      max_consumers: state.max_consumers
    }

    {:reply, info_map, state}
  end

  @impl true
  def handle_info(:messages_available, state) do
    flush_messages_available()
    {:noreply, dispatch_available(state)}
  end

  def handle_info({:retry_pending, token}, %{retry_timer: %{token: token}} = state) do
    {:noreply, dispatch_available(%{state | retry_timer: nil})}
  end

  def handle_info({:retry_pending, _token}, state), do: {:noreply, state}

  def handle_info({:DOWN, monitor_ref, :process, consumer_pid, _reason}, state) do
    case Map.get(state.consumers, consumer_pid) do
      %{monitor_ref: ^monitor_ref} ->
        pending =
          Map.new(state.pending, fn
            {message_id, %{consumer_pid: ^consumer_pid} = metadata} ->
              {message_id,
               %{
                 metadata
                 | consumer_pid: nil,
                   retry_at:
                     if(metadata.retry_count >= state.max_retries,
                       do: now_ms(),
                       else: retry_at(state, metadata.retry_count)
                     )
               }}

            entry ->
              entry
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

  defp prune_dead_consumers(state) do
    state.consumers
    |> Enum.reject(fn {consumer_pid, _consumer} -> Process.alive?(consumer_pid) end)
    |> Enum.reduce(state, fn {consumer_pid, consumer}, acc ->
      Process.demonitor(consumer.monitor_ref, [:flush])

      pending =
        Map.new(acc.pending, fn
          {message_id, %{consumer_pid: ^consumer_pid} = metadata} ->
            {message_id,
             %{
               metadata
               | consumer_pid: nil,
                 retry_at:
                   if(metadata.retry_count >= acc.max_retries,
                     do: now_ms(),
                     else: retry_at(acc, metadata.retry_count)
                   )
             }}

          entry ->
            entry
        end)

      %{acc | consumers: Map.delete(acc.consumers, consumer_pid), pending: pending}
    end)
  end

  defp dispatch_available(state) do
    state =
      Enum.reduce(Map.keys(state.consumers), state, fn consumer_pid, acc ->
        dispatch_to_consumer(acc, consumer_pid)
      end)

    schedule_retry(state)
  end

  defp dispatch_to_consumer(state, consumer_pid) do
    consumer = Map.fetch!(state.consumers, consumer_pid)

    in_flight =
      Enum.count(state.pending, fn {_id, %{consumer_pid: owner}} -> owner == consumer_pid end)

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
    |> Enum.filter(fn {_id, %{consumer_pid: owner} = metadata} ->
      is_nil(owner) and (is_nil(metadata.retry_at) or metadata.retry_at <= now_ms())
    end)
    |> Enum.sort_by(fn {id, _entry} -> stream_id_key(id) end)
    |> Enum.take(available)
    |> Enum.reduce({state, available}, fn {message_id, metadata}, {acc, slots} ->
      if metadata.retry_count >= acc.max_retries do
        case publish_dead_letter(acc, message_id, metadata.retry_count) do
          {:ok, new_state} ->
            {new_state, slots}

          {:error, reason} ->
            Logger.error("Could not dead-letter message #{message_id}: #{inspect(reason)}")

            pending =
              Map.put(acc.pending, message_id, %{
                metadata
                | retry_at: retry_at(acc, metadata.retry_count)
              })

            {%{acc | pending: pending}, slots}
        end
      else
        case MessageStore.get(acc.stream, message_id) do
          {:ok, message} ->
            send(consumer_pid, {:deliver, message})

            pending =
              Map.put(acc.pending, message_id, %{
                consumer_pid: consumer_pid,
                delivered_at: System.system_time(:millisecond),
                retry_count: metadata.retry_count + 1,
                retry_at: nil
              })

            {%{acc | pending: pending}, slots - 1}

          {:error, reason} ->
            Logger.error("Could not redeliver pending message #{message_id}: #{inspect(reason)}")

            pending =
              Map.put(acc.pending, message_id, %{
                metadata
                | retry_at: retry_at(acc, metadata.retry_count)
              })

            {%{acc | pending: pending}, slots}
        end
      end
    end)
  end

  defp handle_nack(state, message_id, metadata) do
    elapsed = System.system_time(:millisecond) - metadata.delivered_at

    if elapsed > 0 do
      Logger.info(
        "Message #{message_id} NACKed after #{elapsed} ms by #{inspect(metadata.consumer_pid)}"
      )
    end

    if metadata.retry_count >= state.max_retries do
      case publish_dead_letter(state, message_id, metadata.retry_count) do
        {:ok, state} ->
          state = dispatch_available(state)
          {:reply, {:ok, :dead_lettered}, state}

        {:error, reason} ->
          pending =
            Map.put(state.pending, message_id, %{
              metadata
              | consumer_pid: nil,
                retry_at: retry_at(state, metadata.retry_count)
            })

          state = %{state | pending: pending} |> dispatch_available()
          {:reply, {:error, reason}, state}
      end
    else
      Logger.info(
        "Message #{message_id} NACKed by #{inspect(metadata.consumer_pid)}. " <>
          "Retry count: #{metadata.retry_count}/#{state.max_retries}"
      )

      pending =
        Map.put(state.pending, message_id, %{
          metadata
          | consumer_pid: nil,
            retry_at: retry_at(state, metadata.retry_count)
        })

      state = %{state | pending: pending} |> dispatch_available()
      {:reply, :ok, state}
    end
  end

  defp publish_dead_letter(state, message_id, retry_count) do
    with {:ok, message} <- MessageStore.get(state.stream, message_id),
         {:ok, dls_id} <-
           StreamServer.publish("#{state.stream}:dead_letter", %{
             original_msg: message.payload,
             original_id: message_id,
             failed_group: state.group_name,
             failed_at: DateTime.utc_now(),
             total_retries: retry_count
           }) do
      Logger.warning(
        "Message #{message_id} exceeded max retries (#{state.max_retries}). " <>
          "Moved to dead-letter stream with ID #{dls_id}."
      )

      pending = Map.delete(state.pending, message_id)
      {:ok, %{state | pending: pending}}
    else
      {:error, :not_found} ->
        {:error, :message_not_found}
    end
  end

  defp fetch_new_messages(state, consumer_pid, count, push?) do
    messages = MessageStore.list(state.stream, after_id: state.last_delivered_id, limit: count)
    now = System.system_time(:millisecond)

    {pending, last_delivered_id} =
      Enum.reduce(messages, {state.pending, state.last_delivered_id}, fn message,
                                                                         {pending, _last_id} ->
        if push?, do: send(consumer_pid, {:deliver, message})

        metadata = %{
          consumer_pid: consumer_pid,
          delivered_at: now,
          retry_count: 0,
          retry_at: nil
        }

        {Map.put(pending, message.id, metadata), message.id}
      end)

    {messages, %{state | pending: pending, last_delivered_id: last_delivered_id}}
  end

  defp schedule_retry(state) do
    state = cancel_retry_timer(state)
    now = now_ms()

    next_retry_at =
      state.pending
      |> Enum.map(fn {_id, metadata} -> metadata end)
      |> Enum.filter(fn metadata ->
        is_nil(metadata.consumer_pid) and is_integer(metadata.retry_at)
      end)
      |> Enum.map(& &1.retry_at)
      |> Enum.min(fn -> nil end)

    case {next_retry_at, has_retry_capacity?(state)} do
      {nil, _} ->
        state

      {_retry_at, false} ->
        state

      {retry_at, true} ->
        token = make_ref()
        timer_ref = Process.send_after(self(), {:retry_pending, token}, max(retry_at - now, 0))
        %{state | retry_timer: %{ref: timer_ref, token: token}}
    end
  end

  defp has_retry_capacity?(state) do
    Enum.any?(state.consumers, fn {consumer_pid, consumer} ->
      in_flight =
        Enum.count(state.pending, fn {_id, %{consumer_pid: owner}} ->
          owner == consumer_pid
        end)

      consumer.prefetch > in_flight
    end)
  end

  defp cancel_retry_timer(%{retry_timer: nil} = state), do: state

  defp cancel_retry_timer(%{retry_timer: %{ref: timer_ref}} = state) do
    Process.cancel_timer(timer_ref)
    %{state | retry_timer: nil}
  end

  defp retry_at(state, retry_count) do
    now_ms() + retry_delay(state.retry_backoff_ms, retry_count, state.max_retry_backoff_ms)
  end

  defp retry_delay(delay, 0, max_delay), do: min(delay, max_delay)
  defp retry_delay(delay, _attempt, max_delay) when delay >= div(max_delay + 1, 2), do: max_delay
  defp retry_delay(delay, attempt, max_delay), do: retry_delay(delay * 2, attempt - 1, max_delay)

  defp now_ms, do: System.monotonic_time(:millisecond)

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
