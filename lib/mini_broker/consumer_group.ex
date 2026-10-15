defmodule MiniBroker.ConsumerGroup do
  @moduledoc """
  Manages a consumer group and stores its delivery offset and pending messages.
  """

  use GenServer

  alias MiniBroker.MessageStore

  defstruct [
    :stream,
    :group_name,
    :last_delivered_id,
    # %{message_id => {consumer_pid, delivered_at_ms}}
    pending: %{}
  ]

  @type t :: %__MODULE__{
          stream: String.t(),
          group_name: String.t(),
          last_delivered_id: String.t() | nil,
          pending: %{binary() => {pid(), integer()}}
        }

  # ===================================================================
  # Public API
  # ===================================================================

  @doc """
  Starts the ConsumerGroup GenServer.
  """
  def start_link(opts) do
    stream = Keyword.fetch!(opts, :stream)
    group_name = Keyword.fetch!(opts, :group_name)
    GenServer.start_link(__MODULE__, opts, name: via_tuple(stream, group_name))
  end

  def fetch_messages(stream, group_name, consumer_pid, count \\ 1) do
    case GenServer.whereis(via_tuple(stream, group_name)) do
      nil -> {:error, :group_not_found}
      pid -> GenServer.call(pid, {:fetch_messages, consumer_pid, count})
    end
  end

  def ack(stream, group_name, message_id) do
    case GenServer.whereis(via_tuple(stream, group_name)) do
      nil -> {:error, :group_not_found}
      pid -> GenServer.call(pid, {:ack, message_id})
    end
  end

  @doc """
  Returns the current consumer group state, including its offset and pending entries list.
  """
  @spec info(String.t(), String.t()) :: map()
  def info(stream, group_name) do
    GenServer.call(via_tuple(stream, group_name), :info)
  end

  def via_tuple(stream, group_name) do
    {:via, Registry, {MiniBroker.GroupRegistry, {stream, group_name}}}
  end

  # ===================================================================
  # GenServer Callbacks
  # ===================================================================

  @impl true
  def init(opts) do
    stream = Keyword.fetch!(opts, :stream)
    group_name = Keyword.fetch!(opts, :group_name)

    # Default the initial offset to "0-0" to read from the beginning of the stream.
    last_delivered_id = Keyword.get(opts, :start_id, "0-0")

    state = %__MODULE__{
      stream: stream,
      group_name: group_name,
      last_delivered_id: last_delivered_id,
      pending: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:fetch_messages, consumer_pid, count}, _from, state) do
    # Read messages from MessageStore whose IDs are greater than last_delivered_id.
    all_messages = MessageStore.list(state.stream, limit: 1000)

    undelivered =
      Enum.filter(all_messages, fn msg ->
        compare_ids(msg.id, state.last_delivered_id) == :gt
      end)
      |> Enum.take(count)

    now = System.system_time(:millisecond)

    # Update the pending entries list and the latest delivered ID.
    {new_pending, new_last_id} =
      Enum.reduce(undelivered, {state.pending, state.last_delivered_id}, fn msg,
                                                                            {acc_pel, _last_id} ->
        {Map.put(acc_pel, msg.id, {consumer_pid, now}), msg.id}
      end)

    new_state = %{state | pending: new_pending, last_delivered_id: new_last_id}
    {:reply, undelivered, new_state}
  end

  @impl true
  def handle_call({:ack, message_id}, _from, state) do
    if Map.has_key?(state.pending, message_id) do
      new_pending = Map.delete(state.pending, message_id)
      {:reply, :ok, %{state | pending: new_pending}}
    else
      {:reply, {:error, :not_pending}, state}
    end
  end

  @impl true
  def handle_call(:info, _from, state) do
    info_map = %{
      stream: state.stream,
      group_name: state.group_name,
      last_delivered_id: state.last_delivered_id,
      pending_count: map_size(state.pending),
      pending_entries: state.pending
    }

    {:reply, info_map, state}
  end

  # ===================================================================
  # Helpers
  # ===================================================================

  # Compare two stream IDs in "timestamp-sequence" format.
  defp compare_ids(id1, id2) do
    [ts1, seq1] = String.split(id1, "-") |> Enum.map(&String.to_integer/1)
    [ts2, seq2] = String.split(id2, "-") |> Enum.map(&String.to_integer/1)

    cond do
      ts1 > ts2 -> :gt
      ts1 < ts2 -> :lt
      seq1 > seq2 -> :gt
      seq1 < seq2 -> :lt
      true -> :eq
    end
  end
end
