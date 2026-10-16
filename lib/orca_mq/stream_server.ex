defmodule OrcaMQ.StreamServer do
  @moduledoc """
  Publishes messages, generates stream IDs in the `timestamp_ms-sequence` format,
  and interacts with MessageStore.
  """

  use GenServer

  alias OrcaMQ.MessageStore

  @notification_batch_ms 1

  @type state :: %{
          ids: %{binary() => {integer(), integer()}},
          pending_notifications: MapSet.t(binary()),
          notification_timer: reference() | nil
        }

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Publishes a payload to a stream and returns its generated message ID."
  @spec publish(String.t(), term()) :: {:ok, String.t()}
  def publish(stream, payload) do
    GenServer.call(__MODULE__, {:publish, stream, payload})
  end

  @doc "Reads messages from a stream through MessageStore."
  @spec read_stream(String.t(), keyword()) :: [MessageStore.t()]
  def read_stream(stream, opts \\ []) do
    MessageStore.list(stream, opts)
  end

  @impl true
  def init(_opts) do
    {:ok, %{ids: %{}, pending_notifications: MapSet.new(), notification_timer: nil}}
  end

  @impl true
  def handle_call({:publish, stream, payload}, _from, state) do
    {msg_id, ids} = generate_id(stream, state.ids)
    :ok = MessageStore.put(stream, msg_id, payload)

    new_state =
      state
      |> Map.put(:ids, ids)
      |> queue_notification(stream)

    {:reply, {:ok, msg_id}, new_state}
  end

  @impl true
  def handle_info(:flush_notifications, state) do
    Enum.each(state.pending_notifications, &notify_consumers/1)
    {:noreply, %{state | pending_notifications: MapSet.new(), notification_timer: nil}}
  end

  defp queue_notification(%{notification_timer: nil} = state, stream) do
    timer = Process.send_after(self(), :flush_notifications, @notification_batch_ms)

    %{
      state
      | pending_notifications: MapSet.put(state.pending_notifications, stream),
        notification_timer: timer
    }
  end

  defp queue_notification(state, stream) do
    %{state | pending_notifications: MapSet.put(state.pending_notifications, stream)}
  end

  defp notify_consumers(stream) do
    Registry.dispatch(OrcaMQ.StreamRegistry, stream, fn entries ->
      Enum.each(entries, fn {pid, _value} -> send(pid, :messages_available) end)
    end)
  end

  defp generate_id(stream, state) do
    now_ms = System.system_time(:millisecond)
    {last_ms, seq} = Map.get(state, stream, {0, 0})

    {new_ms, new_seq} =
      cond do
        now_ms > last_ms -> {now_ms, 0}
        now_ms == last_ms -> {now_ms, seq + 1}
        true -> {last_ms, seq + 1}
      end

    msg_id = "#{new_ms}-#{new_seq}"
    {msg_id, Map.put(state, stream, {new_ms, new_seq})}
  end
end
