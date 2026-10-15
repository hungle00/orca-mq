defmodule MiniBroker.StreamServer do
  @moduledoc """
  Publishes messages, generates stream IDs in the `timestamp_ms-sequence` format,
  and interacts with MessageStore.
  """

  use GenServer

  alias MiniBroker.MessageStore

  # State stores the last timestamp and sequence number for each stream.
  # State format: %{stream_name => {last_time_ms, sequence}}
  @type state :: %{binary() => {integer(), integer()}}

  # ===================================================================
  # Public API
  # ===================================================================

  @doc """
  Starts the StreamServer GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Publishes a payload to a stream.
  Returns `{:ok, message_id}` or `{:error, reason}`.
  """
  @spec publish(String.t(), term()) :: {:ok, String.t()}
  def publish(stream, payload) do
    GenServer.call(__MODULE__, {:publish, stream, payload})
  end

  @doc """
  Reads messages from a stream through MessageStore.
  """
  @spec read_stream(String.t(), keyword()) :: [MessageStore.t()]
  def read_stream(stream, opts \\ []) do
    MessageStore.list(stream, opts)
  end

  # ===================================================================
  # GenServer Callbacks
  # ===================================================================

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:publish, stream, payload}, _from, state) do
    {msg_id, new_state} = generate_id(stream, state)

    # Store the message in MessageStore.
    :ok = MessageStore.put(stream, msg_id, payload)

    {:reply, {:ok, msg_id}, new_state}
  end

  # ===================================================================
  # Internal Helpers
  # ===================================================================

  # Generate IDs in the "<timestamp_ms>-<sequence>" format.
  defp generate_id(stream, state) do
    now_ms = System.system_time(:millisecond)

    {last_ms, seq} = Map.get(state, stream, {0, 0})

    {new_ms, new_seq} =
      cond do
        now_ms > last_ms ->
          {now_ms, 0}

        now_ms == last_ms ->
          {now_ms, seq + 1}

        true ->
          # Handle clock drift or a system clock that moves backward.
          {last_ms, seq + 1}
      end

    msg_id = "#{new_ms}-#{new_seq}"
    updated_state = Map.put(state, stream, {new_ms, new_seq})

    {msg_id, updated_state}
  end
end
