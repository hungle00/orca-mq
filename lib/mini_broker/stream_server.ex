defmodule MiniBroker.StreamServer do
  @moduledoc """
  Quản lý việc publish message, sinh Message ID chuẩn (timestamp_ms-seq)
  và tương tác với MessageStore.
  """

  use GenServer

  alias MiniBroker.MessageStore

  # State lưu trữ last_timestamp và sequence cho từng stream
  # Format state: %{stream_name => {last_time_ms, seq}}
  @type state :: %{binary() => {integer(), integer()}}

  # ===================================================================
  # Public API
  # ===================================================================

  @doc """
  Khởi chạy StreamServer GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Publish một payload vào stream.
  Trả về {:ok, message_id} hoặc {:error, reason}.
  """
  @spec publish(String.t(), term()) :: {:ok, String.t()}
  def publish(stream, payload) do
    GenServer.call(__MODULE__, {:publish, stream, payload})
  end

  @doc """
  Đọc danh sách message từ một stream thông qua MessageStore.
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

    # Đưa vào MessageStore
    :ok = MessageStore.put(stream, msg_id, payload)

    {:reply, {:ok, msg_id}, new_state}
  end

  # ===================================================================
  # Internal Helpers
  # ===================================================================

  # Generates ID format: "<timestamp_ms>-<seq>"
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
          # Xử lý trường hợp clock drift / system time lùi lại
          {last_ms, seq + 1}
      end

    msg_id = "#{new_ms}-#{new_seq}"
    updated_state = Map.put(state, stream, {new_ms, new_seq})

    {msg_id, updated_state}
  end
end
