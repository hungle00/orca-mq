defmodule MiniBroker.MessageStore do
  @moduledoc """
  Quản lý ETS table lưu trữ message.
  Cung cấp các hàm public API để thao tác với ETS table.
  """

  use GenServer

  @table_name :mini_broker_messages

  # Struct đại diện cho một Message trong store
  defstruct [:id, :stream, :payload, :timestamp]

  @type t :: %__MODULE__{
          id: binary() | integer(),
          stream: String.t(),
          payload: term(),
          timestamp: integer()
        }

  # ===================================================================
  # Public API
  # ===================================================================

  @doc """
  Khởi chạy MessageStore GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Lưu một message vào store.
  """
  @spec put(String.t(), binary() | integer(), term()) :: :ok
  def put(stream, message_id, payload) do
    record = %__MODULE__{
      id: message_id,
      stream: stream,
      payload: payload,
      timestamp: System.system_time(:millisecond)
    }

    # Lock tuple format: {{stream, message_id}, record}
    # Việc dùng composite key {stream, message_id} giúp query và sắp xếp theo stream dễ dàng.
    :ets.insert(@table_name, {{stream, message_id}, record})
    :ok
  end

  @doc """
  Lấy một message cụ thể theo stream và message_id.
  """
  @spec get(String.t(), binary() | integer()) :: {:ok, t()} | {:error, :not_found}
  def get(stream, message_id) do
    case :ets.lookup(@table_name, {stream, message_id}) do
      [{{^stream, ^message_id}, %__MODULE__{} = message}] -> {:ok, message}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Lấy danh sách message của một stream.
  Hỗ trợ giới hạn số lượng (limit).
  """
  @spec list(String.t(), keyword()) :: [t()]
  def list(stream, opts \\ []) do
    limit = Keyword.get(opts, :limit, 100)

    # Sử dụng Match Pattern để truy vấn tất cả message thuộc về stream này
    match_spec = [
      {{{stream, :"$1"}, :"$2"}, [], [:"$2"]}
    ]

    case :ets.select(@table_name, match_spec, limit) do
      {records, _continuation} -> records
      :"$end_of_table" -> []
    end
  end

  # ===================================================================
  # GenServer Callbacks
  # ===================================================================

  @impl true
  def init(_opts) do
    # Khai báo bảng ETS với tối ưu hóa concurrency
    table =
      :ets.new(@table_name, [
        :ordered_set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])

    {:ok, %{table: table}}
  end
end
