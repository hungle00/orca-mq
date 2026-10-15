defmodule MiniBroker.MessageStore do
  @moduledoc """
  Manages the ETS table that stores messages.
  Provides a public API for accessing and updating the table.
  """

  use GenServer

  @table_name :orca_messages

  # Struct representing a message in the store.
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
  Starts the MessageStore GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stores a message.
  """
  @spec put(String.t(), binary() | integer(), term()) :: :ok
  def put(stream, message_id, payload) do
    record = %__MODULE__{
      id: message_id,
      stream: stream,
      payload: payload,
      timestamp: System.system_time(:millisecond)
    }

    # ETS object format: {{stream, message_id}, record}.
    # The composite key {stream, message_id} enables stream-scoped lookups and ordering.
    :ets.insert(@table_name, {{stream, message_id}, record})
    :ok
  end

  @doc """
  Retrieves a message by stream and message ID.
  """
  @spec get(String.t(), binary() | integer()) :: {:ok, t()} | {:error, :not_found}
  def get(stream, message_id) do
    case :ets.lookup(@table_name, {stream, message_id}) do
      [{{^stream, ^message_id}, %__MODULE__{} = message}] -> {:ok, message}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Retrieves messages from a stream.
  Supports limiting the number of returned messages with `:limit`.
  """
  @spec list(String.t(), keyword()) :: [t()]
  def list(stream, opts \\ []) do
    limit = Keyword.get(opts, :limit, 100)

    # Use a match pattern to select messages belonging to this stream.
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
    # Create the ETS table with concurrency optimizations.
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
