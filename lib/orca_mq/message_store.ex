defmodule OrcaMQ.MessageStore do
  @moduledoc """
  Manages the ETS table that stores messages.
  Provides a public API for accessing and updating the table.
  """

  use GenServer

  @table_name :orca_messages

  defstruct [:id, :stream, :payload, :timestamp]

  @type t :: %__MODULE__{
          id: binary() | integer(),
          stream: String.t(),
          payload: term(),
          timestamp: integer()
        }

  @doc "Starts the MessageStore GenServer."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Stores a message."
  @spec put(String.t(), binary() | integer(), term()) :: :ok
  def put(stream, message_id, payload) do
    record = %__MODULE__{
      id: message_id,
      stream: stream,
      payload: payload,
      timestamp: System.system_time(:millisecond)
    }

    key = {stream, stream_id_key(message_id)}
    :ets.insert(@table_name, {key, record})
    :ok
  end

  @doc "Retrieves a message by stream and message ID."
  @spec get(String.t(), binary() | integer()) :: {:ok, t()} | {:error, :not_found}
  def get(stream, message_id) do
    key = {stream, stream_id_key(message_id)}

    case :ets.lookup(@table_name, key) do
      [{^key, %__MODULE__{} = message}] -> {:ok, message}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Retrieves messages from a stream in stream-ID order.
  Supports limiting the number returned with `:limit` and starting after `:after_id`.
  """
  @spec list(String.t(), keyword()) :: [t()]
  def list(stream, opts \\ []) do
    limit = Keyword.get(opts, :limit, 100)
    after_id = Keyword.get(opts, :after_id)

    if limit <= 0 do
      []
    else
      cursor = {stream, if(is_nil(after_id), do: :start, else: stream_id_key(after_id))}
      next = :ets.next(@table_name, cursor)
      collect_stream_messages(@table_name, next, stream, limit, [])
    end
  end

  @impl true
  def init(_opts) do
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

  defp collect_stream_messages(_table, _key, _stream, 0, messages),
    do: Enum.reverse(messages)

  defp collect_stream_messages(_table, :"$end_of_table", _stream, _remaining, messages),
    do: Enum.reverse(messages)

  defp collect_stream_messages(table, {stream, _} = key, stream, remaining, messages) do
    [{^key, message}] = :ets.lookup(table, key)

    collect_stream_messages(table, :ets.next(table, key), stream, remaining - 1, [
      message | messages
    ])
  end

  defp collect_stream_messages(_table, _key, _stream, _remaining, messages),
    do: Enum.reverse(messages)

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
