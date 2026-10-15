defmodule MiniBroker.Consumer do
  @moduledoc """
  GenServer worker that continuously fetches messages from a ConsumerGroup,
  invokes the handler function, and acknowledges messages on success.
  """
  use GenServer
  require Logger

  alias MiniBroker.ConsumerGroup

  # Poll interval in milliseconds.
  @default_poll_interval 100

  defstruct [
    :stream,
    :group_name,
    :handler_fn,
    :poll_interval,
    :batch_size
  ]

  @doc """
  Starts a consumer process.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  # ===================================================================
  # GenServer Callbacks
  # ===================================================================

  @impl true
  def init(opts) do
    stream = Keyword.fetch!(opts, :stream)
    group_name = Keyword.fetch!(opts, :group_name)
    handler_fn = Keyword.fetch!(opts, :handler_fn)
    poll_interval = Keyword.get(opts, :poll_interval, @default_poll_interval)
    batch_size = Keyword.get(opts, :batch_size, 10)

    state = %__MODULE__{
      stream: stream,
      group_name: group_name,
      handler_fn: handler_fn,
      poll_interval: poll_interval,
      batch_size: batch_size
    }

    # Start polling as soon as the process is initialized.
    send(self(), :poll_messages)

    {:ok, state}
  end

  @impl true
  def handle_info(:poll_messages, state) do
    # Fetch messages from the ConsumerGroup.
    case ConsumerGroup.fetch_messages(state.stream, state.group_name, self(), state.batch_size) do
      messages when is_list(messages) and messages != [] ->
        # Process each received message.
        Enum.each(messages, fn msg -> process_message(msg, state) end)

        # If messages were received, poll again immediately.
        schedule_poll(0)

      _ ->
        # If the stream is empty or the group returns an error, wait before polling again.
        schedule_poll(state.poll_interval)
    end

    {:noreply, state}
  end

  defp process_message(msg, state) do
    case state.handler_fn.(msg) do
      :ok ->
        ConsumerGroup.ack(state.stream, state.group_name, msg.id)

      {:ok, _result} ->
        ConsumerGroup.ack(state.stream, state.group_name, msg.id)

      # Leave failed messages unacknowledged so they remain in the pending entries list.
      {:error, reason} ->
        Logger.error("Consumer handler failed for msg #{msg.id}: #{inspect(reason)}")

      # Leave messages unacknowledged when the handler returns an unexpected value.
      other ->
        Logger.warning("Consumer handler returned unexpected value: #{inspect(other)}")
    end
  end

  def schedule_poll(delay) do
    Process.send_after(self(), :poll_messages, delay)
  end
end
