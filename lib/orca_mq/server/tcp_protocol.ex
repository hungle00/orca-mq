defmodule OrcaMQ.Server.TCPProtocol do
  @moduledoc """
  Production TCP connection handler implementing `:ranch_protocol`.
  Each client connection is managed by a separate process of this module.
  """

  @behaviour :ranch_protocol

  require Logger
  alias OrcaMQ

  # ===================================================================
  # Ranch Protocol Callbacks
  # ===================================================================

  @impl true
  def start_link(ref, transport, _opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [ref, transport])
    {:ok, pid}
  end

  @doc false
  def init(ref, transport) do
    # Complete the Ranch handshake to transfer socket ownership to this process.
    {:ok, socket} = :ranch.handshake(ref)

    # Use active-once mode to provide backpressure and prevent unbounded buffering.
    :ok = transport.setopts(socket, [{:active, :once}, {:packet, :line}])

    # Start receiving data.
    loop(socket, transport, %{subscribed: false})
  end

  # ===================================================================
  # Connection Loop & Command Processing
  # ===================================================================

  defp loop(socket, transport, state) do
    receive do
      # Handle a packet received from the TCP client.
      {:tcp, ^socket, data} ->
        line = String.trim(data)
        new_state = handle_command(line, socket, transport, state)

        # Enable active-once mode for the next packet.
        transport.setopts(socket, [{:active, :once}])
        loop(socket, transport, new_state)

      # Handle the client closing the TCP connection.
      {:tcp_closed, ^socket} ->
        Logger.info("Client disconnected normally.")
        :ok

      # Handle a TCP connection error.
      {:tcp_error, ^socket, reason} ->
        Logger.error("TCP socket error: #{inspect(reason)}")
        :ok

      # Handle a message pushed by an OrcaMQ consumer.
      {:deliver_msg, msg} ->
        payload_str = Poison.encode!(msg.payload)
        transport.send(socket, "MSG #{msg.stream} #{msg.id} #{payload_str}\r\n")
        loop(socket, transport, state)

      other ->
        Logger.debug("Received unexpected message: #{inspect(other)}")
        loop(socket, transport, state)
    end
  end

  # ===================================================================
  # Command Parsing (PUB & SUB)
  # ===================================================================

  defp handle_command(line, socket, transport, state) do
    case String.split(line, " ", parts: 3) do
      # 1. Publish command: PUB <stream> <payload>
      ["PUB", stream, payload] ->
        case OrcaMQ.publish(stream, payload) do
          {:ok, msg_id} ->
            transport.send(socket, "OK #{msg_id}\r\n")
        end

        state

      # 2. Subscribe command: SUB <stream> <group>
      ["SUB", stream, group] ->
        self_pid = self()

        # Forward new messages to the current process as {:deliver_msg, msg}.
        {:ok, _consumer_pid} =
          OrcaMQ.subscribe(
            stream,
            group,
            fn msg ->
              send(self_pid, {:deliver_msg, msg})
              :ok
            end,
            prefetch: 20
          )

        transport.send(socket, "OK SUBSCRIBED_TO_#{stream}_GROUP_#{group}\r\n")
        %{state | subscribed: true}

      # Ping command for testing the connection.
      ["PING"] ->
        transport.send(socket, "PONG\r\n")
        state

      _ ->
        transport.send(socket, "ERROR INVALID_COMMAND\r\n")
        state
    end
  end
end
