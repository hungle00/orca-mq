defmodule OrcaMQ.Server.TCPProtocol do
  @moduledoc """
  Handler xử lý kết nối TCP chuẩn production triển khai :ranch_protocol.
  Mỗi client kết nối vào sẽ do 1 process riêng của module này quản lý.
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
    # Bắt buộc handshake với Ranch để chuyển ownership của socket về process này
    {:ok, socket} = :ranch.handshake(ref)

    # Đặt socket ở chế độ active: :once để chống tràn bộ nhớ (Backpressure management)
    :ok = transport.setopts(socket, [{:active, :once}, {:packet, :line}])

    # Vòng lặp nhận dữ liệu
    loop(socket, transport, %{subscribed: false})
  end

  # ===================================================================
  # Connection Loop & Command Processing
  # ===================================================================

  defp loop(socket, transport, state) do
    receive do
      # Khi nhận được packet từ TCP Client
      {:tcp, ^socket, data} ->
        line = String.trim(data)
        new_state = handle_command(line, socket, transport, state)

        # Bật lại active: :once cho đợt packet tiếp theo
        transport.setopts(socket, [{:active, :once}])
        loop(socket, transport, new_state)

      # Khi client chủ động đóng kết nối TCP
      {:tcp_closed, ^socket} ->
        Logger.info("Client disconnected normally.")
        :ok

      # Khi xảy ra lỗi đường truyền
      {:tcp_error, ^socket, reason} ->
        Logger.error("TCP socket error: #{inspect(reason)}")
        :ok

      # Xử lý tin nhắn push từ OrcaMQ Consumer
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
      # 1. Lệnh Publish: PUB <stream> <payload>
      ["PUB", stream, payload] ->
        case OrcaMQ.publish(stream, payload) do
          {:ok, msg_id} ->
            transport.send(socket, "OK #{msg_id}\r\n")

          {:error, reason} ->
            transport.send(socket, "ERROR #{inspect(reason)}\r\n")
        end

        state

      # 2. Lệnh Subscribe: SUB <stream> <group>
      ["SUB", stream, group] ->
        self_pid = self()

        # Đăng ký subscribe: khi có msg mới, gửi message {:deliver_msg, msg} tới process hiện tại
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

      # Lệnh Ping test kết nối
      ["PING"] ->
        transport.send(socket, "PONG\r\n")
        state

      _ ->
        transport.send(socket, "ERROR INVALID_COMMAND\r\n")
        state
    end
  end
end
