defmodule RemoteSubscriber do
  def poll(broker_node, stream, group, batch_size, poll_interval) do
    case :rpc.call(
           broker_node,
           OrcaMQ,
           :fetch,
           [stream, group, self(), batch_size],
           5_000
         ) do
      [] ->
        Process.sleep(poll_interval)

      messages when is_list(messages) ->
        Enum.each(messages, &process_message(broker_node, stream, group, &1))

      {:error, reason} ->
        raise "Could not fetch messages: #{inspect(reason)}"

      {:badrpc, reason} ->
        raise "RPC fetch failed: #{inspect(reason)}"

      other ->
        raise "Unexpected fetch response: #{inspect(other)}"
    end

    poll(broker_node, stream, group, batch_size, poll_interval)
  end

  defp process_message(broker_node, stream, group, message) do
    IO.puts("Received message #{message.id}: #{inspect(message.payload)}")

    case :rpc.call(broker_node, OrcaMQ, :ack, [stream, group, message.id], 5_000) do
      :ok -> :ok
      {:error, reason} -> raise "Could not acknowledge message #{message.id}: #{inspect(reason)}"
      {:badrpc, reason} -> raise "RPC acknowledgement failed: #{inspect(reason)}"
      other -> raise "Unexpected acknowledgement response: #{inspect(other)}"
    end
  end
end

broker_node = System.get_env("BROKER_NODE", "broker@127.0.0.1") |> String.to_atom()
stream = "orders"
group = "remote_email_workers"
batch_size = 10
poll_interval = 100

IO.puts("Connecting to Broker Node #{broker_node}...")

case Node.connect(broker_node) do
  true ->
    IO.puts("Connected to #{broker_node}.")
    IO.puts("Consuming stream '#{stream}' in group '#{group}'.")
    IO.puts("Waiting for messages. Press Ctrl+C to exit.\n")
    RemoteSubscriber.poll(broker_node, stream, group, batch_size, poll_interval)

  false ->
    raise "Could not connect to #{broker_node}. Check the node name and Erlang cookie."
end
