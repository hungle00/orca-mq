broker = System.get_env("BROKER_NODE", "broker@127.0.0.1") |> String.to_atom()
stream = "orders"

message_count =
  case Integer.parse(System.get_env("MESSAGE_COUNT", "1000")) do
    {count, ""} when count > 0 -> count
    _ -> raise "MESSAGE_COUNT must be a positive integer."
  end

unless Node.connect(broker) do
  raise "Could not connect to #{broker}. Check the node name and Erlang cookie."
end

Enum.each(1..message_count, fn sequence ->
  payload = %{transaction_id: "tx_#{sequence}", amount: 500}

  case :rpc.call(broker, OrcaMQ, :publish, [stream, payload], 5_000) do
    {:ok, _message_id} ->
      :ok

    {:error, reason} ->
      raise "Publish failed for message #{sequence}: #{inspect(reason)}"

    {:badrpc, reason} ->
      raise "RPC failed for message #{sequence}: #{inspect(reason)}"

    other ->
      raise "Unexpected publish response for message #{sequence}: #{inspect(other)}"
  end
end)

IO.puts("Published #{message_count} messages to stream '#{stream}' on #{broker}.")
