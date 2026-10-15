broker = System.get_env("BROKER_NODE", "broker@127.0.0.1") |> String.to_atom()
stream = "orders"
payload = %{transaction_id: "tx_101", amount: 500}

unless Node.connect(broker) do
  raise "Could not connect to #{broker}. Check the node name and Erlang cookie."
end

case :rpc.call(
       broker,
       OrcaMQ,
       :publish,
       [stream, payload],
       5_000
     ) do
  {:ok, message_id} ->
    IO.puts("Published message #{message_id} to stream '#{stream}' on #{broker}.")

  {:error, reason} ->
    raise "Publish failed: #{inspect(reason)}"

  {:badrpc, reason} ->
    raise "RPC to broker failed: #{inspect(reason)}"

  other ->
    raise "Unexpected publish response: #{inspect(other)}"
end
