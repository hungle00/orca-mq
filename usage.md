```elixir
alias MiniBroker.ConsumerGroup
alias MiniBroker.MessageStore
alias MiniBroker.StreamServer

{:ok, _} = Application.ensure_all_started(:mini_broker)

stream = "payments"
group = "fraud_checkers"

# 1. Subscribing to stream '#{stream}'
{:ok, _pid} = MiniBroker.subscribe(
  stream,
  group,
  fn msg ->
    IO.inspect(msg.payload, label: "-> [Consumer Received]")
    :ok
  end,
  poll_interval: 50
)

# 2. Publishing messages
MiniBroker.publish(stream, %{transaction_id: "tx_101", amount: 500})
MiniBroker.publish(stream, %{transaction_id: "tx_102", amount: 1200})

#{:ok, _pid} = ConsumerGroup.start_link(stream: stream, group_name: group)

# consumer_pid = self()
# fetched = ConsumerGroup.fetch_messages(stream, group, consumer_pid, 2)

# 3. Checking Group Status
IO.inspect(MiniBroker.ConsumerGroup.info(stream, group))
```