# Usage Examples
## 1. Embedded Elixir Usage
You can use `OrcaMQ` directly within your Elixir application:
```elixir
alias OrcaMQ.ConsumerGroup
alias OrcaMQ.MessageStore
alias OrcaMQ.StreamServer

{:ok, _} = Application.ensure_all_started(:orca_mq)

stream = "payments"
group = "fraud_checkers"

# 1. Subscribing to stream '#{stream}'
{:ok, _pid} = OrcaMQ.subscribe(
  stream,
  group,
  fn msg ->
    IO.inspect(msg.payload, label: "-> [Consumer Received]")
    :ok
  end,
  poll_interval: 50
)

# 2. Publishing messages
OrcaMQ.publish(stream, %{transaction_id: "tx_101", amount: 500})
OrcaMQ.publish(stream, %{transaction_id: "tx_102", amount: 1200})

#{:ok, _pid} = ConsumerGroup.start_link(stream: stream, group_name: group)

# consumer_pid = self()
# fetched = ConsumerGroup.fetch_messages(stream, group, consumer_pid, 2)

# 3. Checking Group Status
IO.inspect(OrcaMQ.ConsumerGroup.info(stream, group))
```

## Running on Multiple Nodes (Distributed Erlang)
Because OrcaMQ is built on BEAM, you can execute commands across multiple machines/nodes natively using Erlang RPC without writing custom socket code.

### Step 1: Start the Broker Node (Server)
Start the main Broker application on machine/terminal 1 with a node name:
```
iex --name broker@127.0.0.1 -S mix
```

## Step 2: Running Client Script on a Separate Node
From the project root, start each process in a separate terminal, using the same Erlang cookie:
```
iex --name broker@127.0.0.1 --cookie shared_cookie -S mix
elixir --name subscriber@127.0.0.1 --cookie shared_cookie client/subscriber.exs
elixir --name producer@127.0.0.1 --cookie shared_cookie client/producer.exs
```