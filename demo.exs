# demo.exs

# 1. Khởi động Application (Supervision Tree)
{:ok, _} = Application.ensure_all_started(:orca_mq)

stream_name = "orders"

IO.puts("=== 1. PUBLISH MESSAGES ===")
{:ok, id1} = OrcaMQ.StreamServer.publish(stream_name, %{user_id: 1, amount: 250})
{:ok, id2} = OrcaMQ.StreamServer.publish(stream_name, %{user_id: 2, amount: 500})
{:ok, id3} = OrcaMQ.StreamServer.publish(stream_name, %{user_id: 3, amount: 150})

IO.puts("Published ID 1: #{id1}")
IO.puts("Published ID 2: #{id2}")
IO.puts("Published ID 3: #{id3}")

IO.puts("\n=== 2. READ ALL MESSAGES FROM STREAM ===")
messages = OrcaMQ.StreamServer.read_stream(stream_name)

Enum.each(messages, fn msg ->
  IO.inspect(msg, label: "Message")
end)

IO.puts("\n=== 3. GET SINGLE MESSAGE BY ID ===")
{:ok, msg} = OrcaMQ.MessageStore.get(stream_name, id2)
IO.inspect(msg, label: "Fetched Message #{id2}")
