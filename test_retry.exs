# test_retry.exs
{:ok, _} = Application.ensure_all_started(:orca_mq)

stream = "orders_test"
group = "buggy_workers"

# 1. Subscribe with error handler function
{:ok, _pid} = OrcaMQ.subscribe(
  stream,
  group,
  fn msg ->
    IO.puts(" -> [Worker] Received msg #{msg.id}, processing...")
    # return error to active NACK
    {:error, :db_connection_failed}
  end,
  prefetch: 20
)

# 2. Publish 1 message
{:ok, msg_id} = OrcaMQ.publish(stream, %{order_id: 999})
IO.puts("Published message: #{msg_id}\n")

# Chờ 2 giây để Consumer thử lại đủ 3 lần và chuyển sang DLS
Process.sleep(2000)

# 3. Kiểm tra tin nhắn trong Dead Letter Stream
dls_stream = "#{stream}:dead_letter"
dls_messages = OrcaMQ.MessageStore.list(dls_stream)

IO.puts("\n==========================================")
IO.puts("Dead Letter Stream '#{dls_stream}' Contents:")
IO.inspect(dls_messages)
IO.puts("==========================================")
