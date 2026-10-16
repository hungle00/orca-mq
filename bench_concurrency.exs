# 1. Boot OTP Application
{:ok, _} = Application.ensure_all_started(:orca_mq)

stream_name = "orders_high_load"

# Benchmark Configuration
total_publishers = 50
messages_per_publisher = 2000   # Each process push 1,000 msgs => 20,000 msgs
total_messages = total_publishers * messages_per_publisher

IO.puts("==================================================")
IO.puts("   OrcaMQ HIGH CONCURRENCY BENCHMARK          ")
IO.puts("==================================================")
IO.puts("Total Messages to Publish: #{total_messages}")
IO.puts("Concurrent Publisher Processes: #{total_publishers}")
IO.puts("--------------------------------------------------")

# ===================================================================
# 2. SETUP CONSUMER GROUPS & METRICS COUNTERS
# ===================================================================

# Create Atomic Counter to count msgs that Group handle
counter_group = :counters.new(1, [:write_concurrency])

test_pid = self()

{:ok, _} =
  OrcaMQ.subscribe(
    stream_name,
    "fast_notification_group",
    fn _msg ->
      :counters.add(counter_group, 1, 1)

      # When Group A receives all total_messages, notify the main process
      if :counters.get(counter_group, 1) == total_messages do
        send(test_pid, {:group_done, :group_a})
      end

      :ok
    end,
    prefetch: 100
  )

# ===================================================================
# 3. BENCHMARK PUBLISH (HIGH CONCURRENCY INGESTION)
# ===================================================================

IO.puts("\n🚀 [1/2] Starting Concurrent Ingestion...")
publish_start_time = System.monotonic_time(:millisecond)

1..total_publishers
|> Task.async_stream(
  fn pub_id ->
    for i <- 1..messages_per_publisher do
      payload = %{pub_id: pub_id, seq: i, data: "payload_#{pub_id}_#{i}"}
      OrcaMQ.publish(stream_name, payload)
    end
  end,
  max_concurrency: total_publishers,
  timeout: :infinity
)
|> Stream.run()

publish_end_time = System.monotonic_time(:millisecond)
publish_duration_sec = (publish_end_time - publish_start_time) / 1000.0
publish_tps = round(total_messages / publish_duration_sec)

IO.puts("✅ PUBLISH FINISHED!")
IO.puts("   Time taken     : #{Float.round(publish_duration_sec, 3)} seconds")
IO.puts("   Throughput     : #{publish_tps} msgs/sec")

# ===================================================================
# 4. BENCHMARK CONSUMPTION (MULTI-GROUP PROCESSING)
# ===================================================================

IO.puts("\n [2/2] Processing Messages In Consumer Groups...")
consume_start_time = publish_start_time

receive do
  {:group_done, :group_a} -> IO.puts("   -> Group A (Notifications) finished all messages!")
after
  30_000 -> raise "Consumer group did not finish within 30 seconds"
end


consume_end_time = System.monotonic_time(:millisecond)
consume_duration_sec = (consume_end_time - consume_start_time) / 1000.0
consume_tps = round(total_messages / consume_duration_sec)

IO.puts("\n==================================================")
IO.puts("                BENCHMARK RESULTS                 ")
IO.puts("==================================================")
IO.puts("Published Messages       : #{total_messages}")
IO.puts("Ingestion Speed          : #{publish_tps} msgs/sec")
IO.puts("End-to-end Speed         : #{consume_tps} msgs/sec")
IO.puts("--------------------------------------------------")

# VERIFY DATA IN ETS
messages_in_ets = length(OrcaMQ.MessageStore.list(stream_name, limit: 100_000))
IO.puts("Total Messages in ETS    : #{messages_in_ets}")
