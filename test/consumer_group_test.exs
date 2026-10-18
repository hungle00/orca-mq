defmodule OrcaMQ.ConsumerGroupTest do
  use ExUnit.Case, async: false

  alias OrcaMQ.ConsumerGroup
  alias OrcaMQ.MessageStore
  alias OrcaMQ.StreamServer

  test "fetch/4 creates a remote consumer group and returns messages for acknowledgement" do
    stream = "remote-consumer-fetch"
    group = "remote-consumer-fetch"
    message_id = "1-0"
    :ok = MessageStore.put(stream, message_id, :payload)

    assert [%MessageStore{id: ^message_id, payload: :payload}] =
             OrcaMQ.fetch(stream, group, self(), 10)

    refute_receive {:deliver, _message}, 0
    assert :ok = OrcaMQ.ack(stream, group, message_id)
  end

  test "fetch_messages/4 reads the entire stream in batches larger than 1,000 messages" do
    stream = "consumer-group-large-stream"
    group = "consumer-group-large-stream"
    message_count = 1_105

    for sequence <- 0..(message_count - 1) do
      :ok = MessageStore.put(stream, "1-#{sequence}", sequence)
    end

    start_supervised!({ConsumerGroup, stream: stream, group_name: group})

    message_ids =
      Stream.repeatedly(fn ->
        ConsumerGroup.fetch_messages(stream, group, self(), 137)
      end)
      |> Enum.reduce_while([], fn
        [], acc -> {:halt, acc}
        {:error, reason}, _acc -> raise "fetch failed: #{inspect(reason)}"
        messages, acc -> {:cont, acc ++ Enum.map(messages, & &1.id)}
      end)

    assert length(message_ids) == message_count
    assert message_ids == Enum.map(0..(message_count - 1), &"1-#{&1}")
  end

  test "subscribers receive messages published after registration" do
    stream = "push-consumer-delivery"
    group = "push-consumer-delivery"
    parent = self()

    assert {:ok, _consumer} =
             OrcaMQ.subscribe(stream, group, fn message ->
               send(parent, {:handled, message.payload})
               :ok
             end)

    assert {:ok, _id} = OrcaMQ.publish(stream, :pushed_message)
    assert_receive {:handled, :pushed_message}, 1_000
  end

  test "consumer handler failures NACK and eventually move the message to dead letter" do
    stream = "consumer-handler-failure"
    group = "consumer-handler-failure"

    assert {:ok, _consumer} =
             OrcaMQ.subscribe(
               stream,
               group,
               fn _message ->
                 {:error, :handler_failed}
               end,
               retry_backoff_ms: 10,
               max_retry_backoff_ms: 20
             )

    assert {:ok, message_id} = OrcaMQ.publish(stream, :fail_me)

    assert wait_until(fn ->
             match?([_], MessageStore.list("#{stream}:dead_letter"))
           end)

    assert [
             %MessageStore{
               payload: %{
                 original_id: ^message_id,
                 original_msg: :fail_me,
                 total_retries: 3
               }
             }
           ] = MessageStore.list("#{stream}:dead_letter")
  end

  test "prefetch limits pending delivery and acknowledgements release capacity" do
    stream = "push-consumer-prefetch"
    group = "push-consumer-prefetch"
    parent = self()

    assert {:ok, consumer} =
             OrcaMQ.subscribe(
               stream,
               group,
               fn message ->
                 send(parent, {:started, message.id})

                 receive do
                   :continue -> :ok
                 after
                   1_000 -> {:error, :test_timeout}
                 end
               end,
               prefetch: 2
             )

    for value <- 1..3 do
      assert {:ok, _id} = OrcaMQ.publish(stream, value)
    end

    assert_receive {:started, _first_id}, 1_000
    assert ConsumerGroup.info(stream, group).pending_count == 2

    send(consumer, :continue)
    assert_receive {:started, _second_id}, 1_000
    send(consumer, :continue)
    assert_receive {:started, _third_id}, 1_000
    send(consumer, :continue)
    assert wait_until(fn -> ConsumerGroup.info(stream, group).pending_count == 0 end)
  end

  test "ack/3 remove message from Pending List (PEL)" do
    stream = "orders"
    group = "billing_workers"

    {:ok, _id1} = StreamServer.publish(stream, "order_1")
    {:ok, _pid} = ConsumerGroup.start_link(stream: stream, group_name: group)

    [msg] = ConsumerGroup.fetch_messages(stream, group, self(), 1)

    assert ConsumerGroup.info(stream, group).pending_entries[msg.id].retry_count == 0
    assert :ok == ConsumerGroup.ack(stream, group, msg.id)

    # Ensure message is not in PEL
    info = ConsumerGroup.info(stream, group)
    assert info.pending_count == 0

    # ACK the message with throw error
    assert {:error, :not_pending} == ConsumerGroup.ack(stream, group, msg.id)
  end

  test "nack retries up to max_retries, then moves the message to the dead-letter stream" do
    stream = "consumer-group-nack-retries"
    group = "consumer-group-nack-retries"

    start_supervised!({ConsumerGroup, stream: stream, group_name: group, max_retries: 2})

    assert :ok = ConsumerGroup.register_consumer(stream, group, self(), 1)
    assert {:error, :not_pending} = ConsumerGroup.nack(stream, group, "missing-message")
    assert {:ok, message_id} = StreamServer.publish(stream, %{order_id: 42})

    assert_receive {:deliver, %{id: ^message_id}}, 1_000
    assert :ok = ConsumerGroup.nack(stream, group, message_id)
    assert_receive {:deliver, %{id: ^message_id}}, 1_000
    assert ConsumerGroup.info(stream, group).pending_entries[message_id].retry_count == 1

    assert :ok = ConsumerGroup.nack(stream, group, message_id)
    assert_receive {:deliver, %{id: ^message_id}}, 1_000
    assert ConsumerGroup.info(stream, group).pending_entries[message_id].retry_count == 2

    assert {:ok, :dead_lettered} = ConsumerGroup.nack(stream, group, message_id)
    assert ConsumerGroup.info(stream, group).pending_count == 0

    assert [
             %MessageStore{
               payload: %{
                 original_msg: %{order_id: 42},
                 original_id: ^message_id,
                 failed_group: ^group,
                 total_retries: 2
               }
             }
           ] = MessageStore.list("#{stream}:dead_letter")
  end

  test "NACK retries use exponential backoff" do
    stream = "consumer-group-nack-backoff"
    group = "consumer-group-nack-backoff"

    start_supervised!(
      {ConsumerGroup,
       stream: stream,
       group_name: group,
       max_retries: 2,
       retry_backoff_ms: 80,
       max_retry_backoff_ms: 200}
    )

    assert :ok = ConsumerGroup.register_consumer(stream, group, self(), 1)
    assert {:ok, message_id} = StreamServer.publish(stream, :retry_me)
    assert_receive {:deliver, %{id: ^message_id}}, 1_000

    assert :ok = ConsumerGroup.nack(stream, group, message_id)
    refute_receive {:deliver, %{id: ^message_id}}, 30
    assert_receive {:deliver, %{id: ^message_id}}, 1_000

    assert :ok = ConsumerGroup.nack(stream, group, message_id)
    refute_receive {:deliver, %{id: ^message_id}}, 100
    assert_receive {:deliver, %{id: ^message_id}}, 1_000
  end

  test "supervised consumers restart and receive their pending messages again" do
    stream = "push-consumer-redelivery"
    group = "push-consumer-redelivery"
    parent = self()

    assert {:ok, first_consumer} =
             OrcaMQ.subscribe(
               stream,
               group,
               fn message ->
                 send(parent, {:first_consumer_started, message.payload})

                 receive do
                   :hold -> :ok
                 end
               end,
               prefetch: 1
             )

    assert {:ok, message_id} = OrcaMQ.publish(stream, :retry_me)
    assert_receive {:first_consumer_started, :retry_me}, 1_000
    Process.exit(first_consumer, :kill)

    refute_receive {:first_consumer_started, :retry_me}, 30
    assert_receive {:first_consumer_started, :retry_me}, 1_000

    assert wait_until(fn ->
             case ConsumerGroup.info(stream, group).pending_entries do
               %{^message_id => %{consumer_pid: pid, retry_count: 1}} ->
                 is_pid(pid) and pid != first_consumer

               _ ->
                 false
             end
           end)

    restarted_consumer =
      ConsumerGroup.info(stream, group).pending_entries[message_id].consumer_pid

    assert Process.alive?(restarted_consumer)
    send(restarted_consumer, :hold)
    assert wait_until(fn -> ConsumerGroup.info(stream, group).pending_count == 0 end)
  end

  test "consumer group enforces its configured consumer limit" do
    stream = "consumer-group-consumer-limit"
    group = "consumer-group-consumer-limit"
    handler = fn _message -> :ok end

    assert {:ok, _first_consumer} =
             OrcaMQ.subscribe(stream, group, handler, max_consumers: 1)

    assert {:error, _reason} = OrcaMQ.subscribe(stream, group, handler)
    assert ConsumerGroup.info(stream, group).consumer_count == 1
  end

  defp wait_until(fun, attempts \\ 50)
  defp wait_until(_fun, 0), do: false

  defp wait_until(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end
end
