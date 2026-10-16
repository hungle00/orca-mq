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

    assert :ok == ConsumerGroup.ack(stream, group, msg.id)

    # Ensure message is not in PEL
    info = ConsumerGroup.info(stream, group)
    assert info.pending_count == 0

    # ACK the message with throw error
    assert {:error, :not_pending} == ConsumerGroup.ack(stream, group, msg.id)
  end

  test "messages assigned to a terminated consumer are redelivered" do
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

    assert {:ok, _id} = OrcaMQ.publish(stream, :retry_me)
    assert_receive {:first_consumer_started, :retry_me}, 1_000
    Process.unlink(first_consumer)
    Process.exit(first_consumer, :kill)
    assert wait_until(fn -> ConsumerGroup.info(stream, group).consumer_count == 0 end)

    assert {:ok, _second_consumer} =
             OrcaMQ.subscribe(
               stream,
               group,
               fn message ->
                 send(parent, {:redelivered, message.payload})
                 :ok
               end,
               prefetch: 1
             )

    assert_receive {:redelivered, :retry_me}, 1_000
    assert wait_until(fn -> ConsumerGroup.info(stream, group).pending_count == 0 end)
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
