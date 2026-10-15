defmodule OrcaMQ.ConsumerGroupTest do
  use ExUnit.Case, async: false

  alias OrcaMQ.ConsumerGroup
  alias OrcaMQ.MessageStore

  test "fetch/4 creates a remote consumer group and returns messages for acknowledgement" do
    stream = "remote-consumer-fetch"
    group = "remote-consumer-fetch"
    message_id = "1-0"
    :ok = MessageStore.put(stream, message_id, :payload)

    assert [%MessageStore{id: ^message_id, payload: :payload}] =
             OrcaMQ.fetch(stream, group, self(), 10)

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
end
