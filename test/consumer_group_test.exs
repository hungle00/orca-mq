defmodule MiniBroker.ConsumerGroupTest do
  use ExUnit.Case, async: false

  alias MiniBroker.ConsumerGroup
  alias MiniBroker.MessageStore

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
