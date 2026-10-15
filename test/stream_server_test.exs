defmodule OrcaMQ.StreamServerTest do
  use ExUnit.Case, async: false

  alias OrcaMQ.MessageStore
  alias OrcaMQ.StreamServer

  test "publish/2 generate message ID and save to MessageStore" do
    stream = "orders"
    payload = %{item: "Laptop", price: 1200}

    assert {:ok, msg_id} = StreamServer.publish(stream, payload)
    assert msg_id =~ ~r/^\d+-\d+$/

    assert {:ok, msg} = MessageStore.get(stream, msg_id)
    assert msg.id == msg_id
    assert msg.payload == payload
  end

  test "publish/2 continuously generate incrementing IDs when same millisecond" do
    stream = "metrics"

    {:ok, id1} = StreamServer.publish(stream, "data_1")
    {:ok, id2} = StreamServer.publish(stream, "data_2")

    [ts1, seq1] = String.split(id1, "-") |> Enum.map(&String.to_integer/1)
    [ts2, seq2] = String.split(id2, "-") |> Enum.map(&String.to_integer/1)

    if ts1 == ts2 do
      assert seq2 == seq1 + 1
    else
      assert ts2 > ts1
      assert seq2 == 0
    end
  end

  test "read_stream/2 return list of messages from store" do
    stream = "events"

    {:ok, id1} = StreamServer.publish(stream, "event_1")
    {:ok, id2} = StreamServer.publish(stream, "event_2")

    messages = StreamServer.read_stream(stream)
    assert length(messages) == 2
    assert Enum.map(messages, & &1.id) == [id1, id2]
  end
end
