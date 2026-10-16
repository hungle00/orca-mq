defmodule OrcaMQ.MessageStoreTest do
  use ExUnit.Case, async: false

  alias OrcaMQ.MessageStore

  test "put/3 and get/2 save and retrieve messages correctly" do
    stream = "orders"
    msg_id = "1000-0"
    payload = %{user_id: 1, amount: 250}

    assert :ok == MessageStore.put(stream, msg_id, payload)

    assert {:ok, message} = MessageStore.get(stream, msg_id)
    assert message.id == msg_id
    assert message.stream == stream
    assert message.payload == payload
    assert is_integer(message.timestamp)
  end

  test "list/2 returns IDs in numeric order across ETS pages" do
    stream = "message-store-numeric-order"

    for sequence <- 0..1_104 do
      :ok = MessageStore.put(stream, "1-#{sequence}", sequence)
    end

    messages = MessageStore.list(stream, limit: 1_105)

    assert length(messages) == 1_105
    assert Enum.map(messages, & &1.id) == Enum.map(0..1_104, &"1-#{&1}")
  end
end
