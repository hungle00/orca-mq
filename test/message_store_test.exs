defmodule OrcaMQ.MessageStoreTest do
  use ExUnit.Case, async: false

  alias OrcaMQ.MessageStore

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
