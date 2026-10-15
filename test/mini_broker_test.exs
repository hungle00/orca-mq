defmodule OrcaMQTest do
  use ExUnit.Case
  doctest OrcaMQ

  alias OrcaMQ.MessageStore

  test "publish/2 push messages successfully" do
    assert {:ok, msg_id} =
             OrcaMQ.publish("notifications", %{type: "email", to: "user@test.com"})

    assert is_binary(msg_id)

    assert {:ok, msg} = MessageStore.get("notifications", msg_id)
    assert msg.payload.type == "email"
  end
end
