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

  test "Manual ACK by MiniBroker.ack/3" do
    stream = "tasks"
    group = "manual_ack_workers"

    # Khởi tạo Group
    {:ok, _} =
      DynamicSupervisor.start_child(
        OrcaMQ.GroupSupervisor,
        {OrcaMQ.ConsumerGroup, stream: stream, group_name: group}
      )

    {:ok, msg_id} = OrcaMQ.publish(stream, "task_payload")

    # Fetch thủ công để đưa tin nhắn vào PEL
    [_msg] = OrcaMQ.ConsumerGroup.fetch_messages(stream, group, self(), 1)

    # Gọi ack qua Public API
    assert :ok == OrcaMQ.ack(stream, group, msg_id)
  end
end
