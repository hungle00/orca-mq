defmodule MiniBrokerTest do
  use ExUnit.Case
  doctest MiniBroker

  test "greets the world" do
    assert MiniBroker.hello() == :world
  end
end
