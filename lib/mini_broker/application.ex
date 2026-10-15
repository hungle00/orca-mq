defmodule MiniBroker.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # The Registry tracks ConsumerGroup processes by {stream, group_name}.
      {Registry, keys: :unique, name: MiniBroker.GroupRegistry},
      # The DynamicSupervisor starts ConsumerGroup processes when subscribe/3 is called.
      {DynamicSupervisor, strategy: :one_for_one, name: MiniBroker.GroupSupervisor},
      MiniBroker.MessageStore,
      # StreamServer depends on MessageStore.
      MiniBroker.StreamServer
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html for other strategies and supported options.
    opts = [strategy: :one_for_one, name: MiniBroker.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
