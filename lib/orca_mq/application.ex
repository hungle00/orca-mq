defmodule OrcaMQ.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    tcp_port = String.to_integer(System.get_env("PORT") || "5555")

    children = [
      # The Registry tracks ConsumerGroup processes by {stream, group_name}.
      {Registry, keys: :unique, name: OrcaMQ.GroupRegistry},
      {Registry, keys: :duplicate, name: OrcaMQ.StreamRegistry},
      # The DynamicSupervisor starts ConsumerGroup processes when subscribe/3 is called.
      {DynamicSupervisor, strategy: :one_for_one, name: OrcaMQ.GroupSupervisor},
      OrcaMQ.MessageStore,
      # StreamServer depends on MessageStore.
      OrcaMQ.StreamServer,
      # Khởi chạy Ranch TCP Server Acceptor Pool
      :ranch.child_spec(
        :minibroker_tcp,
        :ranch_tcp,
        %{socket_opts: [port: tcp_port]},
        OrcaMQ.Server.TCPProtocol,
        []
      )
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html for other strategies and supported options.
    opts = [strategy: :one_for_one, name: OrcaMQ.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
