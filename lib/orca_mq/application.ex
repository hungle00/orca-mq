defmodule OrcaMQ.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    tcp_port = String.to_integer(System.get_env("PORT") || "5555")
    max_consumer_workers = Application.get_env(:orca_mq, :max_consumer_workers, 1_000)

    children = [
      # The Registry tracks ConsumerGroup processes by {stream, group_name}.
      {Registry, keys: :unique, name: OrcaMQ.GroupRegistry},
      {Registry, keys: :duplicate, name: OrcaMQ.StreamRegistry},
      # The DynamicSupervisor starts ConsumerGroup processes when subscribe/3 is called.
      {DynamicSupervisor, strategy: :one_for_one, name: OrcaMQ.GroupSupervisor},
      {DynamicSupervisor,
       strategy: :one_for_one, name: OrcaMQ.ConsumerSupervisor, max_children: max_consumer_workers},
      OrcaMQ.MessageStore,
      # StreamServer depends on MessageStore.
      OrcaMQ.StreamServer,
      # Start the Ranch TCP server acceptor pool.
      :ranch.child_spec(
        :orcamq_tcp,
        :ranch_tcp,
        %{socket_opts: [port: tcp_port]},
        OrcaMQ.Server.TCPProtocol,
        []
      )
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html for other strategies and supported options.
    opts = [strategy: :one_for_one, name: OrcaMQ.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        {ip, port} = :ranch.get_addr(:orcamq_tcp)
        Logger.info("OrcaMQ server running on #{:inet.ntoa(ip)}:#{port}")
        {:ok, pid}

      error ->
        error
    end
  end
end
