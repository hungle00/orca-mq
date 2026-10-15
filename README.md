# OrcaMQ 🚀

A lightweight, high-concurrency, in-memory message broker written in **Elixir/OTP**. Inspired by **Redis Streams** (ID-based sequence, consumer groups, PEL) and **RabbitMQ** (Exchanges, Fan-out, Topic routing).

Built from the ground up leveraging **ETS** with parallel read/write concurrency for zero-bottleneck data ingestion, combined with native BEAM process isolation for state management.

---

## Key Features

- **High Performance In-Memory Storage:** Powered by ETS (`:public`, `:ordered_set`, `:bag`) enabling concurrent read/write throughput without GenServer lock contention.
- **Consumer Groups & PEL:** Tracks reading offsets per consumer group along with a Pending Entries List (PEL) to guarantee at-least-once message delivery.
- **Flexible Routing (Exchanges):** Supports both **Fan-out** (publish-subscribe) and **Topic routing** with wildcard matching (`*` and `#`).
- **Distributed Erlang Native:** Seamless clustering across multiple nodes via native Erlang RPC and message passing without external network layers.

---

## 🚀 Getting Started

### Prerequisites  
- Elixir: 1.14+
- Erlang/OTP: 25+

### Installation & Setup

1. Clone the repository:
```
git clone https://github.com/hungle00/orca-mq
cd orca-mq
```

2. Fetch dependencies:
```
mix deps.get
```
