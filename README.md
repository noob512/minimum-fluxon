# Bridging L2–L3

Bridging L2–L3 is a distributed KV Cache and RPC data plane for AI workloads. The name describes the bridge between memory-resident cache data and shared or remote capacity across processes and nodes.

This repository is the compact blind-review source package. It keeps the KV Cache data path, peer-to-peer transport, shared memory, SSD backing, and Python/Rust interfaces required to inspect and reproduce the central design. The user-facing project name is Bridging L2–L3; source-level module, crate, configuration, and API identifiers remain unchanged for build and interface compatibility.

## Research focus

The package focuses on one question: how should a distributed KV Cache move and expose values across memory tiers while preserving a simple client contract?

- The **KV master** coordinates membership, key-version routes, leases, and lifecycle metadata.
- An **owner client** contributes DRAM or local SSD capacity and serves local and remote values.
- An **external client** uses the service plane without contributing storage capacity.
- **KV-RPC** provides typed user calls over the same peer-to-peer transport.
- **Shared memory** provides the local fast path; `MemHolder` carries the resulting value metadata.
- **SSD storage** acts as an owner-local backing tier when a value is not resident in DRAM.

The former file-service and message-queue products, deployment stack, and dedicated test infrastructure are outside this review package. Internal file IO and asynchronous channels that implement the KV path remain part of the source.

## Architecture at a glance

```text
                         membership, routes, leases
                    +-------------------------------+
                    |           KV master            |
                    +---------------+---------------+
                                    |
                   +----------------+----------------+
                   |                                 |
          +--------v---------+               +-------v--------+
          |   owner client   |<--- KV-RPC --->| external client |
          | DRAM / SSD tier  |               | get / put / RPC |
          | shared-memory    |               | no capacity     |
          +------------------+               +-----------------+
```

A `put` publishes a readable owner value and may persist it to the local SSD tier in the background. A `get` first checks readable memory, then falls back to SSD when the route requires it. For same-node access, shared memory can transfer the value without sending the payload through the network. For remote access, the transport returns a holder or RPC result through the same lifecycle and routing model.

## Public Python surface

The Python layer exposes the following stable groups through `Bridging L2–L3_py`:

- `Bridging L2–L3KvClientConfig` and `new_store(...)` for client construction.
- `put`, `get`, `remove`, `is_exist`, `get_size`, and `count_prefix` for KV operations.
- `MemHolder` for native value access and explicit lifetime management.
- `rpc_call` and `rpc_register` for typed user RPC.
- Lease, metrics, instance, and shutdown helpers for service-plane integration.

The detailed argument and lifecycle rules are documented in the KV-RPC interface guide. The Python and Rust names shown in examples are implementation identifiers and are intentionally preserved.

## Requirements

- Linux for the native runtime and SSD/RDMA paths.
- Python 3.10 or newer.
- The Rust toolchain pinned by [`Bridging L2–L3_rs/rust-toolchain.toml`](./Bridging L2–L3_rs/rust-toolchain.toml).
- An etcd instance for cluster metadata and leases.
- A GreptimeDB endpoint when the monitoring configuration is enabled.

## Install and build

Install the Python package in editable mode:

```bash
python3 -m pip install -e .
```

Build the Rust workspace:

```bash
cargo build --manifest-path Bridging L2–L3_rs/Cargo.toml
```

The Python package imports from `Bridging L2–L3_py`. The native extension is produced by the Rust workspace when a platform-specific binding is required.

## Minimal local workflow

1. Start etcd and, if metrics or log export is enabled, start GreptimeDB.
2. Start a local master and owner:

   ```bash
   python3 examples/start_master_owner.py --with-master
   ```

3. In a second process, run the external client example:

   ```bash
   python3 examples/external_put_get_del.py
   ```

The example writes a value, reads it through the KV client, removes it, and checks that the key no longer exists. Before connecting to a real service plane, adjust the endpoint, cluster name, shared-memory directory, and SSD root in the example configuration.

## Repository layout

- `Bridging L2–L3_py/` — Python client, configuration, monitoring, and runtime helpers.
- `Bridging L2–L3_rs/` — Rust KV implementation, transport, RPC, observability, and Python binding.
- `examples/` — small KV Cache and RPC examples.
- `Bridging L2–L3_doc_en/` — English installation, architecture, and KV-RPC documentation.
- `Bridging L2–L3_doc_cn/` — Chinese installation, architecture, and KV-RPC documentation.
- `setup_and_pack/` — packaging helpers retained by the Python build.

## Documentation

Start with the installation and architecture guides, then use the KV-RPC guide for client behavior:

- [`Bridging L2–L3_doc_en/user_doc/User - 0 - Installation.md`](./Bridging%20L2%E2%80%93L3_doc_en/user_doc/User%20-%200%20-%20Installation.md)
- [`Bridging L2–L3_doc_en/user_doc/User - 1 - Architecture and Concepts.md`](./Bridging%20L2%E2%80%93L3_doc_en/user_doc/User%20-%201%20-%20Architecture%20and%20Concepts.md)
- [`Bridging L2–L3_doc_en/user_doc/User - 3 - KV and RPC Interface.md`](./Bridging%20L2%E2%80%93L3_doc_en/user_doc/User%20-%203%20-%20KV%20and%20RPC%20Interface.md)
- [`Bridging L2–L3_doc_cn/user_doc/用户 - 0 - 安装.md`](./Bridging%20L2%E2%80%93L3_doc_cn/user_doc/%E7%94%A8%E6%88%B7%20-%200%20-%20%E5%AE%89%E8%A3%85.md)
- [`Bridging L2–L3_doc_cn/user_doc/用户 - 1 - 架构和概念.md`](./Bridging%20L2%E2%80%93L3_doc_cn/user_doc/%E7%94%A8%E6%88%B7%20-%201%20-%20%E6%9E%B6%E6%9E%84%E5%92%8C%E6%A6%82%E5%BF%B5.md)
- [`Bridging L2–L3_doc_cn/user_doc/用户 - 3 - KV-RPC接口.md`](./Bridging%20L2%E2%80%93L3_doc_cn/user_doc/%E7%94%A8%E6%88%B7%20-%203%20-%20KV-RPC%E6%8E%A5%E5%8F%A3.md)

The design notes cover routing, holder lifetimes, Python/Rust zero-copy calls, concurrency, and SSD storage.

## Verification

Run source-only checks from the repository root:

```bash
python3 -m compileall -q Bridging L2–L3_py examples setup.py
cargo fmt --manifest-path Bridging L2–L3_rs/Cargo.toml --all -- --check
cargo metadata --manifest-path Bridging L2–L3_rs/Cargo.toml --no-deps --format-version 1
```

Full runtime tests additionally require the native toolchain and configured etcd and monitoring services. The package is intended for source inspection and focused evaluation; it is not a release bundle.

## Review boundary

The review target is the KV Cache and RPC data plane. The public KV contract remains `put` / `get` / `remove` plus typed RPC, while the implementation retains the memory, shared-memory, transport, and SSD details needed to evaluate that contract. Dedicated file-service and message-queue APIs are not included, and the existing source identifiers are deliberately left intact.
