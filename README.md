# Fluxon

Fluxon is a distributed KV Cache and RPC data plane for AI workloads. This blind-review package focuses on the KV Cache core, peer-to-peer transport, shared-memory operation, SSD backing, and Python/Rust RPC APIs.

## Scope

- Distributed KV Cache with owner and external client modes
- KV-RPC and peer-to-peer transport
- Shared-memory and SSD-backed cache storage
- Python client and Rust implementation

## Repository layout

- `fluxon_py/`: Python client, configuration, monitoring, and runtime helpers
- `fluxon_rs/`: Rust KV, transport, RPC, observability, and Python bindings
- `examples/`: KV Cache usage examples
- `fluxon_doc_en/` and `fluxon_doc_cn/`: user and architecture documentation

## Quick start

See the installation and KV-RPC interface guides in `fluxon_doc_en/user_doc/` or `fluxon_doc_cn/user_doc/`.
