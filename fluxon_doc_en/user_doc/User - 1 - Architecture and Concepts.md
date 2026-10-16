# Architecture and Concepts

Bridging L2–L3 is a distributed KV Cache and RPC data plane. A KV master coordinates membership and metadata; owner clients provide shared-memory or SSD-backed capacity; external clients access the same data through RPC and peer-to-peer transport.

## Core roles

- **KV master**: membership, routing, and lifecycle coordination.
- **Owner client**: contributes cache capacity and serves local and remote KV operations.
- **External client**: attaches to an existing KV service plane without contributing capacity.
- **RPC**: typed user RPC over the KV transport.

The remaining design documents describe the KV storage, transport, lifecycle, and observability paths.
