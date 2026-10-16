# Installation

Install the Python package and build the Rust extension when native bindings are required. The package in this review release contains the KV Cache and RPC data plane.

## Python

```bash
pip install -e .
```

## Rust

```bash
cargo build --manifest-path fluxon_rs/Cargo.toml
```

Configure the KV master and client using the examples in `examples/` and the KV-RPC guide.
