# Bridging L2–L3

Bridging L2–L3 是面向 AI 工作负载的分布式 KV Cache 与 RPC 数据面。这个名称强调在不同内存层级、进程和节点之间连接缓存数据与共享或远端容量。

本仓库是精简的双盲评审源码包，保留 KV Cache 数据路径、点对点传输、共享内存、SSD 后端以及 Python/Rust 接口，便于检查和复现实验所需的核心设计。用户可见的项目名称统一为 Bridging L2–L3；为了保持构建和接口兼容，源码中的模块名、crate 名、配置键和 API 标识保持不变。

## 研究重点

该版本聚焦一个问题：分布式 KV Cache 如何在不同内存层级之间移动和暴露 value，同时保持简单稳定的客户端契约？

- **KV master**：协调成员、key-version 路由、租约和生命周期元数据。
- **owner client**：提供 DRAM 或本地 SSD 容量，并处理本地及远端 value。
- **external client**：接入服务平面，但不贡献存储容量。
- **KV-RPC**：在同一套点对点传输上提供类型化用户调用。
- **共享内存**：提供同机快路径，通过 `MemHolder` 携带 value 元数据。
- **SSD 存储**：当 value 不在 DRAM 中时，作为 owner 的本地回填层。

此前的文件服务、消息队列、部署栈和专用测试基础设施不属于本评审包。KV 路径内部仍需要的文件 IO 和异步通道会保留。

## 架构概览

```text
                         成员、路由与租约
                    +--------------------------+
                    |        KV master         |
                    +------------+-------------+
                                 |
                +----------------+----------------+
                |                                 |
        +-------v----------+               +------v---------+
        |   owner client   |<--- KV-RPC --->| external client |
        | DRAM / SSD 层    |               | get / put / RPC |
        | 共享内存         |               | 不贡献容量     |
        +------------------+               +-----------------+
```

`put` 在 owner value 可读后发布结果，并可在后台写入本地 SSD 层。`get` 优先读取可用内存副本，路由要求时再从 SSD 回填。同机访问可以通过共享内存传递 value，避免每次都经过网络；远端访问则沿用同一套生命周期和路由模型，通过传输返回 holder 或 RPC 结果。

## Python 公共接口

`fluxon_py` 提供以下几组稳定接口：

- `FluxonKvClientConfig` 与 `new_store(...)`：创建客户端。
- `put`、`get`、`remove`、`is_exist`、`get_size`、`count_prefix`：KV 操作。
- `MemHolder`：访问 native value 并管理显式生命周期。
- `rpc_call` 与 `rpc_register`：类型化用户 RPC。
- 租约、指标、实例信息和关闭操作：接入服务平面。

参数和生命周期规则详见 KV-RPC 接口文档。示例中的 Python/Rust 名称属于实现标识，按要求保持不变。

## 环境要求

- Linux（原生运行时以及 SSD/RDMA 路径）。
- Python 3.10 或更高版本。
- [`fluxon_rs/rust-toolchain.toml`](./fluxon_rs/rust-toolchain.toml) 指定的 Rust 工具链。
- 用于集群元数据和租约的 etcd。
- 启用监控配置时使用 GreptimeDB endpoint。

## 安装与构建

以 editable 模式安装 Python 包：

```bash
python3 -m pip install -e .
```

构建 Rust workspace：

```bash
cargo build --manifest-path fluxon_rs/Cargo.toml
```

Python 包从 `fluxon_py` 导入；需要平台相关原生绑定时，由 Rust workspace 构建对应扩展。

## 最小本地流程

1. 启动 etcd；如果启用了指标或日志导出，再启动 GreptimeDB。
2. 启动本地 master 和 owner：

   ```bash
   python3 examples/start_master_owner.py --with-master
   ```

3. 在另一个进程运行 external client 示例：

   ```bash
   python3 examples/external_put_get_del.py
   ```

示例会写入 value，通过 KV client 读取，再删除并检查 key 已不存在。接入真实服务平面前，请按环境修改 endpoint、cluster name、共享内存目录和 SSD 根目录。

## 目录结构

- `fluxon_py/`：Python 客户端、配置、监控和运行时辅助代码。
- `fluxon_rs/`：Rust KV 实现、传输、RPC、可观测性和 Python 绑定。
- `examples/`：KV Cache 与 RPC 示例。
- `fluxon_doc_en/`：英文安装、架构和 KV-RPC 文档。
- `fluxon_doc_cn/`：中文安装、架构和 KV-RPC 文档。
- `setup_and_pack/`：Python 构建保留的打包辅助工具。

## 文档入口

建议先阅读安装和架构文档，再阅读 KV-RPC 接口文档了解客户端行为：

- [`fluxon_doc_en/user_doc/User - 0 - Installation.md`](./fluxon_doc_en/user_doc/User%20-%200%20-%20Installation.md)
- [`fluxon_doc_en/user_doc/User - 1 - Architecture and Concepts.md`](./fluxon_doc_en/user_doc/User%20-%201%20-%20Architecture%20and%20Concepts.md)
- [`fluxon_doc_en/user_doc/User - 3 - KV and RPC Interface.md`](./fluxon_doc_en/user_doc/User%20-%203%20-%20KV%20and%20RPC%20Interface.md)
- [`fluxon_doc_cn/user_doc/用户 - 0 - 安装.md`](./fluxon_doc_cn/user_doc/%E7%94%A8%E6%88%B7%20-%200%20-%20%E5%AE%89%E8%A3%85.md)
- [`fluxon_doc_cn/user_doc/用户 - 1 - 架构和概念.md`](./fluxon_doc_cn/user_doc/%E7%94%A8%E6%88%B7%20-%201%20-%20%E6%9E%B6%E6%9E%84%E5%92%8C%E6%A6%82%E5%BF%B5.md)
- [`fluxon_doc_cn/user_doc/用户 - 3 - KV-RPC接口.md`](./fluxon_doc_cn/user_doc/%E7%94%A8%E6%88%B7%20-%203%20-%20KV-RPC%E6%8E%A5%E5%8F%A3.md)

设计文档进一步说明路由、holder 生命周期、Python/Rust 零拷贝调用、并发和 SSD 存储。

## 验证命令

在仓库根目录运行源码检查：

```bash
python3 -m compileall -q fluxon_py examples setup.py
cargo fmt --manifest-path fluxon_rs/Cargo.toml --all -- --check
cargo metadata --manifest-path fluxon_rs/Cargo.toml --no-deps --format-version 1
```

完整运行测试还需要原生工具链以及已配置的 etcd 和监控服务。本仓库用于源码检查和聚焦评估，不是完整发布包。

## 评审边界

评审目标是 KV Cache 与 RPC 数据面。公共 KV 契约仍是 `put` / `get` / `remove` 加类型化 RPC，实现中保留内存、共享内存、传输和 SSD 细节以支持评估。独立的文件服务和消息队列接口不包含在内，现有源码标识也有意保持不变。
