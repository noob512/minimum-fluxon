# Fluxon

Fluxon 是面向 AI 工作负载的分布式 KV Cache 与 RPC 数据面。本双盲评审版本聚焦 KV Cache 核心、点对点传输、共享内存、SSD 后端以及 Python/Rust RPC 接口。

## 范围

- 分布式 KV Cache，支持 owner 与 external client 模式
- KV-RPC 与点对点传输
- 共享内存与 SSD 后端
- Python 客户端和 Rust 实现

## 目录

- `fluxon_py/`：Python 客户端、配置、监控和运行时辅助代码
- `fluxon_rs/`：Rust KV、传输、RPC、可观测性和 Python 绑定
- `examples/`：KV Cache 使用示例
- `fluxon_doc_en/`、`fluxon_doc_cn/`：用户与架构文档

## 快速开始

请参阅 `fluxon_doc_en/user_doc/` 或 `fluxon_doc_cn/user_doc/` 中的安装及 KV-RPC 接口文档。
