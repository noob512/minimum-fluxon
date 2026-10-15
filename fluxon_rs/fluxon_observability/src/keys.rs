pub const KEY_CLUSTER_NAME: &str = "fluxon_cluster_name";
pub const KEY_MEMBER_KIND: &str = "fluxon_member_kind";
pub const KEY_ROLE: &str = "fluxon_role";
pub const KEY_MEMBER_ID: &str = "fluxon_member_id";

pub const GREPTIME_LOG_EXTRACT_KEYS_HEADER_VALUE: &str =
    "fluxon_cluster_name,fluxon_member_kind,fluxon_role,fluxon_member_id";

// ---------------- Prometheus/remote-write label keys (generic) ----------------
//
// We intentionally centralize label names to avoid schema divergence between:
// - emitters (business crates)
// - query side (KV monitor PromQL)
//
// Keep these keys stable once published.
pub const PROM_LABEL_NODE: &str = "node";
pub const PROM_LABEL_ROLE: &str = "role";
pub const PROM_LABEL_METRIC: &str = "metric";
pub const PROM_LABEL_STAT: &str = "stat";
pub const PROM_LABEL_COMPONENT: &str = "component";
pub const PROM_LABEL_PEER: &str = "peer";
pub const PROM_LABEL_RDMA_DEVICE: &str = "rdma_device";
pub const PROM_LABEL_RDMA_PORT: &str = "rdma_port";
pub const PROM_LABEL_RDMA_NETDEV: &str = "rdma_netdev";
pub const PROM_LABEL_RDMA_PCI_BDF: &str = "rdma_pci_bdf";
pub const PROM_LABEL_RDMA_TRANSFER_STATE: &str = "rdma_transfer_state";

// ---------------- KV peer network observe schema ----------------
//
// This metric is intentionally "per-peer" (higher cardinality) because it is consumed by the
// topology UI to attribute traffic to owner/external relationships and to machine/sub_cluster
// aggregates. Emitters must use best-effort / non-blocking paths only.
pub const PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL: &str = "kv_peer_network_bytes_total";

pub const PROM_VALUE_KV_COMPONENT_RPC_TRANSPORT: &str = "rpc_transport";
pub const PROM_VALUE_KV_COMPONENT_TRANSFER_ENGINE: &str = "transfer_engine";
// Local IPC payload attribution.
//
// English note:
// - This is not a "network" byte counter; it is payload bytes moved via local IPC tiers, including:
//   - iceoryx2 P2P transport (external <-> external)
//   - owner shared-memory fast path (external <-> owner; direct mmap + memcpy)
// - We emit it under KV peer network metric so topology can aggregate it consistently with other components.
pub const PROM_VALUE_KV_COMPONENT_LOCAL_IPC: &str = "local_ipc";

// ---------------- Process-level resource metrics (generic) ----------------
//
// These gauges are emitted by long-running Fluxon processes via the observe actor.
// Keep names stable once published.
pub const PROM_METRIC_PROCESS_CPU_USAGE_PERCENT: &str = "process_cpu_usage_percent";
pub const PROM_METRIC_CONTAINER_MEMORY_USAGE_BYTES: &str = "container_memory_usage_bytes";
pub const PROM_METRIC_CONTAINER_MEMORY_LIMIT_BYTES: &str = "container_memory_limit_bytes";

// ---------------- KV owner storage observe schema ----------------
//
// Memory segment metrics are emitted by the master from MasterSegManager. KV SSD
// metrics are emitted by each owner because SSD shard/ring state is owner-local.
pub const PROM_METRIC_KV_SSD_CAPACITY_BYTES: &str = "kv_ssd_capacity_bytes";
pub const PROM_METRIC_KV_SSD_USED_BYTES: &str = "kv_ssd_used_bytes";

// ---------------- Tokio runtime observe schema (stable runtime metrics) ----------------
//
// These gauges intentionally use only stable Tokio runtime metrics APIs.
// They are primarily consumed by the KV monitor page to inspect owner runtime health
// without enabling `tokio_unstable`.
pub const PROM_METRIC_TOKIO_NUM_WORKERS: &str = "tokio_num_workers";
pub const PROM_METRIC_TOKIO_ALIVE_TASKS: &str = "tokio_alive_tasks";
pub const PROM_METRIC_TOKIO_GLOBAL_QUEUE_DEPTH: &str = "tokio_global_queue_depth";
pub const PROM_METRIC_TOKIO_BUSY_PERCENT: &str = "tokio_busy_percent";
pub const PROM_METRIC_TOKIO_MAX_WORKER_BUSY_PERCENT: &str = "tokio_max_worker_busy_percent";
pub const PROM_METRIC_TOKIO_PARK_UNPARK_RATE_HZ: &str = "tokio_park_unpark_rate_hz";

// ---------------- RDMA observe schema ----------------
//
// RDMA metrics are emitted by the commu layer from the same periodic self-probe that publishes
// etcd `rdma_runtime`. The schema is intentionally split into:
// - node-scoped health/count gauges
// - per-port gauges keyed by stable hardware/network labels
pub const PROM_METRIC_RDMA_PROBE_PORT_COUNT: &str = "rdma_probe_port_count";
pub const PROM_METRIC_RDMA_PROBE_USABLE_PORT_COUNT: &str = "rdma_probe_usable_port_count";
pub const PROM_METRIC_RDMA_PROBE_ERROR: &str = "rdma_probe_error";
pub const PROM_METRIC_RDMA_PORT_USABLE: &str = "rdma_port_usable";
pub const PROM_METRIC_RDMA_PORT_SPEED_GBPS: &str = "rdma_port_speed_gbps";
pub const PROM_METRIC_RDMA_PORT_ACTIVE_MTU_BYTES: &str = "rdma_port_active_mtu_bytes";
pub const PROM_METRIC_RDMA_PORT_GID_COUNT: &str = "rdma_port_gid_count";
pub const PROM_METRIC_RDMA_PORT_NUMA_NODE: &str = "rdma_port_numa_node";
pub const PROM_METRIC_RDMA_TRANSFER_ENGINE_STATE: &str = "rdma_transfer_engine_state";
pub const PROM_METRIC_RDMA_TRANSFER_ENGINE_START_FAILURES: &str =
    "rdma_transfer_engine_start_failures";

// ---------------- tcp_thread observe schema ----------------
//
// Windowed latency gauges emitted by the observe actor from best-effort transport samples.
pub const PROM_LABEL_TCP_THREAD_LANE: &str = "lane";
pub const PROM_METRIC_TCP_THREAD_LATENCY_STAT_US: &str = "tcp_thread_latency_stat_microseconds";
pub const PROM_METRIC_TCP_THREAD_LATENCY_SAMPLE_COUNT: &str = "tcp_thread_latency_sample_count";
pub const PROM_METRIC_TCP_THREAD_TRANSPORT_BYTES_TOTAL: &str = "tcp_thread_transport_bytes_total";
pub const PROM_METRIC_TCP_THREAD_TRANSPORT_MESSAGES_TOTAL: &str =
    "tcp_thread_transport_messages_total";
pub const PROM_METRIC_P2P_RECV_TRANSPORT_BYTES_TOTAL: &str = "p2p_recv_transport_bytes_total";
pub const PROM_METRIC_P2P_RECV_TRANSPORT_MESSAGES_TOTAL: &str = "p2p_recv_transport_messages_total";
pub const PROM_METRIC_P2P_RPC_COMPLETION_LATENCY_STAT_US: &str =
    "p2p_rpc_completion_latency_stat_microseconds";
pub const PROM_METRIC_P2P_RPC_COMPLETION_LATENCY_SAMPLE_COUNT: &str =
    "p2p_rpc_completion_latency_sample_count";
pub const PROM_METRIC_P2P_RPC_COMPLETION_BYTES_TOTAL: &str = "p2p_rpc_completion_bytes_total";
pub const PROM_METRIC_P2P_RPC_COMPLETION_MESSAGES_TOTAL: &str = "p2p_rpc_completion_messages_total";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_RESPONSE_SUBMITTED: &str = "response_submitted";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_RESPONSE_SUBMIT_FAILED: &str =
    "response_submit_failed";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_USED: &str =
    "user_rpc_request_fast_path_used";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_SLOW_PATH_USED: &str =
    "user_rpc_request_slow_path_used";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_TRANSPORT_POLICY:
    &str = "user_rpc_request_fast_path_bypass_transport_policy";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_LANE_NOT_DIRECT:
    &str = "user_rpc_request_fast_path_bypass_lane_not_direct";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_REMAINING_HOPS:
    &str = "user_rpc_request_fast_path_bypass_remaining_hops";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_PEER_NOT_READY:
    &str = "user_rpc_request_fast_path_bypass_peer_not_ready";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_BACKEND_EPOCH_MISSING:
    &str = "user_rpc_request_fast_path_bypass_backend_epoch_missing";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_FALLBACK_SEND_NOT_READY:
    &str = "user_rpc_request_fast_path_fallback_send_not_ready";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_FALLBACK_SEND_ERROR:
    &str = "user_rpc_request_fast_path_fallback_send_error";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_USED: &str =
    "user_rpc_response_fast_path_used";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_SLOW_PATH_USED: &str =
    "user_rpc_response_slow_path_used";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_TRANSPORT_POLICY:
    &str = "user_rpc_response_fast_path_bypass_transport_policy";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_LANE_NOT_DIRECT:
    &str = "user_rpc_response_fast_path_bypass_lane_not_direct";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_REMAINING_HOPS:
    &str = "user_rpc_response_fast_path_bypass_remaining_hops";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_PEER_NOT_READY:
    &str = "user_rpc_response_fast_path_bypass_peer_not_ready";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_BACKEND_EPOCH_MISSING:
    &str = "user_rpc_response_fast_path_bypass_backend_epoch_missing";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_FALLBACK_SEND_NOT_READY:
    &str = "user_rpc_response_fast_path_fallback_send_not_ready";
pub const PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_FALLBACK_SEND_ERROR:
    &str = "user_rpc_response_fast_path_fallback_send_error";
pub const PROM_VALUE_P2P_RPC_COMPLETION_LATENCY_USER_RPC_REQUEST_FAST_PATH_TOTAL: &str =
    "user_rpc_request_fast_path_total";
pub const PROM_VALUE_P2P_RPC_COMPLETION_LATENCY_USER_RPC_REQUEST_SLOW_PATH_TOTAL: &str =
    "user_rpc_request_slow_path_total";
pub const PROM_VALUE_P2P_RPC_COMPLETION_LATENCY_USER_RPC_RESPONSE_FAST_PATH_TOTAL: &str =
    "user_rpc_response_fast_path_total";
pub const PROM_VALUE_P2P_RPC_COMPLETION_LATENCY_USER_RPC_RESPONSE_SLOW_PATH_TOTAL: &str =
    "user_rpc_response_slow_path_total";
