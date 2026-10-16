from __future__ import annotations

TRANSPORT_BACKENDS: tuple[str, ...] = ("fastws", "tquic", "sockudo_ws", "tcp", "tcp_thread")
RDMA_BACKENDS: tuple[str, ...] = ("closed_sdk",)
PUBLIC_TRANSPORT_BACKEND = "tcp_thread"
PUBLIC_TRANSPORT_PROFILE_ID = "Bridging L2–L3_tcp_thread"
TRANSPORT_PROFILE_IDS: dict[str, str] = {
    "fastws": "Bridging L2–L3_fastws",
    "tquic": "Bridging L2–L3_tquic",
    "sockudo_ws": "Bridging L2–L3_sockudo_ws",
    "tcp": "Bridging L2–L3_tcp",
    "tcp_thread": "Bridging L2–L3_tcp_thread",
}

__all__ = [
    "TRANSPORT_BACKENDS",
    "RDMA_BACKENDS",
    "PUBLIC_TRANSPORT_BACKEND",
    "PUBLIC_TRANSPORT_PROFILE_ID",
    "TRANSPORT_PROFILE_IDS",
]
