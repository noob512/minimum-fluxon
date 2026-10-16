"""Bridging L2–L3 KV Cache Python API."""

from .kvclient.kvclient_interface import KvClient, KvFuture, MemHolder
from .api_error import (
    Result, ApiFileNotFoundError, FileAccessDeniedError, FileReadError,
    InvalidRangeError, CacheCorruptedError, CacheInvalidationError,
    EtcdError, TransportName, TransportUser, JoinError, InternalError,
    InvalidConfigurationError, ResourceCleanupError, exception_to_ext_error,
    validate_file_range,
)
from .kvclient import KvClientType, new_store
from .config import Bridging L2–L3KvClientConfig
from .monitor import render_monitor_cli, render_monitor_web

__version__ = "0.2.4"
__all__ = [
    "KvClient", "KvFuture", "MemHolder", "Bridging L2–L3MemHolder",
    "KvClientType", "new_store", "Bridging L2–L3KvClientConfig",
    "render_monitor_cli", "render_monitor_web", "Result",
    "ApiFileNotFoundError", "FileAccessDeniedError", "FileReadError",
    "InvalidRangeError", "CacheCorruptedError", "CacheInvalidationError",
    "EtcdError", "TransportName", "TransportUser", "JoinError",
    "InternalError", "InvalidConfigurationError", "ResourceCleanupError",
    "exception_to_ext_error", "validate_file_range",
]

from typing import Any
import importlib
_LAZY_PYO3 = {"Bridging L2–L3MemHolder": ("kvclient.Bridging L2–L3", "Bridging L2–L3MemHolder")}
def __getattr__(name: str) -> Any:
    if name in _LAZY_PYO3:
        mod_name, attr = _LAZY_PYO3[name]
        value = getattr(importlib.import_module(f"{__name__}.{mod_name}"), attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY_PYO3))
