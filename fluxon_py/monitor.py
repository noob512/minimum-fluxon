"""KV observability integration hooks."""

def render_monitor_cli(*, config_path: str, workdir: str) -> str:
    raise RuntimeError("The minimal package exposes KV Cache APIs; use the KV observability backend directly.")

def render_monitor_web(*, config_path: str, workdir: str) -> str:
    raise RuntimeError("The minimal package exposes KV Cache APIs; use the KV observability backend directly.")
