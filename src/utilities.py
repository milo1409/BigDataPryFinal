from __future__ import annotations

import json
import os
import time
import functools
from pathlib import Path
from typing import Any, Dict, Optional, Union, Callable, Iterable
import subprocess
import sys
from pyspark.sql import SparkSession
import pandas as pd
import traceback

try:
    import psutil
except Exception:
    psutil = None


class Utils:

    @staticmethod
    def timeit(logger: Optional[Callable[[str], None]] = None, label: Optional[str] = None):
        def deco(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                t0 = time.perf_counter()
                try:
                    return fn(*args, **kwargs)
                finally:
                    dt = time.perf_counter() - t0
                    msg = f"[timeit] {label or fn.__name__}: {dt:.3f}s"
                    (logger or print)(msg)
            return wrapper
        return deco
    

    @staticmethod
    def resourceit(logger: Optional[Callable[[str], None]] = None, label: Optional[str] = None):
        def deco(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                proc = psutil.Process(os.getpid()) if psutil else None
                mem0 = proc.memory_info().rss if proc else None
                t0 = time.perf_counter()
                try:
                    return fn(*args, **kwargs)
                finally:
                    dt = time.perf_counter() - t0
                    if proc:
                        mem1 = proc.memory_info().rss
                        dmem = (mem1 - mem0) / (1024 * 1024)
                        mem1_mb = mem1 / (1024 * 1024)
                        msg = f"[resourceit] {label or fn.__name__}: {dt:.3f}s | RSS {mem1_mb:.1f} MB (Δ {dmem:+.1f} MB)"
                    else:
                        msg = f"[resourceit] {label or fn.__name__}: {dt:.3f}s | psutil no disponible (solo tiempo)"
                    (logger or print)(msg)
            return wrapper
        return deco
    
    @staticmethod
    def read_json_config(
        source: Union[str, Path, Dict[str, Any]],
        *,
        defaults: Optional[Dict[str, Any]] = None,
        required_keys: Optional[Iterable[str]] = None,
        encoding: str = "utf-8"
    ) -> Dict[str, Any]:

        cfg = Utils._load_json(source, encoding=encoding)
        if not isinstance(cfg, dict):
            raise ValueError("La configuración debe ser un objeto JSON (dict).")

        if defaults:
            cfg = Utils._deep_merge(defaults, cfg)

        if required_keys:
            missing = [k for k in required_keys if not Utils._has_path(cfg, k)]
            if missing:
                raise ValueError(f"Faltan llaves requeridas en config: {missing}")

        return cfg
    
    @staticmethod
    def _load_json(source: Union[str, Path, Dict[str, Any]], *, encoding: str = "utf-8") -> Any:
        if isinstance(source, dict):
            return source

        s = str(source)
        if s.lstrip().startswith(("{", "[")):
            return json.loads(s)

        p = Path(s)
        if not p.exists():
            raise FileNotFoundError(f"No existe el archivo: {p}")
        return json.loads(p.read_text(encoding=encoding))
    
    @staticmethod
    def resolve_path(path: str, base_path: str | None = None) -> str:
        import os
        from pathlib import Path

        if path is None:
            raise ValueError("path no puede ser None")

        p = str(path).strip()

        if os.path.isabs(p):
            return str(Path(p).expanduser().resolve())

        if base_path:
            return str((Path(base_path).expanduser().resolve() / p).resolve())

        return str(Path(p).expanduser().resolve())

    @staticmethod
    def pip_install_requirements(requirements_file: str, *, base_path: Optional[str] = None, logger=None) -> int:
    
        req_path = Utils.resolve_path(requirements_file, base_path=base_path)

        cmd = [sys.executable, "-m", "pip", "install", "-r", req_path]
        res = subprocess.run(cmd, capture_output=True, text=True)

        if res.stdout:
            (logger or print)(res.stdout)
        if res.returncode != 0:
            if res.stderr:
                (logger or print)(res.stderr)
            raise RuntimeError(f"pip install falló (code={res.returncode}).")

        return res.returncode

    @staticmethod
    def pip_install_requirements_from_config(cfg: Dict[str, Any],*,key: str = "load_requirements",base_key: Optional[str] = "base_path",fallback_base_path: Optional[str] = None,logger=None) -> int:
        if key not in cfg:
            raise KeyError(f"No existe la llave '{key}' en la configuración.")

        req = cfg[key]
        if not isinstance(req, str):
            raise TypeError(f"cfg['{key}'] debe ser string (ruta a requirements.txt).")

        base_path = None
        if base_key and isinstance(cfg.get(base_key), str) and cfg.get(base_key):
            base_path = cfg.get(base_key)
        else:
            base_path = fallback_base_path

        return Utils.pip_install_requirements(req, base_path=base_path, logger=logger)
    
    @staticmethod
    def list_files(folder: str, pattern: str = "*", recursive: bool = True):

        root = Path(folder).expanduser().resolve()
        if not root.exists():
            return []

        if recursive:
            paths = root.rglob(pattern)
        else:
            paths = root.glob(pattern)

        return sorted([str(p.resolve()) for p in paths if p.is_file()])
    
    @staticmethod
    def get_spark(app_name="ETL", master="local[*]"):
        return SparkSession.builder.appName(app_name).master(master).getOrCreate()
    
    @staticmethod
    def _logs_dir(base_path: str) -> str:
        from pathlib import Path
        logs_dir = Path(base_path).expanduser().resolve() / "data" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        return str(logs_dir)

    @staticmethod
    def _now_iso():
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    @staticmethod
    def _proc_metrics():
       
        if psutil is None:
            return {}

        p = psutil.Process(os.getpid())
        mem = p.memory_info()
        cpu = p.cpu_times()

        return {
            "pid": p.pid,
            "rss_mb": mem.rss / (1024 * 1024),
            "vms_mb": mem.vms / (1024 * 1024),
            "cpu_user_s": float(getattr(cpu, "user", 0.0)),
            "cpu_system_s": float(getattr(cpu, "system", 0.0)),
        }

    @staticmethod
    def log_performance_event(
        base_path: str,
        event: Dict[str, Any],
        filename: str = "performance.jsonl",
    ) -> str:
        
        log_dir = Utils._logs_dir(base_path)
        log_path = os.path.join(log_dir, filename)

        payload = {
            "timestamp": Utils._now_iso(),
            **event,
        }

        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

        return log_path

    
    @staticmethod
    def perf_logger(
        base_path: str,
        name: Optional[str] = None,
        filename: str = "performance.jsonl",
    ):

        def decorator(fn):
            label = name or fn.__name__

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                t0 = time.perf_counter()
                m0 = Utils._proc_metrics()
                ok = True
                err = None

                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    ok = False
                    err = {
                        "error_type": type(e).__name__,
                        "error_msg": str(e),
                        "trace": traceback.format_exc(limit=15),
                    }
                    raise
                finally:
                    t1 = time.perf_counter()
                    m1 = Utils._proc_metrics()

                    event = {
                        "event": "function_perf",
                        "name": label,
                        "function": fn.__name__,
                        "elapsed_s": t1 - t0,
                        "ok": ok,
                    }

                    if m0 and m1:
                        event.update({
                            "cpu_user_s_start": m0.get("cpu_user_s"),
                            "cpu_user_s_end": m1.get("cpu_user_s"),
                            "cpu_system_s_start": m0.get("cpu_system_s"),
                            "cpu_system_s_end": m1.get("cpu_system_s"),
                            "rss_mb_start": m0.get("rss_mb"),
                            "rss_mb_end": m1.get("rss_mb"),
                            "rss_mb_delta": (
                                m1.get("rss_mb") - m0.get("rss_mb")
                                if m0.get("rss_mb") is not None else None
                            ),
                            "pid": m1.get("pid"),
                        })

                    if err:
                        event.update(err)

                    Utils.log_performance_event(
                        base_path=base_path,
                        event=event,
                        filename=filename,
                    )

            return wrapper

        return decorator

    @staticmethod
    def perf_scope(
        base_path: str,
        name: str,
        filename: str = "performance.jsonl",
    ):

        class _Scope:
            def __enter__(self):
                self.t0 = time.perf_counter()
                self.m0 = Utils._proc_metrics()
                return self

            def __exit__(self, exc_type, exc, tb):
                t1 = time.perf_counter()
                m1 = Utils._proc_metrics()
                ok = exc is None

                event = {
                    "event": "scope_perf",
                    "name": name,
                    "elapsed_s": t1 - self.t0,
                    "ok": ok,
                }

                if self.m0 and m1:
                    event.update({
                        "cpu_user_s_start": self.m0.get("cpu_user_s"),
                        "cpu_user_s_end": m1.get("cpu_user_s"),
                        "cpu_system_s_start": self.m0.get("cpu_system_s"),
                        "cpu_system_s_end": m1.get("cpu_system_s"),
                        "rss_mb_start": self.m0.get("rss_mb"),
                        "rss_mb_end": m1.get("rss_mb"),
                        "rss_mb_delta": (
                            m1.get("rss_mb") - self.m0.get("rss_mb")
                            if self.m0.get("rss_mb") is not None else None
                        ),
                        "pid": m1.get("pid"),
                    })

                if not ok:
                    event.update({
                        "error_type": exc_type.__name__ if exc_type else None,
                        "error_msg": str(exc) if exc else None,
                        "trace": "".join(traceback.format_tb(tb, limit=10)) if tb else None,
                    })

                Utils.log_performance_event(
                    base_path=base_path,
                    event=event,
                    filename=filename,
                )
                return False 

        return _Scope()