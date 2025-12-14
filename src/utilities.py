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