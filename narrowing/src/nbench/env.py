"""Environment record and disk bookkeeping. Every command is tolerant: a missing tool is recorded, not fatal."""

from __future__ import annotations

import datetime as dt
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path


def sh(cmd: str, timeout: int = 60) -> str:
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return (r.stdout + r.stderr).strip()
    except Exception as e:  # noqa: BLE001
        return f"unavailable: {type(e).__name__}"


def docker_json(cmd: str):
    out = sh(cmd)
    try:
        return json.loads(out)
    except Exception:  # noqa: BLE001
        return out


def environment(container: str, out: Path, disk_path: str = "/") -> dict:
    import pymongo

    run_url = None
    if os.environ.get("GITHUB_RUN_ID"):
        run_url = f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/{os.environ.get('GITHUB_REPOSITORY')}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    inspect = docker_json(f"docker inspect {container}")
    info = inspect[0] if isinstance(inspect, list) and inspect else {}
    host_cfg = info.get("HostConfig", {})
    image_id = info.get("Image", "")
    image_name = info.get("Config", {}).get("Image", "")
    digests = docker_json(f"docker image inspect {image_name}")
    repo_digests = digests[0].get("RepoDigests") if isinstance(digests, list) and digests else None
    logs = sh(f"docker logs {container} 2>&1 | head -c 2000000")
    mongot_ver = sorted(set(re.findall(r"mongot[^\n]{0,40}?(\d+\.\d+\.\d+)", logs)))[:3]
    env = {
        "date_utc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "workflow_run_url": run_url,
        "github_run_number": os.environ.get("GITHUB_RUN_NUMBER"),
        "runner_image": {"ImageOS": os.environ.get("ImageOS"), "ImageVersion": os.environ.get("ImageVersion"),
                         "RUNNER_NAME_masked": bool(os.environ.get("RUNNER_NAME")), "RUNNER_OS": os.environ.get("RUNNER_OS")},
        "os_release": sh("grep PRETTY_NAME /etc/os-release") if Path("/etc/os-release").exists() else platform.platform(),
        "nproc": os.cpu_count(),
        "lscpu_model": sh("lscpu | grep -i 'model name'") if shutil.which("lscpu") else "n/a",
        "free_m": sh("free -m") if shutil.which("free") else "n/a",
        "df_h": sh(f"df -h {disk_path}") if shutil.which("df") else "n/a",
        "docker_version": sh("docker version --format '{{.Server.Version}}'").strip("'"),
        "container": {
            "name": container,
            "image": image_name,
            "image_id": image_id,
            "repo_digests": repo_digests,
            "cpu_limit_nano": host_cfg.get("NanoCpus"),
            "memory_limit_bytes": host_cfg.get("Memory"),
        },
        "mongod_version": sh(f"docker exec {container} mongod --version | head -1"),
        "mongot_version_from_logs": mongot_ver,
        "python": sys.version.split()[0],
        "pymongo": pymongo.version,
    }
    out.write_text(json.dumps(env, indent=2))
    return env


class DiskSampler:
    """Samples free space of the host disk every 2 s, so peak usage over a phase is measured, not assumed."""

    def __init__(self, path: str = "/") -> None:
        self.path = path
        self.start_free = shutil.disk_usage(path).free
        self.min_free = self.start_free
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._loop, daemon=True)
        self._t.start()

    def _loop(self) -> None:
        while not self._stop.wait(2.0):
            self.min_free = min(self.min_free, shutil.disk_usage(self.path).free)

    def stop(self) -> dict:
        self._stop.set()
        self._t.join(timeout=3)
        self.min_free = min(self.min_free, shutil.disk_usage(self.path).free)
        return {"start_free_bytes": self.start_free, "min_free_bytes": self.min_free, "peak_used_bytes": self.start_free - self.min_free}


def container_usage(container: str) -> dict:
    """Bytes under the container's data directories (mongod files, search index files)."""
    out = {}
    for name, path in (("mongod", "/data/db"), ("mongot", "/data/mongot"), ("configdb", "/data/configdb")):
        r = sh(f"docker exec {container} du -sk {path}")
        m = re.match(r"(\d+)\s", r)
        out[name] = int(m.group(1)) * 1024 if m else None
    return out


def stats_once(container: str) -> dict | None:
    try:
        r = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", container], capture_output=True, text=True, timeout=30)
        out = r.stdout.strip()
    except Exception:  # noqa: BLE001
        return None
    try:
        j = json.loads(out)
    except Exception:  # noqa: BLE001
        return None
    mem = j.get("MemUsage", "").split("/")[0].strip()
    m = re.match(r"([\d.]+)\s*([KMG]i?B)", mem)
    mult = {"KiB": 1 / 1024, "KB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}
    return {"t": time.time(), "cpu_pct": float(j.get("CPUPerc", "0").rstrip("%") or 0),
            "mem_mib": float(m.group(1)) * mult[m.group(2)] if m else None}
