"""Collect environment.json: runner, hardware, Docker and engine versions, limits, workflow run URL."""

import json
import os
import platform
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path


def _sh(cmd: list[str] | str, timeout: float = 20.0) -> str:
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, shell=isinstance(cmd, str))
        return (out.stdout + out.stderr).strip()
    except Exception as e:  # noqa: BLE001
        return f"unavailable: {type(e).__name__}"


def collect(container: str | None, mongo_client_info: dict | None = None) -> dict:
    import pymongo

    env: dict = {
        "date_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "python": sys.version.split()[0],
        "pymongo": pymongo.version,
        "platform": platform.platform(),
        "runner_image": {k: os.environ.get(k) for k in ("ImageOS", "ImageVersion", "RUNNER_OS", "RUNNER_ARCH", "RUNNER_NAME")},
        "nproc": os.cpu_count(),
    }
    run_url = None
    if os.environ.get("GITHUB_RUN_ID"):
        run_url = (f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/{os.environ.get('GITHUB_REPOSITORY')}"
                   f"/actions/runs/{os.environ['GITHUB_RUN_ID']}")
    env["workflow_run_url"] = run_url
    env["github"] = {k: os.environ.get(k) for k in ("GITHUB_WORKFLOW", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_EVENT_NAME")}
    if shutil.which("lscpu"):
        env["lscpu_model"] = _sh("lscpu | grep -i 'model name' | head -1")
        env["lscpu"] = _sh("lscpu")
    if shutil.which("free"):
        env["free_m"] = _sh(["free", "-m"])
    if shutil.which("df"):
        env["df_h"] = _sh(["df", "-h"])
    if shutil.which("nproc"):
        env["nproc_cmd"] = _sh(["nproc"])
    if Path("/etc/os-release").exists():
        env["os_release"] = Path("/etc/os-release").read_text()[:600]
    if shutil.which("docker"):
        env["docker_version"] = _sh(["docker", "version", "--format", "{{.Server.Version}}"])
        if container:
            insp = _sh(["docker", "inspect", container])
            try:
                d = json.loads(insp)[0]
                env["container"] = {
                    "name": container,
                    "image": d["Config"]["Image"],
                    "image_id": d["Image"],
                    "memory_limit_bytes": d["HostConfig"]["Memory"],
                    "nano_cpus": d["HostConfig"]["NanoCpus"],
                    "cpus": d["HostConfig"]["NanoCpus"] / 1e9 if d["HostConfig"]["NanoCpus"] else None,
                }
                digests = _sh(["docker", "image", "inspect", d["Image"], "--format", "{{json .RepoDigests}}"])
                env["container"]["repo_digests"] = digests
            except Exception as e:  # noqa: BLE001
                env["container"] = {"error": type(e).__name__}
            env["mongot_version"] = _sh(
                ["docker", "exec", container, "sh", "-c",
                 "ls /opt/mongot 2>/dev/null; cat /opt/mongot/VERSION 2>/dev/null; "
                 "grep -rhoE 'mongot[ -]v?[0-9]+\\.[0-9]+\\.[0-9]+' /var/log 2>/dev/null | head -1"])
    if mongo_client_info:
        env["engine"] = mongo_client_info
    return env
