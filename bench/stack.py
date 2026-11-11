"""Control the Docker Compose stack from the harness (runs on the host)."""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECT = "market-pipeline"  # `name:` in docker-compose.yml
INFRA = ["kafka-1", "kafka-2", "kafka-3", "kafka-init", "postgres"]
PROFILES = ["--profile", "smoke", "--profile", "bench", "--profile", "live"]


def compose(*args: str, env: dict | None = None, check: bool = True, capture: bool = True) -> str:
    cmd = ["docker", "compose", *args]
    r = subprocess.run(cmd, cwd=ROOT, env={**os.environ, **(env or {})}, text=True, capture_output=capture)
    if check and r.returncode != 0:
        tail = (r.stdout + r.stderr)[-2000:]
        raise RuntimeError(f"{' '.join(cmd)} failed ({r.returncode}), last output:\n{tail}")
    return r.stdout if capture else ""


def docker(*args: str, check: bool = True) -> str:
    r = subprocess.run(["docker", *args], text=True, capture_output=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed: {r.stderr.strip()}")
    return r.stdout


def build(*services: str, env: dict | None = None) -> None:
    """Rebuild images: the application code is copied into them."""
    compose(*PROFILES, "build", *services, env=env)


def reset() -> None:
    """Stop everything and delete all volumes: Kafka data, PostgreSQL, Spark checkpoints."""
    compose(*PROFILES, "down", "-v", "--remove-orphans")


def up(*services: str, env: dict | None = None, wait: bool = True) -> None:
    args = [*PROFILES, "up", "-d", "--no-deps"]
    if wait:
        args.append("--wait")
    compose(*args, *services, env=env)


def start_infra(env: dict | None = None) -> None:
    """Kafka x3 and PostgreSQL healthy, then kafka-init run to completion (idempotent topic creation).
    The one-shot kafka-init is run in the foreground: `up --wait` can fail when it exits mid-wait."""
    compose("up", "-d", "--wait", *[s for s in INFRA if s != "kafka-init"], env=env)
    compose("up", "--no-deps", "--exit-code-from", "kafka-init", "kafka-init", env=env)


def stop(*services: str) -> None:
    compose(*PROFILES, "rm", "-sf", *services)


def kill(container: str) -> None:
    """SIGKILL, like a crash (`docker kill` sends SIGKILL by default)."""
    docker("kill", container)


def is_running(container: str) -> bool:
    out = docker("inspect", "-f", "{{.State.Running}}", container, check=False).strip()
    return out == "true"


def exit_code(container: str) -> int | None:
    out = docker("inspect", "-f", "{{.State.ExitCode}}", container, check=False).strip()
    return int(out) if out.lstrip("-").isdigit() else None


def wipe_volume(volume: str) -> None:
    """Delete a named volume of this project (for example a Spark checkpoint). Its users must be stopped."""
    docker("volume", "rm", "-f", f"{PROJECT}_{volume}")


def logs(container: str) -> str:
    r = subprocess.run(["docker", "logs", container], text=True, capture_output=True)
    return r.stdout + r.stderr


def wait_for_log(container: str, pattern: str, timeout_s: float = 120, count: int = 1) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if logs(container).count(pattern) >= count:
            return
        if not is_running(container):
            raise RuntimeError(
                f"{container} exited while waiting for {pattern!r}:\n{logs(container)[-3000:]}"
            )
        time.sleep(1)
    raise TimeoutError(f"{container}: {pattern!r} not seen within {timeout_s}s")


def machine_info() -> dict:
    """Specs recorded with every run (PLAN.md 4.5)."""
    info: dict = {"cpus": os.cpu_count()}
    try:
        mem_kb = next(int(line.split()[1]) for line in open("/proc/meminfo") if line.startswith("MemTotal"))
        info["mem_gb"] = round(mem_kb / 1024 / 1024, 1)
    except (OSError, StopIteration):
        pass
    info["docker"] = docker("version", "--format", "{{.Server.Version}}", check=False).strip()
    info["uname"] = " ".join(os.uname())
    return info
