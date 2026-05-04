"""Infrastructure management for benchmark databases."""

import subprocess
import time


CONTAINERS = {
    "questdb": {
        "image": "questdb/questdb:latest",
        "name": "rayforce-bench-questdb",
        "ports": {"9000": "9000", "8812": "8812", "9009": "9009"},
        "env": {},
        "ready_check": ("localhost", 8812),
        "ready_timeout": 30,
    },
    "timescale": {
        "image": "timescale/timescaledb:latest-pg16",
        "name": "rayforce-bench-timescale",
        "ports": {"5433": "5432"},
        "env": {"POSTGRES_PASSWORD": "postgres"},
        "ready_check": ("localhost", 5433),
        "ready_timeout": 30,
        # Postgres opens its port before initdb finishes, so the port-ready
        # check returns long before psql can actually connect. Wait up to
        # 30s for SELECT 1 to succeed before issuing CREATE DATABASE.
        "post_start": [
            "for i in $(seq 1 15); do docker exec {name} psql -U postgres -c 'SELECT 1;' >/dev/null 2>&1 && break || sleep 2; done",
            "docker exec {name} psql -U postgres -c 'CREATE DATABASE benchmark;' 2>/dev/null || true",
        ],
    },
}


def is_docker_available() -> bool:
    """Check if Docker is available."""
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=5)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def is_container_running(name: str) -> bool:
    """Check if a container is running."""
    result = subprocess.run(
        ["docker", "ps", "-q", "-f", f"name={name}"],
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip())


def container_exists(name: str) -> bool:
    """Check if a container exists (running or stopped)."""
    result = subprocess.run(
        ["docker", "ps", "-aq", "-f", f"name={name}"],
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip())


def wait_for_port(host: str, port: int, timeout: int = 30) -> bool:
    """Wait for a port to become available."""
    import socket

    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                s.connect((host, port))
                return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            time.sleep(0.5)
    return False


def start_container(db_name: str, quiet: bool = False) -> bool:
    """Start a database container."""
    if db_name not in CONTAINERS:
        return False

    config = CONTAINERS[db_name]
    name = config["name"]

    if is_container_running(name):
        return True

    if container_exists(name):
        result = subprocess.run(["docker", "start", name], capture_output=True)
        if result.returncode != 0:
            if not quiet:
                print(f"Failed to start {db_name}")
            return False
    else:
        cmd = ["docker", "run", "-d", "--name", name]
        for host_port, container_port in config["ports"].items():
            cmd.extend(["-p", f"{host_port}:{container_port}"])
        for key, value in config.get("env", {}).items():
            cmd.extend(["-e", f"{key}={value}"])
        cmd.append(config["image"])

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            if not quiet:
                print(f"Failed to create {db_name}")
            return False

    host, port = config["ready_check"]
    if not wait_for_port(host, port, config["ready_timeout"]):
        if not quiet:
            print(f"{db_name} failed to start")
        return False

    for cmd_template in config.get("post_start", []):
        cmd = cmd_template.format(name=name)
        subprocess.run(cmd, shell=True, capture_output=True)
        time.sleep(0.5)

    return True


def stop_container(db_name: str, quiet: bool = False) -> bool:
    """Stop a database container."""
    if db_name not in CONTAINERS:
        return False

    name = CONTAINERS[db_name]["name"]
    if is_container_running(name):
        subprocess.run(["docker", "stop", name], capture_output=True)
    return True


def remove_container(db_name: str) -> bool:
    """Remove a database container."""
    if db_name not in CONTAINERS:
        return False

    name = CONTAINERS[db_name]["name"]
    stop_container(db_name, quiet=True)
    if container_exists(name):
        subprocess.run(["docker", "rm", name], capture_output=True)
    return True


def start_required_infrastructure(adapters: list[str], quiet: bool = False) -> bool:
    """Start infrastructure required for the given adapters."""
    needs_docker = [a for a in adapters if a in CONTAINERS]

    if not needs_docker:
        return True

    if not is_docker_available():
        if not quiet:
            print("Docker not available")
        return False

    all_started = True
    for db in needs_docker:
        if not start_container(db, quiet=quiet):
            all_started = False

    return all_started


def stop_infrastructure(adapters: list[str] | None = None, quiet: bool = False):
    """Stop infrastructure containers."""
    dbs = adapters if adapters else list(CONTAINERS.keys())
    for db in dbs:
        if db in CONTAINERS:
            stop_container(db, quiet=quiet)


def cleanup_infrastructure():
    """Remove all infrastructure containers."""
    for db in CONTAINERS:
        remove_container(db)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Manage benchmark infrastructure")
    parser.add_argument("action", choices=["start", "stop", "cleanup", "status"])
    parser.add_argument("-d", "--databases", nargs="+", default=list(CONTAINERS.keys()))

    args = parser.parse_args()

    if args.action == "start":
        start_required_infrastructure(args.databases)
    elif args.action == "stop":
        stop_infrastructure(args.databases)
    elif args.action == "cleanup":
        cleanup_infrastructure()
    elif args.action == "status":
        for db, config in CONTAINERS.items():
            name = config["name"]
            if is_container_running(name):
                print(f"✓ {db}: running")
            elif container_exists(name):
                print(f"○ {db}: stopped")
            else:
                print(f"✗ {db}: not created")
