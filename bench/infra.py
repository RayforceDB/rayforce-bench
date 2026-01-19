"""Infrastructure management for benchmark databases."""

import subprocess
import time
import sys


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
        "ports": {"5433": "5432"},  # Use 5433 to avoid conflict with local postgres
        "env": {"POSTGRES_PASSWORD": "postgres"},
        "ready_check": ("localhost", 5433),
        "ready_timeout": 30,
        "post_start": [
            "for i in 1 2 3 4 5; do docker exec {name} psql -U postgres -c 'SELECT 1;' 2>/dev/null && break || sleep 2; done",
            "docker exec {name} psql -U postgres -c 'CREATE DATABASE benchmark;' 2>/dev/null || true",
        ],
    },
}


def is_docker_available() -> bool:
    """Check if Docker is available."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
        )
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


def start_container(db_name: str) -> bool:
    """Start a database container."""
    if db_name not in CONTAINERS:
        print(f"  Unknown database: {db_name}")
        return False

    config = CONTAINERS[db_name]
    name = config["name"]

    # Check if already running
    if is_container_running(name):
        print(f"  ✓ {db_name}: already running")
        return True

    # If container exists but stopped, start it
    if container_exists(name):
        print(f"  Starting existing {db_name} container...")
        result = subprocess.run(["docker", "start", name], capture_output=True)
        if result.returncode != 0:
            print(f"  ✗ Failed to start {db_name}")
            return False
    else:
        # Create and start new container
        print(f"  Creating {db_name} container...")
        cmd = ["docker", "run", "-d", "--name", name]

        for host_port, container_port in config["ports"].items():
            cmd.extend(["-p", f"{host_port}:{container_port}"])

        for key, value in config.get("env", {}).items():
            cmd.extend(["-e", f"{key}={value}"])

        cmd.append(config["image"])

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  ✗ Failed to create {db_name}: {result.stderr}")
            return False

    # Wait for readiness
    host, port = config["ready_check"]
    print(f"  Waiting for {db_name} to be ready...")
    if not wait_for_port(host, port, config["ready_timeout"]):
        print(f"  ✗ {db_name} failed to start (timeout)")
        return False

    # Run post-start commands
    for cmd_template in config.get("post_start", []):
        cmd = cmd_template.format(name=name)
        subprocess.run(cmd, shell=True, capture_output=True)
        time.sleep(0.5)

    print(f"  ✓ {db_name}: ready")
    return True


def stop_container(db_name: str) -> bool:
    """Stop a database container."""
    if db_name not in CONTAINERS:
        return False

    name = CONTAINERS[db_name]["name"]
    if is_container_running(name):
        print(f"  Stopping {db_name}...")
        subprocess.run(["docker", "stop", name], capture_output=True)
        print(f"  ✓ {db_name}: stopped")
    return True


def remove_container(db_name: str) -> bool:
    """Remove a database container."""
    if db_name not in CONTAINERS:
        return False

    name = CONTAINERS[db_name]["name"]
    stop_container(db_name)
    if container_exists(name):
        print(f"  Removing {db_name}...")
        subprocess.run(["docker", "rm", name], capture_output=True)
        print(f"  ✓ {db_name}: removed")
    return True


def start_required_infrastructure(adapters: list[str]) -> bool:
    """Start infrastructure required for the given adapters."""
    # Determine which databases need Docker
    needs_docker = []
    for adapter in adapters:
        if adapter in CONTAINERS:
            needs_docker.append(adapter)

    if not needs_docker:
        return True

    print("\n=== Starting Infrastructure ===")

    # Check Docker
    if not is_docker_available():
        print("  ✗ Docker is not available. Install Docker to use questdb/timescale.")
        print(f"  Skipping: {', '.join(needs_docker)}")
        return False

    # Start required containers
    all_started = True
    for db in needs_docker:
        if not start_container(db):
            all_started = False

    print()
    return all_started


def stop_infrastructure(adapters: list[str] | None = None):
    """Stop infrastructure containers."""
    print("\n=== Stopping Infrastructure ===")

    dbs = adapters if adapters else list(CONTAINERS.keys())
    for db in dbs:
        if db in CONTAINERS:
            stop_container(db)


def cleanup_infrastructure():
    """Remove all infrastructure containers."""
    print("\n=== Cleaning Up Infrastructure ===")
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
        print("\n=== Infrastructure Status ===")
        for db, config in CONTAINERS.items():
            name = config["name"]
            if is_container_running(name):
                print(f"  ✓ {db}: running")
            elif container_exists(name):
                print(f"  ○ {db}: stopped")
            else:
                print(f"  ✗ {db}: not created")
