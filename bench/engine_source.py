"""Resolve rayforce source: local directory or git branch.

Lets users benchmark a development branch without manually cloning. Pattern
borrowed from teide-bench/engine_utils.py.

The resolved directory is fed back into RayforceAdapter(local_path=...),
so a branch ends up acting like a local checkout.
"""

import os
import subprocess
from pathlib import Path


# rayforce-py is the Python wrapper; the C core lives in RayforceDB/rayforce
# but is consumed by rayforce-py via vendoring during its build.
RAYFORCE_PY_REPO = "https://github.com/RayforceDB/rayforce-py.git"
RAYFORCE_C_REPO  = "https://github.com/RayforceDB/rayforce.git"

DEPS_DIR = Path(__file__).resolve().parent.parent / ".deps"


def _git_capture(args: list[str]) -> str:
    try:
        return subprocess.check_output(args, stderr=subprocess.DEVNULL,
                                       text=True).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return ""


def git_info(directory: Path) -> tuple[str, str, bool]:
    """Return (branch, short-commit, is-dirty) for a git working tree."""
    d = str(directory)
    branch = _git_capture(["git", "-C", d, "branch", "--show-current"])
    commit = _git_capture(["git", "-C", d, "log", "--oneline", "-1",
                           "--no-decorate"])[:12]
    dirty = bool(_git_capture(["git", "-C", d, "status", "--porcelain"]))
    return branch, commit, dirty


def resolve_branch(repo: str, branch: str, name: str) -> Path:
    """Clone or update <repo> at <branch> into .deps/<name>-<branch>."""
    DEPS_DIR.mkdir(exist_ok=True)
    safe = branch.replace("/", "_")
    clone_dir = DEPS_DIR / f"{name}-branch-{safe}"

    if clone_dir.exists():
        subprocess.run(["git", "-C", str(clone_dir), "fetch", "-q", "origin"],
                       check=True, capture_output=True)
        subprocess.run(["git", "-C", str(clone_dir), "checkout", "-q", branch],
                       capture_output=True)
        subprocess.run(["git", "-C", str(clone_dir), "reset", "-q", "--hard",
                        f"origin/{branch}"], capture_output=True)
    else:
        subprocess.run(["git", "clone", "--depth", "1", "-b", branch, repo,
                        str(clone_dir)], check=True, capture_output=True)

    print(f"  Resolved {name} branch '{branch}' -> {clone_dir}")
    return clone_dir


def resolve_rayforce_py(local_dir: str | None,
                        branch: str | None) -> Path | None:
    """Pick a rayforce-py source: --rayforce-local wins, else --rayforce-branch.

    Returns the directory to pass as --rayforce-local to the worker, or None
    if neither flag is set (worker falls back to PyPI).
    """
    if local_dir:
        return Path(local_dir).expanduser().resolve()
    if branch:
        return resolve_branch(RAYFORCE_PY_REPO, branch, "rayforce-py")
    return None


def engine_label(engine: str, src_dir: Path | None) -> str:
    """Build a label like 'rayforce@feature/sort (a1b2c3d) dirty' for reports."""
    if src_dir is None:
        return engine
    branch, commit, dirty = git_info(src_dir)
    parts = [engine + (f"@{branch}" if branch else "")]
    if commit:
        parts.append(f"({commit})")
    if dirty:
        parts.append("dirty")
    return " ".join(parts)
