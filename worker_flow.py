from prefect import flow, task
import subprocess
import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional, Union, List

CommandType = Union[str, List[str]]


def _run(
    cmd: CommandType,
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    check: bool = True,
    prefix: str = "",
) -> Dict[str, Any]:
    """Run a command and stream its output line by line."""
    use_shell = isinstance(cmd, str)
    label = " ".join(cmd) if isinstance(cmd, list) else cmd
    print(f"[{prefix}] {label}" if prefix else label)
    sys.stdout.flush()

    process = subprocess.Popen(
        cmd,
        shell=use_shell,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env or os.environ.copy(),
    )
    lines: list[str] = []
    for line in iter(process.stdout.readline, ""):
        if line:
            print(line.rstrip())
            lines.append(line)
            sys.stdout.flush()
    returncode = process.wait()
    output = "".join(lines)

    if check and returncode != 0:
        raise RuntimeError(f"Command failed (rc={returncode}): {label}\n{output}")

    return {"returncode": returncode, "stdout": output, "stderr": ""}


def _ensure_filelock():
    """Import filelock, installing it via uv into the current interpreter if missing."""
    try:
        from filelock import FileLock  # noqa: F401
        return
    except ImportError:
        pass

    print("[ensure_filelock] filelock missing — installing via uv")
    sys.stdout.flush()
    _run(
        "command -v uv || curl -LsSf https://astral.sh/uv/install.sh | sh",
        prefix="ensure uv",
    )
    # Resolve uv even if it was just installed into ~/.local/bin or ~/.cargo/bin.
    uv_bin = "uv"
    for candidate in (Path.home() / ".local/bin/uv", Path.home() / ".cargo/bin/uv"):
        if candidate.exists():
            uv_bin = str(candidate)
            break
    _run(
        f"{uv_bin} pip install --python {sys.executable} filelock",
        prefix="uv pip install filelock",
    )


def _inject_token(url: str, token: str) -> str:
    if token and url.startswith("https://"):
        return url.replace("https://", f"https://oauth2:{token}@", 1)
    return url


def _clone(authed_url: str, path: str, branch: Optional[str], env: dict) -> None:
    cmd = f"git clone --depth 1{f' --branch {branch}' if branch else ''} {authed_url} {path}"
    _run(cmd, env=env, prefix="git clone")


def _fetch(path: str, env: dict) -> None:
    _run("git fetch --all --prune", cwd=path, env=env, prefix="git fetch")


def _clean(path: str, env: dict) -> None:
    _run("git reset --hard", cwd=path, env=env, prefix="git reset")
    _run("git clean -fd",    cwd=path, env=env, prefix="git clean")


def _checkout_commit(path: str, commit: str, env: dict) -> None:
    _run("git fetch --unshallow", cwd=path, env=env, prefix="git fetch", check=False)
    _run(f"git checkout {commit}", cwd=path, env=env, prefix="git checkout")
    _run("git log -1 --oneline",   cwd=path, env=env, prefix="git log")


def _checkout_branch(path: str, branch: str, env: dict) -> None:
    local  = _run(f"git show-ref --verify --quiet refs/heads/{branch}",
                  cwd=path, env=env, check=False, prefix="git show-ref")
    remote = _run(f"git ls-remote --heads origin {branch}",
                  cwd=path, env=env, check=False, prefix="git ls-remote")

    remote_exists = (
        remote["returncode"] == 0
        and f"refs/heads/{branch}" in remote["stdout"]
    )

    if local["returncode"] == 0:
        result = _run(f"git checkout {branch}",
                      cwd=path, env=env, check=False, prefix="git checkout")
    elif remote_exists:
        result = _run(f"git checkout -b {branch} origin/{branch}",
                      cwd=path, env=env, check=False, prefix="git checkout")
    else:
        branches = _run("git branch -r", cwd=path, env=env, check=False)
        raise RuntimeError(
            f"Branch '{branch}' not found locally or on remote.\n"
            f"Available remote branches:\n{branches['stdout']}"
        )

    if result["returncode"] != 0:
        raise RuntimeError(f"Failed to checkout '{branch}':\n{result['stdout']}")

    if remote_exists:
        _run(f"git pull --ff-only origin {branch} || git reset --hard origin/{branch}",
             cwd=path, env=env, check=False, prefix="git pull")

    _run("git log -1 --oneline", cwd=path, env=env, prefix="git log")


def _run_init_script(path: str, script: str, env: dict) -> None:
    init_sh = Path(path) / script
    if init_sh.exists():
        _run(f"bash {init_sh}", cwd=path, env=env, prefix=script)


@task
def setup_git_token(github_token: str) -> None:
    """Write GitHub PAT to ~/.netrc so all git commands pick it up."""
    netrc = Path.home() / ".netrc"
    entry = f"machine github.com login oauth2 password {github_token}\n"
    existing = netrc.read_text() if netrc.exists() else ""
    if "machine github.com" not in existing:
        with netrc.open("a") as f:
            f.write(entry)
        netrc.chmod(0o600)
    print("[setup_git_token] ~/.netrc configured")


@task
def setup_repository(
    repo_url: str,
    repo_path: str,
    branch: Optional[str] = None,
    commit: Optional[str] = None,
    github_token: Optional[str] = None,
    init_script: str = "init.sh",
) -> str:
    """
    Clone or update the repo, checkout branch/commit, run init.sh.
    All git operations are protected by a per-repo file lock.
    """
    _ensure_filelock()
    from filelock import FileLock

    path_obj  = Path(repo_path).expanduser().resolve()
    path_str  = str(path_obj)
    lock_path = path_obj.parent / f".{path_obj.name}.lock"
    authed_url = _inject_token(repo_url, github_token or "")
    env = os.environ.copy()

    print(f"[setup_repository] Waiting for lock: {lock_path}")
    with FileLock(str(lock_path), timeout=600):
        print("[setup_repository] Lock acquired")

        if not path_obj.exists() or not (path_obj / ".git").exists():
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            _clone(authed_url, path_str, branch, env)
        elif branch or commit:
            _fetch(path_str, env)

        if branch or commit:
            _clean(path_str, env)

        if commit:
            _checkout_commit(path_str, commit, env)
        elif branch:
            _checkout_branch(path_str, branch, env)

        _run_init_script(path_str, init_script, env)

    print(f"[setup_repository] Done — {path_str}")
    return path_str


@task
def run_uv_sync(repo_path: str) -> None:
    """Install project dependencies with uv sync."""
    env = {**os.environ.copy(), "TZ": "Europe/London"}
    _run(
        "command -v uv || curl -LsSf https://astral.sh/uv/install.sh | sh",
        env=env, prefix="ensure uv",
    )
    _run("uv sync", cwd=repo_path, env=env, prefix="uv sync")
    print("[run_uv_sync] Done")


@task
def run_command(repo_path: str, cmd: CommandType) -> Dict[str, Any]:
    """Execute cmd inside the repo directory."""
    label = " ".join(cmd) if isinstance(cmd, list) else cmd
    print(f"[run_command] {label}")
    return _run(cmd, cwd=repo_path, prefix="run")


@flow(name="setup-update-run-cmd-flow", log_prints=True)
def setup_update_run_cmd_flow(
    repo_url: str,
    repo_local_path: str,
    github_token: str = "",
    branch: Optional[str] = None,
    commit: Optional[str] = None,
    cmd: CommandType = "echo 'hello from prefect'",
) -> Dict[str, Any]:
    """
    Full pipeline: auth → clone/update repo → uv sync → run cmd.

    Args:
        repo_url:        Experiment repo URL (HTTPS)
        repo_local_path: Where to clone the repo on this worker
        github_token:    GitHub PAT for cloning private repos
        branch:          Branch to checkout (optional)
        commit:          Commit hash to checkout (takes priority over branch)
        cmd:             Command to run — str uses shell=True, list uses shell=False
    """
    setup_git_token(github_token)
    repo_path = setup_repository(
        repo_url,
        repo_local_path,
        branch=branch,
        commit=commit,
        github_token=github_token,
    )
    run_uv_sync(repo_path)
    return run_command(repo_path, cmd)


if __name__ == "__main__":
    setup_update_run_cmd_flow(cmd="echo 'hello from prefect'")
