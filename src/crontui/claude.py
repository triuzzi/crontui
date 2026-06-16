"""Claude task support for crontui.

A *Claude task* is a periodic, unattended `claude -p` run. The crontab line invokes this
module's runner (`crontui exec <slug>`); the task payload (prompt + config) lives in
``~/.crontui/tasks/`` and secrets in ``~/.crontui/env``. This module is intentionally free of
any Textual import so it can run from cron with a minimal environment.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from pathlib import Path

CRONTUI_DIR = Path.home() / ".crontui"
TASKS_DIR = CRONTUI_DIR / "tasks"
LOGS_DIR = CRONTUI_DIR / "logs"
ENV_FILE = CRONTUI_DIR / "env"

# Non-interactive-safe modes first: bypassPermissions assumes "yes", dontAsk assumes "no"
# unless allow-listed. acceptEdits/default/auto can block on a prompt with no TTY — avoid for cron.
PERMISSION_MODES = ["bypassPermissions", "dontAsk", "acceptEdits", "plan", "default", "auto"]
AUTH_MODES = ["oauth", "api_key", "keychain"]
DEFAULT_TIMEOUT_SEC = 1800
OAUTH_TOKEN_VAR = "CLAUDE_CODE_OAUTH_TOKEN"

# Cron's PATH is minimal; these are where Homebrew / user installs put `claude` (and `node`).
_PATH_HINTS = ["/opt/homebrew/bin", "/usr/local/bin", str(Path.home() / ".local" / "bin")]

_EXEC_RE = re.compile(r"(?:-m\s+crontui|(?:^|/)crontui)\s+exec\s+([A-Za-z0-9._-]+)")


# --------------------------------------------------------------------------- model


@dataclass
class TaskConfig:
    description: str = ""
    cwd: str = field(default_factory=lambda: str(Path.home()))
    model: str = ""  # "" => claude's configured default
    permission_mode: str = "bypassPermissions"
    allowed_tools: list[str] = field(default_factory=list)  # only applied when mode == dontAsk
    disallowed_tools: list[str] = field(default_factory=list)
    add_dirs: list[str] = field(default_factory=list)
    max_turns: int | None = None
    max_budget_usd: float | None = None
    timeout_sec: int = DEFAULT_TIMEOUT_SEC
    output_format: str = "text"
    auth: str = "oauth"
    extra_args: list[str] = field(default_factory=list)
    created_at: str = ""

    @classmethod
    def from_dict(cls, data: dict) -> TaskConfig:
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})


# --------------------------------------------------------------------------- slugs


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", name.strip().lower()).strip("-")
    return slug[:48] or "task"


def unique_slug(name: str) -> str:
    base = slugify(name)
    slug, n = base, 2
    while (TASKS_DIR / f"{slug}.json").exists():
        slug = f"{base}-{n}"
        n += 1
    return slug


# --------------------------------------------------------------------------- storage


def task_paths(slug: str) -> tuple[Path, Path]:
    return TASKS_DIR / f"{slug}.json", TASKS_DIR / f"{slug}.md"


def task_exists(slug: str) -> bool:
    return (TASKS_DIR / f"{slug}.json").exists()


def list_task_slugs() -> list[str]:
    if not TASKS_DIR.exists():
        return []
    return sorted(p.stem for p in TASKS_DIR.glob("*.json"))


def save_task(slug: str, cfg: TaskConfig, prompt: str) -> None:
    TASKS_DIR.mkdir(parents=True, exist_ok=True)
    if not cfg.created_at:
        cfg.created_at = _now_iso()
    cfg_path, prompt_path = task_paths(slug)
    cfg_path.write_text(json.dumps(asdict(cfg), indent=2) + "\n")
    prompt_path.write_text(prompt if prompt.endswith("\n") else prompt + "\n")


def load_task(slug: str) -> tuple[TaskConfig, str]:
    cfg_path, prompt_path = task_paths(slug)
    if not cfg_path.exists():
        raise FileNotFoundError(
            f"No Claude task '{slug}' at {cfg_path}. Create one with `crontui add-claude` or the TUI (Ctrl+P)."
        )
    cfg = TaskConfig.from_dict(json.loads(cfg_path.read_text()))
    prompt = prompt_path.read_text() if prompt_path.exists() else ""
    return cfg, prompt


def prompt_preview(prompt: str, width: int = 60) -> str:
    line = next((s.strip() for s in prompt.splitlines() if s.strip()), "")
    return line if len(line) <= width else line[: width - 1] + "…"


# --------------------------------------------------------------------------- env / auth


def load_env_file(path: Path = ENV_FILE) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def write_oauth_token(token: str, path: Path = ENV_FILE) -> None:
    token = token.strip()
    if not token:
        raise ValueError("Empty token.")
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = load_env_file(path)
    existing[OAUTH_TOKEN_VAR] = token
    path.write_text("\n".join(f"{k}={v}" for k, v in existing.items()) + "\n")
    path.chmod(0o600)


def _augmented_path(base: str | None) -> str:
    parts = base.split(os.pathsep) if base else []
    for hint in _PATH_HINTS:
        if hint and hint not in parts:
            parts.append(hint)
    return os.pathsep.join(p for p in parts if p)


def resolve_claude_bin() -> str:
    found = shutil.which("claude", path=_augmented_path(os.environ.get("PATH")))
    if not found:
        raise RuntimeError("Could not find the `claude` binary. Install Claude Code or add it to PATH.")
    return found


def build_env(cfg: TaskConfig) -> tuple[dict[str, str], str | None]:
    """Build the subprocess environment and an optional fatal error message."""
    env = dict(os.environ)
    env.update(load_env_file())
    env.setdefault("HOME", str(Path.home()))
    env["PATH"] = _augmented_path(env.get("PATH"))

    if cfg.auth == "oauth":
        # The OAuth token sits below ANTHROPIC_API_KEY in precedence; drop the key so it is used.
        env.pop("ANTHROPIC_API_KEY", None)
        if not env.get(OAUTH_TOKEN_VAR):
            return env, (
                f"Missing {OAUTH_TOKEN_VAR}. Run `claude setup-token`, then store it with `crontui auth` "
                "(or the TUI: Ctrl+P → 'Set up Claude auth token')."
            )
    elif cfg.auth == "api_key":
        if not env.get("ANTHROPIC_API_KEY"):
            return env, "Missing ANTHROPIC_API_KEY in ~/.crontui/env (auth = api_key)."
    return env, None


# --------------------------------------------------------------------------- command building


def build_claude_argv(claude_bin: str, cfg: TaskConfig, prompt: str) -> list[str]:
    argv = [claude_bin, "-p", prompt, "--permission-mode", cfg.permission_mode]
    if cfg.auth == "api_key":
        argv.append("--bare")
    if cfg.model:
        argv += ["--model", cfg.model]
    if cfg.permission_mode == "dontAsk" and cfg.allowed_tools:
        argv += ["--allowedTools", ",".join(cfg.allowed_tools)]
    if cfg.disallowed_tools:
        argv += ["--disallowedTools", ",".join(cfg.disallowed_tools)]
    for d in cfg.add_dirs:
        argv += ["--add-dir", d]
    if cfg.max_turns is not None:
        argv += ["--max-turns", str(cfg.max_turns)]
    if cfg.max_budget_usd is not None:
        argv += ["--max-budget-usd", str(cfg.max_budget_usd)]
    if cfg.output_format and cfg.output_format != "text":
        argv += ["--output-format", cfg.output_format]
    argv += cfg.extra_args
    return argv


def runner_invocation() -> str:
    """Shell prefix that runs `crontui` from cron's minimal environment (before `exec <slug>`)."""
    exe = shutil.which("crontui")
    if exe:
        return shlex.quote(exe)
    src_dir = str(Path(__file__).resolve().parents[1])  # the dir containing the `crontui` package
    return f"PYTHONPATH={shlex.quote(src_dir)} {shlex.quote(sys.executable)} -m crontui"


def crontab_command(slug: str) -> str:
    log_path = LOGS_DIR / f"{slug}.log"
    return f"{runner_invocation()} exec {shlex.quote(slug)} >> {shlex.quote(str(log_path))} 2>&1"


def claude_slug(command: str) -> str | None:
    """Recover the task slug from a crontab command, or None if it is not a Claude task."""
    match = _EXEC_RE.search(command)
    return match.group(1) if match else None


# --------------------------------------------------------------------------- runner


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _notify(title: str, message: str) -> None:
    osa = shutil.which("osascript")
    if not osa:
        return
    safe_title = title.replace('"', "'")
    safe_msg = message.replace('"', "'")[:200]
    # a cosmetic notification must never break the task
    with contextlib.suppress(subprocess.SubprocessError, OSError):
        subprocess.run(
            [osa, "-e", f'display notification "{safe_msg}" with title "{safe_title}"'],
            capture_output=True,
            timeout=10,
        )


def run_claude_task(slug: str, *, on_line: Callable[[str], None] | None = None) -> int:
    """Run a Claude task to completion. Returns the process exit code (or a small non-zero on setup failure).

    With ``on_line`` set (TUI), output is delivered line-by-line and nothing is printed to stdout.
    Without it (cron / CLI), output is printed to stdout so the crontab redirect captures it.
    """
    to_stdout = on_line is None

    def emit(line: str) -> None:
        if to_stdout:
            print(line, flush=True)
        else:
            on_line(line)

    cfg, prompt = load_task(slug)
    started = time.time()
    emit(f"--- claude task '{slug}' @ {_now_iso()} ---")
    emit(f"    cwd={cfg.cwd}  mode={cfg.permission_mode}  model={cfg.model or 'default'}  auth={cfg.auth}")

    env, auth_error = build_env(cfg)
    if auth_error:
        emit(f"[error] {auth_error}")
        _notify(f"crontui: '{slug}' not run", auth_error)
        return 2

    if not Path(cfg.cwd).is_dir():
        message = f"working directory does not exist: {cfg.cwd}"
        emit(f"[error] {message}")
        _notify(f"crontui: '{slug}' failed", message)
        return 4

    try:
        claude_bin = resolve_claude_bin()
    except RuntimeError as exc:
        emit(f"[error] {exc}")
        _notify(f"crontui: '{slug}' failed", str(exc))
        return 3

    argv = build_claude_argv(claude_bin, cfg, prompt)
    try:
        proc = subprocess.run(
            argv, cwd=cfg.cwd, env=env, capture_output=True, text=True, timeout=cfg.timeout_sec
        )
    except subprocess.TimeoutExpired as exc:
        for line in (exc.stdout or "").splitlines():
            emit(line)
        duration = int(time.time() - started)
        emit(f"--- TIMEOUT after {duration}s (limit {cfg.timeout_sec}s) ---")
        _notify(f"crontui: '{slug}' timed out", f"after {duration}s")
        return 124

    for line in proc.stdout.splitlines():
        emit(line)
    for line in proc.stderr.splitlines():
        emit(f"[stderr] {line}")
    duration = int(time.time() - started)
    emit(f"--- exit {proc.returncode} in {duration}s ---")
    if proc.returncode != 0:
        _notify(f"crontui: '{slug}' failed", f"exit {proc.returncode} after {duration}s")
    return proc.returncode
