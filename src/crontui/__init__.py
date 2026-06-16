from __future__ import annotations

import os
import platform
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path
from threading import Thread

from croniter import croniter
from rich.markup import escape
from rich.text import Text
from textual import on
from textual.app import App, ComposeResult, SystemCommand
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.theme import Theme
from textual.widgets import (
    Button,
    Collapsible,
    DataTable,
    DirectoryTree,
    Footer,
    Input,
    Label,
    RichLog,
    Select,
    Static,
    TextArea,
)

from . import claude

CRON_FIELD_RE = re.compile(
    r"^[\d\*,/\-]+$|^(sun|mon|tue|wed|thu|fri|sat)$|^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)$",
    re.IGNORECASE,
)
DISABLED_PREFIX = "# disabled: "
DESCRIPTION_PREFIX = "# description: "
CRONTUI_DIR = Path.home() / ".crontui"
CRONTUI_LOGS = CRONTUI_DIR / "logs"
CRONTUI_THEME = Theme(
    name="crontui",
    primary="#00a4d6",
    secondary="#007a9e",
    accent="#00a4d6",
    warning="#e65100",
    error="#b71c1c",
    success="#1b5e20",
    background="#ffffff",
    surface="#ffffff",
    panel="#f5f5f5",
    foreground="#1a1a1a",
    dark=False,
)


def _relative_time(dt: datetime) -> str:
    secs = int((datetime.now() - dt).total_seconds())
    if secs < 0:
        return "just now"
    if secs < 60:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs // 60}m ago"
    if secs < 86400:
        h, m = divmod(secs, 3600)
        return f"{h}h {m // 60}m ago"
    days = secs // 86400
    if days == 1:
        return "yesterday"
    if days < 30:
        return f"{days}d ago"
    return dt.strftime("%Y-%m-%d")


def _slug(command: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "-", command.split()[0].split("/")[-1]).strip("-")[:40]


def _find_log_path(command: str) -> Path | None:
    match = re.search(r">>?\s*(\S+)", command)
    if match:
        p = Path(match.group(1)).expanduser()
        if p.exists():
            return p
    fallback = CRONTUI_LOGS / f"{_slug(command)}.log"
    return fallback if fallback.exists() else None


@dataclass
class CronJob:
    schedule: str
    command: str
    enabled: bool
    description: str = ""

    @property
    def next_run_display(self) -> str:
        if not self.enabled:
            return "—"
        try:
            dt = croniter(self.schedule, datetime.now()).get_next(datetime)
        except (ValueError, KeyError):
            return "—"
        secs = int((dt - datetime.now()).total_seconds())
        if secs < 60:
            return f"in {secs}s"
        if secs < 3600:
            return f"in {secs // 60}m"
        if secs < 86400:
            h, m = divmod(secs, 3600)
            return f"in {h}h {m // 60}m"
        return dt.strftime("%Y-%m-%d %H:%M")

    @property
    def last_run_display(self) -> str:
        lp = _find_log_path(self.command)
        if lp is None:
            return "—"
        return _relative_time(datetime.fromtimestamp(os.path.getmtime(lp)))


@dataclass
class CrontabManager:
    _raw_lines: list[str] = field(default_factory=list)

    def load(self) -> list[CronJob]:
        try:
            result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
            if result.returncode != 0:
                return []
            self._raw_lines = result.stdout.splitlines()
        except FileNotFoundError:
            return []
        return self._parse()

    def save(self, jobs: list[CronJob]) -> None:
        lines: list[str] = []
        for job in jobs:
            if job.description:
                lines.append(f"{DESCRIPTION_PREFIX}{job.description}")
            if job.enabled:
                lines.append(f"{job.schedule} {job.command}")
            else:
                lines.append(f"{DISABLED_PREFIX}{job.schedule} {job.command}")
        subprocess.run(["crontab", "-"], input="\n".join(lines) + "\n", text=True, check=True)

    def _parse(self) -> list[CronJob]:
        jobs: list[CronJob] = []
        pending_desc = ""
        for line in self._raw_lines:
            stripped = line.strip()
            if stripped.lower().startswith(DESCRIPTION_PREFIX.lower()):
                pending_desc = stripped[len(DESCRIPTION_PREFIX) :].strip()
                continue
            if stripped.startswith(DISABLED_PREFIX):
                schedule, command = self._split_cron(stripped[len(DISABLED_PREFIX) :])
                if schedule:
                    jobs.append(CronJob(schedule, command, False, pending_desc))
                    pending_desc = ""
                    continue
            schedule, command = self._split_cron(stripped)
            if schedule:
                jobs.append(CronJob(schedule, command, True, pending_desc))
                pending_desc = ""
        return jobs

    @staticmethod
    def _split_cron(line: str) -> tuple[str, str]:
        parts = line.split()
        if len(parts) < 6:
            return ("", "")
        if not all(CRON_FIELD_RE.match(p) for p in parts[:5]):
            return ("", "")
        schedule = " ".join(parts[:5])
        try:
            croniter(schedule)
        except (ValueError, KeyError):
            return ("", "")
        return (schedule, " ".join(parts[5:]))


class HeaderBar(Static):
    DEFAULT_CSS = """
    HeaderBar { dock: top; height: 1; background: #00a4d6; color: #ffffff; padding: 0 1; }
    """

    def __init__(self) -> None:
        super().__init__("")
        self._active_count = 0
        self._disabled_count = 0

    def update_counts(self, active: int, disabled_count: int) -> None:
        self._active_count = active
        self._disabled_count = disabled_count
        self._refresh_text()

    def on_mount(self) -> None:
        self.set_interval(1.0, self._refresh_text)

    def _refresh_text(self) -> None:
        if self.size.width < 10:
            return
        host = platform.node().split(".")[0]
        now = datetime.now().strftime("%H:%M:%S")
        total = self._active_count + self._disabled_count
        left = f"[bold]Jobs[/]    {total} total, {self._active_count} active, {self._disabled_count} disabled"
        right = f"[b]Ctrl+P[/] commands   {host}  {now}"
        left_plain = f"Jobs    {total} total, {self._active_count} active, {self._disabled_count} disabled"
        right_plain = f"Ctrl+P commands   {host}  {now}"
        pad = max(1, self.size.width - len(left_plain) - len(right_plain) - 2)
        self.update(f"{left}{' ' * pad}{right}")


class HelpModal(ModalScreen[None]):
    BINDINGS = [Binding("escape", "dismiss", "Close"), Binding("question_mark", "dismiss", "Close")]
    DEFAULT_CSS = """
    HelpModal { align: center middle; }
    #help-box {
        width: 60; height: auto; max-height: 80%;
        background: white; border: round #1a1a1a;
        padding: 1 2; color: #212121;
    }
    """

    def compose(self) -> ComposeResult:
        with Vertical(id="help-box"):
            yield Label("[bold]crontui[/] — command-palette driven\n")
            yield Label(
                "[bold]Ctrl+P[/]   Open the command palette — everything lives here:\n"
                "           New Claude task · New script job · Run now · Edit ·\n"
                "           View logs · Enable/Disable · Delete · Set up auth token\n"
                "           (type a job name to act on it directly)\n\n"
                "[bold]↑ ↓ / j k[/] Move selection      [bold]Enter[/]  Edit selected\n"
                "[bold]q[/]         Quit\n\n"
                "[dim]Tip: in iTerm2 you can map Cmd+P → send Ctrl+P.[/]"
            )


class ConfirmModal(ModalScreen[bool]):
    DEFAULT_CSS = """
    ConfirmModal { align: center middle; }
    #confirm-box { width: 60; height: auto; background: white; border: round #b71c1c; padding: 1 2; color: #212121; }
    #confirm-buttons { height: 3; margin-top: 1; align: center middle; }
    #confirm-buttons Button { margin: 0 1; }
    """

    def __init__(self, message: str) -> None:
        super().__init__()
        self.message = message

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-box"):
            yield Label(self.message)
            with Horizontal(id="confirm-buttons"):
                yield Button("[bold]Yes[/]", variant="error", id="yes")
                yield Button("No", variant="default", id="no")

    @on(Button.Pressed, "#yes")
    def on_yes(self) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#no")
    def on_no(self) -> None:
        self.dismiss(False)

    def key_y(self) -> None:
        self.dismiss(True)

    def key_n(self) -> None:
        self.dismiss(False)

    def key_escape(self) -> None:
        self.dismiss(False)


class JobFormModal(ModalScreen[dict | None]):
    DEFAULT_CSS = """
    JobFormModal { align: center middle; }
    #form-box { width: 70; height: auto; background: white; border: round #1a1a1a; padding: 1 2; color: #212121; }
    #form-box Label { margin-top: 1; color: #424242; }
    #form-box Input { margin-bottom: 0; }
    #form-buttons { height: 3; margin-top: 1; align: center middle; }
    #form-buttons Button { margin: 0 1; }
    #form-error { color: #b71c1c; margin-top: 1; height: 1; }
    """

    def __init__(self, title: str = "Add Job", schedule: str = "", command: str = "", description: str = "") -> None:
        super().__init__()
        self.title_text = title
        self.initial_schedule = schedule
        self.initial_command = command
        self.initial_description = description

    def compose(self) -> ComposeResult:
        with Vertical(id="form-box"):
            yield Label(f"[bold]{self.title_text}[/]")
            yield Label("Schedule (cron expression):")
            yield Input(value=self.initial_schedule, placeholder="* * * * *", id="schedule")
            yield Label("Command:")
            yield Input(value=self.initial_command, placeholder="/path/to/script.sh", id="command")
            yield Label("Description (optional):")
            yield Input(value=self.initial_description, placeholder="My daily backup", id="description")
            yield Label("", id="form-error")
            with Horizontal(id="form-buttons"):
                yield Button("Save", variant="success", id="save")
                yield Button("Cancel", variant="default", id="cancel")

    @on(Button.Pressed, "#save")
    def on_save(self) -> None:
        self._try_save()

    @on(Button.Pressed, "#cancel")
    def on_cancel(self) -> None:
        self.dismiss(None)

    def key_escape(self) -> None:
        self.dismiss(None)

    def key_enter(self) -> None:
        self._try_save()

    def _try_save(self) -> None:
        schedule = self.query_one("#schedule", Input).value.strip()
        command = self.query_one("#command", Input).value.strip()
        description = self.query_one("#description", Input).value.strip()
        error_label = self.query_one("#form-error", Label)
        if not schedule:
            error_label.update("[bold red]Schedule is required[/]")
            return
        if not command:
            error_label.update("[bold red]Command is required[/]")
            return
        try:
            croniter(schedule)
        except (ValueError, KeyError):
            error_label.update("[bold red]Invalid cron expression[/]")
            return
        self.dismiss({"schedule": schedule, "command": command, "description": description})


class LogViewerModal(ModalScreen[None]):
    BINDINGS = [Binding("escape", "dismiss", "Close"), Binding("q", "dismiss", "Close")]
    DEFAULT_CSS = """
    LogViewerModal { align: center middle; }
    #log-box { width: 90%; height: 80%; background: white; border: round #1a1a1a; padding: 1 2; color: #212121; }
    #log-title { dock: top; height: 1; margin-bottom: 1; color: #424242; }
    """

    def __init__(self, job: CronJob) -> None:
        super().__init__()
        self.job = job

    def compose(self) -> ComposeResult:
        with Vertical(id="log-box"):
            yield Label(f"[bold]Logs[/] — [#757575]{self.job.command[:60]}[/]  (q/esc to close)", id="log-title")
            yield RichLog(id="log-output", wrap=True)

    def on_mount(self) -> None:
        log = self.query_one("#log-output", RichLog)
        lp = _find_log_path(self.job.command)
        if lp and lp.exists():
            try:
                lines = lp.read_text().splitlines()
                for line in lines[-100:] if len(lines) > 100 else lines:
                    log.write(line)
            except Exception as e:
                log.write(f"[red]Error reading log: {e}[/red]")
        else:
            log.write("[dim]No log file found. Job has not run yet.[/dim]")
            log.write(f"\n[dim]Command: {self.job.command}[/dim]")
            log.write(f"\n[dim]Logs are stored in: {CRONTUI_LOGS}/[/dim]")


def _int_or(text: str, default: int | None) -> int | None:
    text = text.strip()
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        return default


def _float_or(text: str, default: float | None) -> float | None:
    text = text.strip()
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


class DirectoryPickerScreen(Screen[str | None]):
    """Browse the filesystem and return a chosen directory."""

    BINDINGS = [Binding("escape", "cancel", "Cancel")]
    CSS = """
    DirectoryPickerScreen { background: #ffffff; color: #1a1a1a; align: center middle; }
    #picker-box { width: 80%; height: 80%; border: round #1a1a1a; padding: 1 2; background: #ffffff; }
    #tree { height: 1fr; border: round #c0c0c0; margin: 1 0; }
    #picker-current { color: #007a9e; height: 1; }
    #picker-buttons { height: 3; margin-top: 1; align: center middle; }
    #picker-buttons Button { margin: 0 1; }
    """

    def __init__(self, start: str) -> None:
        super().__init__()
        start_path = Path(start).expanduser()
        self.start = str(start_path) if start_path.is_dir() else str(Path.home())
        self.selected = self.start

    def compose(self) -> ComposeResult:
        with Vertical(id="picker-box"):
            yield Label("[bold]Pick a working directory[/]  [dim](click a folder to choose it)[/]")
            yield DirectoryTree(self.start, id="tree")
            yield Static(self.selected, id="picker-current")
            with Horizontal(id="picker-buttons"):
                yield Button("Select", variant="success", id="select")
                yield Button("Cancel", id="cancel")

    @on(DirectoryTree.DirectorySelected)
    def _on_dir_selected(self, event: DirectoryTree.DirectorySelected) -> None:
        self.selected = str(event.path)
        self.query_one("#picker-current", Static).update(self.selected)

    @on(Button.Pressed, "#select")
    def _on_select(self) -> None:
        self.dismiss(self.selected)

    @on(Button.Pressed, "#cancel")
    def _on_cancel(self) -> None:
        self.dismiss(None)

    def action_cancel(self) -> None:
        self.dismiss(None)


class AuthSetupScreen(Screen[bool]):
    """Capture and store the long-lived OAuth token used by unattended runs."""

    BINDINGS = [Binding("escape", "cancel", "Cancel")]
    CSS = """
    AuthSetupScreen { background: #ffffff; color: #1a1a1a; align: center middle; }
    #auth-box { width: 80; height: auto; border: round #1a1a1a; padding: 1 2; background: #ffffff; }
    #auth-box Input { margin-top: 1; }
    #auth-error { color: #b71c1c; height: 1; margin-top: 1; }
    #auth-buttons { height: 3; margin-top: 1; align: center middle; }
    #auth-buttons Button { margin: 0 1; }
    """

    def compose(self) -> ComposeResult:
        with Vertical(id="auth-box"):
            yield Label("[bold]Set up Claude auth token[/]\n")
            yield Label(
                "Unattended cron runs authenticate with a long-lived OAuth token.\n\n"
                "1. In a terminal run:  [b]claude setup-token[/]\n"
                "2. Paste the token below (hidden), then Save.\n"
                f"It is stored in {claude.ENV_FILE} (chmod 600)."
            )
            yield Input(password=True, placeholder="CLAUDE_CODE_OAUTH_TOKEN", id="token")
            yield Static("", id="auth-error")
            with Horizontal(id="auth-buttons"):
                yield Button("Save", variant="success", id="save")
                yield Button("Cancel", id="cancel")

    @on(Button.Pressed, "#save")
    def _on_save(self) -> None:
        token = self.query_one("#token", Input).value.strip()
        if not token:
            self.query_one("#auth-error", Static).update("[b red]Token is required[/]")
            return
        try:
            claude.write_oauth_token(token)
        except ValueError as exc:
            self.query_one("#auth-error", Static).update(f"[b red]{escape(str(exc))}[/]")
            return
        self.dismiss(True)

    @on(Button.Pressed, "#cancel")
    def _on_cancel(self) -> None:
        self.dismiss(False)

    def action_cancel(self) -> None:
        self.dismiss(False)


class ClaudeTaskScreen(Screen[dict | None]):
    """Full-screen editor for composing or editing a Claude task."""

    BINDINGS = [Binding("escape", "cancel", "Cancel"), Binding("ctrl+s", "save", "Save")]
    CSS = """
    ClaudeTaskScreen { background: #ffffff; color: #1a1a1a; align: center top; }
    #form { width: 90%; max-width: 110; height: 1fr; padding: 1 2; }
    #form > Label { margin-top: 1; color: #424242; text-style: bold; }
    #form Input, #form Select { width: 1fr; }
    #cwd-row { height: 3; }
    #cwd-row #browse { width: auto; margin-left: 1; }
    #prompt { height: 12; border: round #c0c0c0; }
    #selects { height: auto; }
    #selects > Vertical { width: 1fr; margin-right: 1; }
    #sched-preview { color: #007a9e; height: 1; }
    #form-error { color: #b71c1c; height: 1; margin-top: 1; }
    #buttons { height: 3; margin-top: 1; align: center middle; }
    #buttons Button { margin: 0 1; }
    """

    def __init__(
        self,
        *,
        title: str = "New Claude Task",
        slug: str | None = None,
        schedule: str = "",
        cfg: claude.TaskConfig | None = None,
        prompt: str = "",
    ) -> None:
        super().__init__()
        self.title_text = title
        self.slug = slug
        self.initial_schedule = schedule
        self.cfg = cfg or claude.TaskConfig()
        self.initial_prompt = prompt

    def compose(self) -> ComposeResult:
        cfg = self.cfg
        model_options = [
            ("default (your claude default)", ""),
            ("opus", "opus"),
            ("sonnet", "sonnet"),
            ("haiku", "haiku"),
        ]
        with VerticalScroll(id="form"):
            yield Label(f"[bold]{escape(self.title_text)}[/]  [dim](Ctrl+S save · Esc cancel)[/]")
            yield Label("Name")
            yield Input(value=(self.slug or cfg.description), placeholder="ks-structure", id="name")
            yield Label("Description (optional)")
            yield Input(value=cfg.description, id="description")
            yield Label("Schedule (cron)")
            yield Input(value=self.initial_schedule, placeholder="0 10 * * *", id="schedule")
            yield Static("", id="sched-preview")
            yield Label("Working directory")
            with Horizontal(id="cwd-row"):
                yield Input(value=cfg.cwd, placeholder=str(Path.home()), id="cwd")
                yield Button("Browse…", id="browse")
            yield Label("Prompt")
            yield TextArea(
                self.initial_prompt, id="prompt", soft_wrap=True, placeholder="What should Claude do every run?"
            )
            with Horizontal(id="selects"):
                with Vertical():
                    yield Label("Model")
                    yield Select(model_options, value=cfg.model, allow_blank=False, id="model")
                with Vertical():
                    yield Label("Permission mode")
                    yield Select(
                        [(m, m) for m in claude.PERMISSION_MODES],
                        value=cfg.permission_mode,
                        allow_blank=False,
                        id="permission",
                    )
            with Collapsible(title="Advanced", collapsed=True):
                yield Label("Auth")
                yield Select([(m, m) for m in claude.AUTH_MODES], value=cfg.auth, allow_blank=False, id="auth")
                yield Label("Timeout (seconds)")
                yield Input(value=str(cfg.timeout_sec), id="timeout")
                yield Label("Max turns (blank = unlimited)")
                yield Input(value=("" if cfg.max_turns is None else str(cfg.max_turns)), id="max_turns")
                yield Label("Max budget USD (blank = none)")
                yield Input(value=("" if cfg.max_budget_usd is None else str(cfg.max_budget_usd)), id="max_budget")
                yield Label("Allowed tools, comma-separated (used when permission mode is dontAsk)")
                yield Input(
                    value=",".join(cfg.allowed_tools),
                    placeholder="Read,Edit,Bash(./build-index.sh)",
                    id="allowed",
                )
                yield Label("Extra claude args (space-separated)")
                yield Input(value=" ".join(cfg.extra_args), id="extra")
            yield Static("", id="form-error")
            with Horizontal(id="buttons"):
                yield Button("Save", variant="success", id="save")
                yield Button("Cancel", id="cancel")

    def on_mount(self) -> None:
        self._update_preview(self.initial_schedule)

    @on(Input.Changed, "#schedule")
    def _on_schedule_changed(self, event: Input.Changed) -> None:
        self._update_preview(event.value)

    def _update_preview(self, expr: str) -> None:
        preview = self.query_one("#sched-preview", Static)
        expr = expr.strip()
        if not expr:
            preview.update("")
            return
        try:
            it = croniter(expr, datetime.now())
            runs = [it.get_next(datetime).strftime("%a %d %b %H:%M") for _ in range(3)]
        except (ValueError, KeyError):
            preview.update("[#b71c1c]invalid cron expression[/]")
            return
        preview.update("next: " + "   •   ".join(runs))

    @on(Button.Pressed, "#browse")
    def _on_browse(self) -> None:
        start = self.query_one("#cwd", Input).value.strip() or str(Path.home())

        def apply(path: str | None) -> None:
            if path:
                self.query_one("#cwd", Input).value = path

        self.app.push_screen(DirectoryPickerScreen(start), apply)

    @on(Button.Pressed, "#save")
    def _on_save(self) -> None:
        self.action_save()

    @on(Button.Pressed, "#cancel")
    def _on_cancel(self) -> None:
        self.dismiss(None)

    def action_cancel(self) -> None:
        self.dismiss(None)

    def action_save(self) -> None:
        name = self.query_one("#name", Input).value.strip()
        schedule = self.query_one("#schedule", Input).value.strip()
        prompt = self.query_one("#prompt", TextArea).text.strip()
        cwd = self.query_one("#cwd", Input).value.strip() or str(Path.home())
        error = self.query_one("#form-error", Static)
        if not name:
            error.update("[b red]Name is required[/]")
            return
        if not prompt:
            error.update("[b red]Prompt is required[/]")
            return
        try:
            croniter(schedule)
        except (ValueError, KeyError):
            error.update("[b red]Invalid cron expression[/]")
            return
        cwd_path = Path(cwd).expanduser()
        if not cwd_path.is_dir():
            error.update("[b red]Working directory does not exist[/]")
            return
        self.dismiss(
            {
                "name": name,
                "slug": self.slug,
                "schedule": schedule,
                "prompt": prompt,
                "description": self.query_one("#description", Input).value.strip(),
                "cwd": str(cwd_path),
                "model": str(self.query_one("#model", Select).value),
                "permission_mode": str(self.query_one("#permission", Select).value),
                "auth": str(self.query_one("#auth", Select).value),
                "timeout_sec": _int_or(self.query_one("#timeout", Input).value, claude.DEFAULT_TIMEOUT_SEC),
                "max_turns": _int_or(self.query_one("#max_turns", Input).value, None),
                "max_budget_usd": _float_or(self.query_one("#max_budget", Input).value, None),
                "allowed_tools": [t.strip() for t in self.query_one("#allowed", Input).value.split(",") if t.strip()],
                "extra_args": self.query_one("#extra", Input).value.split(),
            }
        )


class CrontuiApp(App):
    TITLE = "crontui"
    COMMAND_PALETTE_BINDING = "ctrl+p"

    CSS = """
    Screen { background: #ffffff; color: #1a1a1a; }
    #main { height: 1fr; }
    #jobs-table { width: 3fr; height: 1fr; background: #ffffff; color: #1a1a1a; overflow-x: hidden; }
    DataTable > .datatable--header { background: #f5f5f5; color: #333333; text-style: bold; }
    DataTable > .datatable--cursor { background: #e0f2f9; color: #1a1a1a; }
    DataTable > .datatable--hover { background: #f0f0f0; color: #1a1a1a; }
    DataTable > .datatable--even-row { background: #ffffff; }
    DataTable > .datatable--odd-row { background: #fafafa; }
    #detail { width: 2fr; height: 1fr; border-left: solid #d0d0d0; padding: 0 1; background: #fafafa; }
    #detail-body { color: #1a1a1a; }
    ModalScreen { background: rgba(0, 0, 0, 0.4); }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("ctrl+c", "quit", "Quit", show=False),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("question_mark", "help", "Help", show=False),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.register_theme(CRONTUI_THEME)
        self.theme = "crontui"
        self.manager = CrontabManager()
        self.jobs: list[CronJob] = []

    # ---- command palette -------------------------------------------------

    def get_system_commands(self, screen: Screen):
        yield from super().get_system_commands(screen)
        yield SystemCommand("New Claude task", "Create a periodic Claude (AI) task", self.action_add_claude)
        yield SystemCommand("New script job", "Create a shell-script cron job", self.action_add)
        yield SystemCommand(
            "Set up Claude auth token", "Store CLAUDE_CODE_OAUTH_TOKEN for unattended runs", self.action_auth
        )
        yield SystemCommand("Refresh", "Reload jobs from crontab", self.action_refresh)
        yield SystemCommand("Help", "Show crontui help", self.action_help)
        idx = self._get_selected()
        if idx is not None:
            job = self.jobs[idx]
            label = self._job_label(job)
            yield SystemCommand("Run now", f"Run '{label}' immediately", self.action_run_now)
            yield SystemCommand("Edit", f"Edit '{label}'", self.action_edit_selected)
            yield SystemCommand("View logs", f"Logs for '{label}'", self.action_logs)
            toggle = "Disable" if job.enabled else "Enable"
            yield SystemCommand(toggle, f"{toggle} '{label}'", self.action_toggle)
            yield SystemCommand("Delete", f"Delete '{label}'", self.action_delete)
        for i, job in enumerate(self.jobs):
            label = self._job_label(job)
            yield SystemCommand(
                f"Run: {label}", "Run this job now", partial(self._act_on, i, self.action_run_now), discover=False
            )
            yield SystemCommand(
                f"Edit: {label}", "Edit this job", partial(self._act_on, i, self.action_edit_selected), discover=False
            )
            yield SystemCommand(
                f"Logs: {label}", "View this job's logs", partial(self._act_on, i, self.action_logs), discover=False
            )

    def _act_on(self, index: int, action) -> None:
        table = self.query_one("#jobs-table", DataTable)
        if 0 <= index < table.row_count:
            table.move_cursor(row=index)
        action()

    def _job_label(self, job: CronJob) -> str:
        slug = claude.claude_slug(job.command)
        if slug:
            return job.description or slug
        if job.description:
            return job.description
        parts = job.command.split()
        return parts[0].split("/")[-1] if parts else job.command

    # ---- layout ----------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield HeaderBar()
        with Horizontal(id="main"):
            yield DataTable(id="jobs-table")
            with VerticalScroll(id="detail"):
                yield Static("[dim]No job selected[/]", id="detail-body")
        yield Footer()

    def _compute_column_widths(self) -> list[int]:
        table = self.query_one("#jobs-table", DataTable)
        total = (table.size.width or (self.size.width * 3 // 5)) - 2
        status_w = 6
        sched_w = max(9, max((len(j.schedule) for j in self.jobs), default=9))
        next_w = max(8, max((len(j.next_run_display) for j in self.jobs), default=8))
        last_w = max(8, max((len(j.last_run_display) for j in self.jobs), default=8))
        job_w = max(12, total - status_w - sched_w - next_w - last_w)
        return [status_w, sched_w, next_w, last_w, job_w]

    def _apply_column_widths(self) -> None:
        table = self.query_one("#jobs-table", DataTable)
        for col, w in zip(table.ordered_columns, self._compute_column_widths(), strict=False):
            col.width = w

    def on_mount(self) -> None:
        CRONTUI_LOGS.mkdir(parents=True, exist_ok=True)
        table = self.query_one("#jobs-table", DataTable)
        table.zebra_stripes = True
        table.cursor_type = "row"
        for name in ["", "Schedule", "Next", "Last", "Job"]:
            table.add_column(name, width=10)
        self._load_jobs()
        self._apply_column_widths()
        self.set_interval(30.0, self._refresh_next_run)

    def on_resize(self) -> None:
        self._apply_column_widths()
        self._refresh_table()

    def _load_jobs(self) -> None:
        self.jobs = self.manager.load()
        self._refresh_table()

    @staticmethod
    def _trunc(text: str, width: int) -> str:
        return text if len(text) <= width else text[: width - 1] + "…"

    def _job_text(self, job: CronJob, width: int, enabled: bool) -> Text:
        slug = claude.claude_slug(job.command)
        if slug:
            try:
                _, prompt = claude.load_task(slug)
                label = f"✦ {slug}: {claude.prompt_preview(prompt, width)}"
            except FileNotFoundError:
                label = f"✦ {slug}"
            style = "#00a4d6" if enabled else "#bbbbbb"
        else:
            label = job.description or job.command
            style = "#1a1a1a" if enabled else "#bbbbbb"
        return Text(self._trunc(label, width), style=style)

    def _refresh_table(self) -> None:
        table = self.query_one("#jobs-table", DataTable)
        saved = table.cursor_row
        table.clear()
        widths = self._compute_column_widths()
        job_w = widths[4]
        active = sum(1 for j in self.jobs if j.enabled)
        disabled = sum(1 for j in self.jobs if not j.enabled)
        self.query_one(HeaderBar).update_counts(active, disabled)
        for job in self.jobs:
            is_claude = claude.claude_slug(job.command) is not None
            if job.enabled:
                marker = "✦ ON" if is_claude else "● ON"
                table.add_row(
                    Text(marker, style="bold #00a4d6" if is_claude else "bold #1b5e20"),
                    Text(job.schedule, style="bold #1a1a1a"),
                    Text(job.next_run_display, style="#007a9e"),
                    Text(job.last_run_display, style="#666666"),
                    self._job_text(job, job_w, True),
                )
            else:
                marker = "✦ OFF" if is_claude else "○ OFF"
                table.add_row(
                    Text(marker, style="#bbbbbb"),
                    Text(job.schedule, style="#bbbbbb"),
                    Text("—", style="#bbbbbb"),
                    Text("—", style="#bbbbbb"),
                    self._job_text(job, job_w, False),
                )
        self._apply_column_widths()
        if table.row_count:
            target = 0 if saved is None else min(saved, table.row_count - 1)
            table.move_cursor(row=target)
        self._update_detail(self._get_selected())

    def _refresh_next_run(self) -> None:
        table = self.query_one("#jobs-table", DataTable)
        for i, job in enumerate(self.jobs):
            if job.enabled and i < table.row_count:
                table.update_cell_at((i, 2), Text(job.next_run_display, style="#007a9e"))

    def _get_selected(self) -> int | None:
        table = self.query_one("#jobs-table", DataTable)
        if table.row_count == 0:
            return None
        row = table.cursor_row
        return row if 0 <= row < len(self.jobs) else None

    def _update_detail(self, idx: int | None) -> None:
        body = self.query_one("#detail-body", Static)
        if idx is None or not (0 <= idx < len(self.jobs)):
            body.update("[dim]No job selected[/]")
            return
        job = self.jobs[idx]
        state = "[#1b5e20]● enabled[/]" if job.enabled else "[#999999]○ disabled[/]"
        lines: list[str] = []
        slug = claude.claude_slug(job.command)
        if slug:
            lines.append("[b #00a4d6]✦ Claude task[/]   " + state)
            lines.append("")
            try:
                cfg, prompt = claude.load_task(slug)
                lines.append(f"[b]slug[/]   {escape(slug)}")
                lines.append(f"[b]cwd[/]    {escape(cfg.cwd)}")
                lines.append(f"[b]model[/]  {escape(cfg.model or 'default')}")
                lines.append(f"[b]mode[/]   {escape(cfg.permission_mode)}    [b]auth[/] {escape(cfg.auth)}")
                limits = [f"timeout {cfg.timeout_sec}s"]
                if cfg.max_turns is not None:
                    limits.append(f"max-turns {cfg.max_turns}")
                if cfg.max_budget_usd is not None:
                    limits.append(f"budget ${cfg.max_budget_usd}")
                lines.append("[b]limits[/] " + escape(", ".join(limits)))
                lines.append("")
                lines.append("[b]Prompt[/]")
                lines.append("[#333333]" + escape(prompt.strip()[:600]) + "[/]")
            except FileNotFoundError:
                lines.append("[#b71c1c]task files are missing[/]")
        else:
            lines.append("[b #007a9e]▷ Script job[/]   " + state)
            lines.append("")
            lines.append("[b]command[/]")
            lines.append("[#333333]" + escape(job.command) + "[/]")
        lines.append("")
        lines.append(f"[b]schedule[/] {escape(job.schedule)}")
        lines.append(f"[b]next[/] {escape(job.next_run_display)}    [b]last[/] {escape(job.last_run_display)}")
        log_path = _find_log_path(job.command)
        if log_path and log_path.exists():
            try:
                tail = log_path.read_text().splitlines()[-8:]
            except OSError:
                tail = []
            if tail:
                lines.append("")
                lines.append("[b]Recent log[/]")
                lines.extend("[#888888]" + escape(t[:90]) + "[/]" for t in tail)
        body.update("\n".join(lines))

    @on(DataTable.RowHighlighted)
    def _on_row_highlighted(self) -> None:
        self._update_detail(self._get_selected())

    @on(DataTable.RowSelected)
    def _on_row_selected(self) -> None:
        self.action_edit_selected()

    def action_cursor_down(self) -> None:
        self.query_one("#jobs-table", DataTable).action_cursor_down()

    def action_cursor_up(self) -> None:
        self.query_one("#jobs-table", DataTable).action_cursor_up()

    def action_help(self) -> None:
        self.push_screen(HelpModal())

    def action_refresh(self) -> None:
        self._load_jobs()
        self.notify("Refreshed", timeout=2)

    def action_auth(self) -> None:
        def on_result(ok: bool | None) -> None:
            if ok:
                self.notify("Auth token saved", timeout=3)

        self.push_screen(AuthSetupScreen(), on_result)

    def action_toggle(self) -> None:
        if (idx := self._get_selected()) is None:
            return
        self.jobs[idx].enabled = not self.jobs[idx].enabled
        self.manager.save(self.jobs)
        self.notify(f"Job {'enabled' if self.jobs[idx].enabled else 'disabled'}", timeout=2)
        self._refresh_table()

    def action_delete(self) -> None:
        if (idx := self._get_selected()) is None:
            return
        label = self._job_label(self.jobs[idx])

        def on_confirm(confirmed: bool | None) -> None:
            if confirmed:
                self.jobs.pop(idx)
                self.manager.save(self.jobs)
                self.notify("Deleted", timeout=2)
                self._refresh_table()

        self.push_screen(ConfirmModal(f"Delete job?\n\n[bold]{escape(label)}[/]"), on_confirm)

    def action_add(self) -> None:
        def on_result(result: dict | None) -> None:
            if result:
                cmd = _ensure_log_redirect(result["command"])
                self.jobs.append(CronJob(result["schedule"], cmd, True, result["description"]))
                self.manager.save(self.jobs)
                self.notify("Added", timeout=2)
                self._refresh_table()

        self.push_screen(JobFormModal(title="Add Job"), on_result)

    def action_add_claude(self) -> None:
        def on_result(result: dict | None) -> None:
            if not result:
                return
            slug = claude.unique_slug(result["name"])
            self._write_claude_task(slug, result)
            description = result["description"] or claude.prompt_preview(result["prompt"], 60)
            self.jobs.append(CronJob(result["schedule"], claude.crontab_command(slug), True, description))
            self.manager.save(self.jobs)
            self.notify(f"Created Claude task '{slug}'", timeout=3)
            self._refresh_table()

        self.push_screen(ClaudeTaskScreen(), on_result)

    def action_edit_selected(self) -> None:
        if (idx := self._get_selected()) is None:
            return
        job = self.jobs[idx]
        slug = claude.claude_slug(job.command)
        if slug:
            self._edit_claude(idx, slug)
        else:
            self._edit_script(idx, job)

    def _edit_claude(self, idx: int, slug: str) -> None:
        try:
            cfg, prompt = claude.load_task(slug)
        except FileNotFoundError:
            self.notify("Task files missing — cannot edit", severity="error", timeout=4)
            return
        job = self.jobs[idx]

        def on_result(result: dict | None) -> None:
            if not result:
                return
            self._write_claude_task(slug, result, created_at=cfg.created_at)
            job.schedule = result["schedule"]
            job.command = claude.crontab_command(slug)
            job.description = result["description"] or claude.prompt_preview(result["prompt"], 60)
            self.manager.save(self.jobs)
            self.notify(f"Updated Claude task '{slug}'", timeout=3)
            self._refresh_table()

        self.push_screen(
            ClaudeTaskScreen(title=f"Edit: {slug}", slug=slug, schedule=job.schedule, cfg=cfg, prompt=prompt),
            on_result,
        )

    def _edit_script(self, idx: int, job: CronJob) -> None:
        def on_result(result: dict | None) -> None:
            if not result:
                return
            job.schedule = result["schedule"]
            job.command = result["command"]
            job.description = result["description"]
            self.manager.save(self.jobs)
            self.notify("Updated", timeout=2)
            self._refresh_table()

        self.push_screen(
            JobFormModal(title="Edit Job", schedule=job.schedule, command=job.command, description=job.description),
            on_result,
        )

    def _write_claude_task(self, slug: str, result: dict, created_at: str = "") -> None:
        cfg = claude.TaskConfig(
            description=result["description"],
            cwd=result["cwd"],
            model=result["model"],
            permission_mode=result["permission_mode"],
            auth=result["auth"],
            allowed_tools=result["allowed_tools"],
            max_turns=result["max_turns"],
            max_budget_usd=result["max_budget_usd"],
            timeout_sec=result["timeout_sec"],
            extra_args=result["extra_args"],
            created_at=created_at,
        )
        claude.save_task(slug, cfg, result["prompt"])

    def action_run_now(self) -> None:
        if (idx := self._get_selected()) is None:
            return
        job = self.jobs[idx]
        slug = claude.claude_slug(job.command)
        if slug:
            self._run_claude_task(slug)
        else:
            self._run_script(job)

    def _run_claude_task(self, slug: str) -> None:
        self.notify(f"Running Claude task '{slug}'…", timeout=3)
        log_path = CRONTUI_LOGS / f"{slug}.log"

        def worker() -> None:
            try:
                with open(log_path, "a") as f:

                    def on_line(line: str) -> None:
                        f.write(line + "\n")
                        f.flush()

                    code = claude.run_claude_task(slug, on_line=on_line)
            except Exception as e:
                self.call_from_thread(self.notify, f"Error: {e!s:.60}", severity="error", timeout=6)
                return
            if code == 0:
                self.call_from_thread(self.notify, f"Task '{slug}' completed", timeout=4)
            else:
                self.call_from_thread(self.notify, f"Task '{slug}' exited {code}", severity="error", timeout=6)
            self.call_from_thread(self._refresh_table)

        Thread(target=worker, daemon=True).start()

    def _run_script(self, job: CronJob) -> None:
        cmd = re.sub(r"\s*>>?\s*\S+\s*2>&1\s*$", "", job.command)
        self.notify(f"Running: {cmd[:40]}…", timeout=2)

        def worker() -> None:
            try:
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
                log_path = CRONTUI_LOGS / f"{_slug(job.command)}.log"
                with open(log_path, "a") as f:
                    f.write(f"\n--- manual run {datetime.now().isoformat()} ---\n")
                    if result.stdout:
                        f.write(result.stdout)
                    if result.stderr:
                        f.write(result.stderr)
                if result.returncode == 0:
                    self.call_from_thread(self.notify, "Job completed", timeout=3)
                else:
                    self.call_from_thread(
                        self.notify, f"Job failed (exit {result.returncode})", severity="error", timeout=5
                    )
            except subprocess.TimeoutExpired:
                self.call_from_thread(self.notify, "Job timed out (5m)", severity="error", timeout=5)
            except Exception as e:
                self.call_from_thread(self.notify, f"Error: {e!s:.50}", severity="error", timeout=5)
            self.call_from_thread(self._refresh_table)

        Thread(target=worker, daemon=True).start()

    def action_logs(self) -> None:
        if (idx := self._get_selected()) is None:
            return
        self.push_screen(LogViewerModal(self.jobs[idx]))


def _ensure_log_redirect(command: str) -> str:
    if re.search(r">>?\s*\S+", command):
        return command
    log_file = CRONTUI_LOGS / f"{_slug(command)}.log"
    return f"{command} >> {log_file} 2>&1"


def _cli_list(args: object) -> None:
    manager = CrontabManager()
    jobs = manager.load()
    if not jobs:
        print("No cron jobs found.")
        return
    for i, job in enumerate(jobs):
        status = "ON " if job.enabled else "OFF"
        desc = f"  # {job.description}" if job.description else ""
        print(f"{i}  {status}  {job.schedule}  {_display_command(job.command)}{desc}")


def _display_command(command: str) -> str:
    """Friendly command for Claude tasks (✦ slug: prompt), raw command otherwise."""
    slug = claude.claude_slug(command)
    if slug is None:
        return command
    try:
        _, prompt = claude.load_task(slug)
        return f"✦ {slug}: {claude.prompt_preview(prompt, 50)}"
    except FileNotFoundError:
        return f"✦ {slug} (task files missing)"


def _cli_add_claude(args: object) -> None:
    CRONTUI_LOGS.mkdir(parents=True, exist_ok=True)
    schedule = args.schedule  # type: ignore[attr-defined]
    try:
        croniter(schedule)
    except (ValueError, KeyError):
        print(f"Invalid cron expression: {schedule}")
        raise SystemExit(1) from None
    prompt_file = args.prompt_file  # type: ignore[attr-defined]
    prompt = Path(prompt_file).expanduser().read_text() if prompt_file else args.prompt  # type: ignore[attr-defined]
    name = args.name or args.description or claude.prompt_preview(prompt, 40)  # type: ignore[attr-defined]
    slug = claude.unique_slug(name)
    cfg = claude.TaskConfig(
        description=args.description or "",  # type: ignore[attr-defined]
        cwd=str(Path(args.cwd).expanduser()) if args.cwd else str(Path.home()),  # type: ignore[attr-defined]
        model=args.model or "",  # type: ignore[attr-defined]
        permission_mode=args.permission_mode,  # type: ignore[attr-defined]
        allowed_tools=args.allow or [],  # type: ignore[attr-defined]
        max_turns=args.max_turns,  # type: ignore[attr-defined]
        max_budget_usd=args.max_budget_usd,  # type: ignore[attr-defined]
        timeout_sec=args.timeout,  # type: ignore[attr-defined]
        auth=args.auth,  # type: ignore[attr-defined]
    )
    claude.save_task(slug, cfg, prompt)
    manager = CrontabManager()
    jobs = manager.load()
    description = cfg.description or claude.prompt_preview(prompt, 60)
    jobs.append(CronJob(schedule, claude.crontab_command(slug), True, description))
    manager.save(jobs)
    cfg_path, prompt_path = claude.task_paths(slug)
    print(f"Added Claude task '{slug}': {schedule}")
    print(f"  prompt: {claude.prompt_preview(prompt, 60)}")
    print(f"  config: {cfg_path}")
    print(f"  prompt file: {prompt_path}")


def _cli_auth(args: object) -> None:
    token = args.token  # type: ignore[attr-defined]
    if not token:
        print("Paste your Claude OAuth token (from `claude setup-token`), then press Ctrl-D:")
        token = sys.stdin.read()
    try:
        claude.write_oauth_token(token)
    except ValueError as exc:
        print(f"Error: {exc}")
        raise SystemExit(1) from None
    print(f"Stored {claude.OAUTH_TOKEN_VAR} in {claude.ENV_FILE} (chmod 600).")


def _cli_add(args: object) -> None:
    CRONTUI_LOGS.mkdir(parents=True, exist_ok=True)
    manager = CrontabManager()
    jobs = manager.load()
    schedule = args.schedule  # type: ignore[attr-defined]
    command = args.command  # type: ignore[attr-defined]
    description = args.description or ""  # type: ignore[attr-defined]
    try:
        croniter(schedule)
    except (ValueError, KeyError):
        print(f"Invalid cron expression: {schedule}")
        raise SystemExit(1) from None
    cmd = _ensure_log_redirect(command)
    jobs.append(CronJob(schedule, cmd, True, description))
    manager.save(jobs)
    print(f"Added job {len(jobs) - 1}: {schedule} {cmd}")


def _cli_remove(args: object) -> None:
    manager = CrontabManager()
    jobs = manager.load()
    idx = args.index  # type: ignore[attr-defined]
    if idx < 0 or idx >= len(jobs):
        print(f"Invalid index {idx}. Use 'crontui list' to see jobs.")
        raise SystemExit(1)
    removed = jobs.pop(idx)
    manager.save(jobs)
    print(f"Removed job {idx}: {removed.schedule} {removed.command}")


def _cli_enable(args: object) -> None:
    manager = CrontabManager()
    jobs = manager.load()
    idx = args.index  # type: ignore[attr-defined]
    if idx < 0 or idx >= len(jobs):
        print(f"Invalid index {idx}. Use 'crontui list' to see jobs.")
        raise SystemExit(1)
    jobs[idx].enabled = True
    manager.save(jobs)
    print(f"Enabled job {idx}: {jobs[idx].schedule} {jobs[idx].command}")


def _cli_disable(args: object) -> None:
    manager = CrontabManager()
    jobs = manager.load()
    idx = args.index  # type: ignore[attr-defined]
    if idx < 0 or idx >= len(jobs):
        print(f"Invalid index {idx}. Use 'crontui list' to see jobs.")
        raise SystemExit(1)
    jobs[idx].enabled = False
    manager.save(jobs)
    print(f"Disabled job {idx}: {jobs[idx].schedule} {jobs[idx].command}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(prog="crontui", description="TUI and CLI for managing cron jobs")
    sub = parser.add_subparsers(dest="subcmd")

    sub.add_parser("list", help="List all cron jobs")

    p_add = sub.add_parser("add", help="Add a new cron job")
    p_add.add_argument("schedule", help="Cron expression (e.g. '*/5 * * * *')")
    p_add.add_argument("command", help="Command to run")
    p_add.add_argument("-d", "--description", default="", help="Job description")

    p_rm = sub.add_parser("remove", help="Remove a cron job by index")
    p_rm.add_argument("index", type=int, help="Job index (from 'crontui list')")

    p_en = sub.add_parser("enable", help="Enable a cron job by index")
    p_en.add_argument("index", type=int, help="Job index")

    p_dis = sub.add_parser("disable", help="Disable a cron job by index")
    p_dis.add_argument("index", type=int, help="Job index")

    p_claude = sub.add_parser("add-claude", help="Add a periodic Claude task")
    p_claude.add_argument("schedule", help="Cron expression (e.g. '0 10 * * *')")
    grp = p_claude.add_mutually_exclusive_group(required=True)
    grp.add_argument("--prompt", help="Prompt text for Claude")
    grp.add_argument("--prompt-file", help="Read the prompt from a file")
    p_claude.add_argument("--cwd", default="", help="Working directory (default: home)")
    p_claude.add_argument("-n", "--name", default="", help="Task name (used for the slug)")
    p_claude.add_argument("-d", "--description", default="", help="Job description")
    p_claude.add_argument("--model", default="", help="Model alias/name (default: your claude default)")
    p_claude.add_argument(
        "--permission-mode", default="bypassPermissions", choices=claude.PERMISSION_MODES
    )
    p_claude.add_argument("--auth", default="oauth", choices=claude.AUTH_MODES)
    p_claude.add_argument("--timeout", type=int, default=claude.DEFAULT_TIMEOUT_SEC, help="Timeout (seconds)")
    p_claude.add_argument("--max-turns", type=int, default=None)
    p_claude.add_argument("--max-budget-usd", type=float, default=None)
    p_claude.add_argument(
        "--allow", action="append", help="Allowed tool, repeatable (for --permission-mode dontAsk)"
    )

    p_exec = sub.add_parser("exec", help="Run a Claude task now (invoked by cron)")
    p_exec.add_argument("slug", help="Task slug")

    p_auth = sub.add_parser("auth", help="Store the Claude OAuth token for unattended runs")
    p_auth.add_argument("--token", default="", help="Token value (otherwise read from stdin)")

    args = parser.parse_args()

    match args.subcmd:
        case None:
            CrontuiApp().run()
        case "list":
            _cli_list(args)
        case "add":
            _cli_add(args)
        case "remove":
            _cli_remove(args)
        case "enable":
            _cli_enable(args)
        case "disable":
            _cli_disable(args)
        case "add-claude":
            _cli_add_claude(args)
        case "exec":
            raise SystemExit(claude.run_claude_task(args.slug))
        case "auth":
            _cli_auth(args)


if __name__ == "__main__":
    main()
