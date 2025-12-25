from __future__ import annotations

import os
import platform
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import Thread

from croniter import croniter
from rich.text import Text
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.theme import Theme
from textual.widgets import Button, DataTable, Footer, Input, Label, RichLog, Static

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
        right = f"{host}  {now}"
        left_plain = f"Jobs    {total} total, {self._active_count} active, {self._disabled_count} disabled"
        pad = max(1, self.size.width - len(left_plain) - len(right) - 2)
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
            yield Label("[bold]Keyboard Shortcuts[/]\n")
            yield Label(
                "[bold]space[/]    Toggle enable/disable\n"
                "[bold]a[/]        Add new job\n"
                "[bold]e[/]        Edit selected job\n"
                "[bold]d[/]        Delete selected job\n"
                "[bold]x[/]        Run selected job now\n"
                "[bold]l[/]        View logs for selected job\n"
                "[bold]r[/]        Refresh from crontab\n"
                "[bold]?[/]        This help screen\n"
                "[bold]q[/]        Quit"
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


class CrontuiApp(App):
    TITLE = "crontui"

    CSS = """
    Screen { background: #ffffff; color: #1a1a1a; }
    DataTable { height: 1fr; background: #ffffff; color: #1a1a1a; overflow-x: hidden; }
    DataTable > .datatable--header { background: #f5f5f5; color: #333333; text-style: bold; }
    DataTable > .datatable--cursor { background: #e0f2f9; color: #1a1a1a; }
    DataTable > .datatable--hover { background: #f0f0f0; color: #1a1a1a; }
    DataTable > .datatable--even-row { background: #ffffff; }
    DataTable > .datatable--odd-row { background: #fafafa; }
    ModalScreen { background: rgba(0, 0, 0, 0.4); }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("ctrl+c", "quit", "Quit", show=False),
        Binding("question_mark", "help", "?:Help"),
        Binding("a", "add", "Add"),
        Binding("d", "delete", "Delete"),
        Binding("e", "edit", "Edit"),
        Binding("space", "toggle", "Toggle"),
        Binding("x", "run_now", "Run"),
        Binding("l", "logs", "Logs"),
        Binding("r", "refresh", "Refresh"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.register_theme(CRONTUI_THEME)
        self.theme = "crontui"
        self.manager = CrontabManager()
        self.jobs: list[CronJob] = []

    def compose(self) -> ComposeResult:
        yield HeaderBar()
        yield DataTable(id="jobs-table")
        yield Footer()

    def _compute_column_widths(self) -> list[int]:
        total = self.size.width - 2
        status_w = 5
        sched_w = max(8, max((len(j.schedule) for j in self.jobs), default=8))
        next_w = max(8, max((len(j.next_run_display) for j in self.jobs), default=8))
        last_w = max(8, max((len(j.last_run_display) for j in self.jobs), default=8))
        cmd_need = max(7, max((len(j.command) for j in self.jobs), default=7))
        desc_need = max(11, max((len(j.description) for j in self.jobs), default=11))
        fixed = status_w + sched_w + next_w + last_w
        remaining = max(0, total - fixed)
        needs = [cmd_need, desc_need]
        total_need = sum(needs)
        if total_need <= remaining:
            extra = remaining - total_need
            cmd_w = cmd_need + extra // 2
            desc_w = desc_need + extra - extra // 2
        elif remaining <= 0:
            cmd_w, desc_w = 7, 11
        else:
            cmd_w = max(7, round(remaining * cmd_need / total_need))
            desc_w = max(11, remaining - cmd_w)
        return [status_w, sched_w, next_w, last_w, cmd_w, desc_w]

    def _apply_column_widths(self) -> None:
        table = self.query_one("#jobs-table", DataTable)
        for col, w in zip(table.ordered_columns, self._compute_column_widths(), strict=False):
            col.width = w

    def on_mount(self) -> None:
        CRONTUI_LOGS.mkdir(parents=True, exist_ok=True)
        table = self.query_one("#jobs-table", DataTable)
        table.zebra_stripes = True
        table.cursor_type = "row"
        for name in ["", "Schedule", "Next Run", "Last Run", "Command", "Description"]:
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

    def _refresh_table(self) -> None:
        table = self.query_one("#jobs-table", DataTable)
        table.clear()
        widths = self._compute_column_widths()
        cmd_w, desc_w = widths[4], widths[5]
        active = sum(1 for j in self.jobs if j.enabled)
        disabled = sum(1 for j in self.jobs if not j.enabled)
        self.query_one(HeaderBar).update_counts(active, disabled)
        for job in self.jobs:
            cmd = self._trunc(job.command, cmd_w)
            desc = self._trunc(job.description, desc_w)
            if job.enabled:
                table.add_row(
                    Text("● ON", style="bold #1b5e20"),
                    Text(job.schedule, style="bold #1a1a1a"),
                    Text(job.next_run_display, style="#007a9e"),
                    Text(job.last_run_display, style="#666666"),
                    Text(cmd, style="#1a1a1a"),
                    Text(desc, style="#888888"),
                )
            else:
                table.add_row(
                    Text("○ OFF", style="#bbbbbb"),
                    Text(job.schedule, style="#bbbbbb"),
                    Text("—", style="#bbbbbb"),
                    Text("—", style="#bbbbbb"),
                    Text(cmd, style="#bbbbbb"),
                    Text(desc, style="#bbbbbb"),
                )

    def _refresh_next_run(self) -> None:
        table = self.query_one("#jobs-table", DataTable)
        for i, job in enumerate(self.jobs):
            if job.enabled:
                table.update_cell_at((i, 2), Text(job.next_run_display))

    def _get_selected(self) -> int | None:
        table = self.query_one("#jobs-table", DataTable)
        if table.row_count == 0:
            return None
        row = table.cursor_row
        return row if 0 <= row < len(self.jobs) else None

    def action_help(self) -> None:
        self.push_screen(HelpModal())

    def action_refresh(self) -> None:
        self._load_jobs()
        self.notify("Refreshed", timeout=2)

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
        preview = self.jobs[idx].command[:50] + ("..." if len(self.jobs[idx].command) > 50 else "")

        def on_confirm(confirmed: bool | None) -> None:
            if confirmed:
                self.jobs.pop(idx)
                self.manager.save(self.jobs)
                self.notify("Deleted", timeout=2)
                self._refresh_table()

        self.push_screen(ConfirmModal(f"Delete job?\n\n[bold]{preview}[/]"), on_confirm)

    @staticmethod
    def _ensure_log_redirect(command: str) -> str:
        if re.search(r">>?\s*\S+", command):
            return command
        log_file = CRONTUI_LOGS / f"{_slug(command)}.log"
        return f"{command} >> {log_file} 2>&1"

    def action_add(self) -> None:
        def on_result(result: dict | None) -> None:
            if result:
                cmd = self._ensure_log_redirect(result["command"])
                self.jobs.append(CronJob(result["schedule"], cmd, True, result["description"]))
                self.manager.save(self.jobs)
                self.notify("Added", timeout=2)
                self._refresh_table()

        self.push_screen(JobFormModal(title="Add Job"), on_result)

    def action_edit(self) -> None:
        if (idx := self._get_selected()) is None:
            return
        job = self.jobs[idx]

        def on_result(result: dict | None) -> None:
            if result:
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

    def action_run_now(self) -> None:
        if (idx := self._get_selected()) is None:
            return
        job = self.jobs[idx]
        cmd = re.sub(r"\s*>>?\s*\S+\s*2>&1\s*$", "", job.command)
        self.notify(f"Running: {cmd[:40]}...", timeout=2)

        def _worker() -> None:
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
                    self.app.call_from_thread(self.notify, "Job completed", timeout=3)
                else:
                    self.app.call_from_thread(
                        self.notify, f"Job failed (exit {result.returncode})", timeout=5, severity="error"
                    )
            except subprocess.TimeoutExpired:
                self.app.call_from_thread(self.notify, "Job timed out (5m)", timeout=5, severity="error")
            except Exception as e:
                self.app.call_from_thread(self.notify, f"Error: {e!s:.50}", timeout=5, severity="error")
            self.app.call_from_thread(self._refresh_table)

        Thread(target=_worker, daemon=True).start()

    def action_logs(self) -> None:
        if (idx := self._get_selected()) is None:
            return
        self.push_screen(LogViewerModal(self.jobs[idx]))


def main() -> None:
    CrontuiApp().run()


if __name__ == "__main__":
    main()
