# crontui

TUI and CLI for managing cron jobs — and **periodic Claude tasks** (unattended `claude -p` runs
that do real work on a schedule).

![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

## Install

```bash
pip install git+https://github.com/triuzzi/crontui.git
```

## TUI

```bash
crontui
```

crontui is **command-palette driven**: press **`Ctrl+P`** and everything is one fuzzy search away
— *New Claude task*, *New script job*, *Run now*, *Edit*, *View logs*, *Enable/Disable*, *Delete*,
*Set up Claude auth token*. Type a job's name to act on it directly. The main screen is a master–detail
view: the job list on the left, full details (prompt, working dir, schedule, recent log) on the right.

| Key | Action |
|-----|--------|
| `Ctrl+P` | Command palette — **everything lives here** |
| `↑ ↓` / `j k` | Move selection |
| `Enter` | Edit the selected job |
| `q` | Quit |

> Terminals can't deliver `Cmd+P` to a TUI, so the palette is bound to `Ctrl+P`. In iTerm2 you can map
> `Cmd+P` → send `Ctrl+P` (Settings → Keys) if you want the literal Cmd feel.

## Claude tasks

A Claude task runs `claude -p "<your prompt>"` in a working directory on a cron schedule. The schedule
and enabled/disabled state live in your crontab; the prompt and settings live in `~/.crontui/tasks/`.

### One-time auth setup

Unattended runs authenticate with a long-lived OAuth token (so they don't depend on the macOS keychain,
which can hang with no TTY):

```bash
claude setup-token          # prints a ~1-year token
crontui auth                # paste the token (stored in ~/.crontui/env, chmod 600)
```

(Or do it in the TUI: `Ctrl+P` → *Set up Claude auth token*.) Prefer pay-per-use API billing instead?
Put `ANTHROPIC_API_KEY=...` in `~/.crontui/env` and create the task with `--auth api_key` (runs `--bare`).

### Create one

Easiest via the TUI (`Ctrl+P` → *New Claude task*) — a full-screen editor with a prompt box, a directory
picker, model/permission selectors, and an Advanced section. Or from the CLI:

```bash
crontui add-claude '0 10 * * *' \
  --cwd ~/knowledge-sharings \
  --prompt-file ./fix-topics.md \
  -n ks-structure -d "Fix knowledge-sharings topic.json + rebuild index"
```

| `add-claude` option | Meaning |
|---------------------|---------|
| `--prompt` / `--prompt-file` | The task prompt (inline or from a file) — **required** |
| `--cwd` | Working directory (default: home) |
| `-n, --name` | Task name (used for the slug) |
| `--model` | Model alias/name (default: your `claude` default) |
| `--permission-mode` | `bypassPermissions` (default), `dontAsk`, `acceptEdits`, … |
| `--auth` | `oauth` (default), `api_key`, or `keychain` |
| `--timeout` | Seconds before the run is killed (default: 1800) |
| `--max-turns`, `--max-budget-usd` | Guardrails passed straight to `claude` |
| `--allow` | Allowed tool, repeatable — used when `--permission-mode dontAsk` |

`crontui exec <slug>` runs a task immediately (this is what cron invokes); the TUI's *Run now* does the same.

### Permissions

Cron has no TTY, so a run must never block on a permission prompt. Two safe postures:

- **`bypassPermissions`** (default) — full autonomy; the working directory is the practical safety boundary.
  Needed when the task must edit files and run scripts freely. (`rm -rf /` and `rm -rf ~` are still blocked.)
- **`dontAsk` + `--allow`** — fail-fast allowlist; anything not listed is auto-denied (the task may stop
  early) but never hangs. E.g. `--permission-mode dontAsk --allow Read --allow Edit --allow 'Bash(./build-index.sh)'`.

## Script jobs (CLI)

```bash
crontui list
crontui add '*/5 * * * *' '/path/to/script.sh' -d 'My backup job'
crontui remove 2
crontui enable 1
crontui disable 0
```

Jobs added via CLI get automatic log redirection to `~/.crontui/logs/`.

## Files & macOS cron notes

```
~/.crontui/tasks/<slug>.json   # task config        ~/.crontui/logs/<slug>.log   # output
~/.crontui/tasks/<slug>.md     # task prompt         ~/.crontui/env               # secrets (chmod 600)
```

- Cron runs with a minimal `PATH`; the runner resolves `claude` from `/opt/homebrew/bin` etc. and injects
  `~/.crontui/env`. If you version `~/.crontui/` in dotfiles, sync `tasks/` but **never** `env`.
- `crontui exec` enforces the per-task timeout (macOS has no `timeout` binary) and sends a desktop
  notification on failure.

## License

MIT
