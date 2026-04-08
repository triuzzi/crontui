# crontui

TUI and CLI for managing cron jobs.

![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

## Install

```bash
pip install git+https://github.com/triuzzi/crontui.git
```

## Usage

### TUI

```bash
crontui
```

| Key | Action |
|-----|--------|
| `space` | Toggle enable/disable |
| `a` | Add job |
| `e` | Edit job |
| `d` | Delete job |
| `x` | Run job now |
| `l` | View logs |
| `r` | Refresh |
| `?` | Help |
| `q` | Quit |

### CLI

```bash
crontui list
crontui add '*/5 * * * *' '/path/to/script.sh' -d 'My backup job'
crontui remove 2
crontui enable 1
crontui disable 0
```

| Command | Description |
|---------|-------------|
| `list` | Show all jobs with index, status, schedule, command |
| `add <schedule> <command> [-d desc]` | Add a new cron job |
| `remove <index>` | Remove a job by index |
| `enable <index>` | Enable a disabled job |
| `disable <index>` | Disable a job without removing it |

Jobs added via CLI get automatic log redirection to `~/.crontui/logs/`.

## License

MIT
