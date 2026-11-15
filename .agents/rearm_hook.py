#!/usr/bin/env python3
"""Hook Antigravity de réarmement déclaratif des démons et crons (.agents/schedules.json).

Se déclenche :
1. À chaque `SessionStart` (nouvelle conversation).
2. Au premier `PreInvocation` suivant un redémarrage machine (marqueur volatile dans XDG_RUNTIME_DIR / `/run/user/$UID`
   ou port 8000 fermé), ou dès que l'utilisateur saisit la commande courte `rearm` / `/rearm` / `relance les crons`.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import sys


TRIGGER_COMMANDS = {
    "rearm",
    "/rearm",
    "rearm-automations",
    "relance les crons",
    "relancer les crons",
    "restore crons",
    "restore le cron",
}


def _is_port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.4)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def main() -> None:
    raw_stdin = sys.stdin.read().strip()
    payload = json.loads(raw_stdin) if raw_stdin else {}
    event = payload.get("hookEventName", "")
    conv_id = payload.get("conversationId", "default")
    last_input = (payload.get("lastUserInput") or "").strip().lower()

    here = Path(__file__).resolve().parent
    sched_file = here / "schedules.json"
    if not sched_file.exists():
        print("{}")
        return

    data = json.loads(sched_file.read_text(encoding="utf-8"))
    active_daemons = [d for d in data.get("daemons", []) if d.get("enabled", True)]
    active_schedules = [s for s in data.get("schedules", []) if s.get("enabled", True)]
    if not active_daemons and not active_schedules:
        print("{}")
        return

    runtime_dir = Path(os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}")
    if not runtime_dir.exists():
        runtime_dir = Path("/tmp")
    stamp_file = runtime_dir / f"morning-review-rearmed-{conv_id}.stamp"

    explicit_rearm = last_input in TRIGGER_COMMANDS
    port_down = any(not _is_port_open(int(d["port"])) for d in active_daemons if d.get("port"))

    should_inject = False
    if event == "SessionStart":
        should_inject = True
    elif event == "PreInvocation":
        if explicit_rearm or not stamp_file.exists() or port_down:
            should_inject = True

    if not should_inject:
        print("{}")
        return

    try:
        stamp_file.write_text("armed\n", encoding="utf-8")
    except OSError:
        pass

    msg = (
        f"[Auto-Rearm Automations ({event})] Vérifie les tâches actives avec `manage_task(Action=\"list\")`. "
        "Pour chaque démon et chaque cron déclaré dans `.agents/schedules.json` qui n'est pas encore actif dans "
        "cette session (ou si le port 8000 est fermé), réarme-le immédiatement (`run_command` avec `IsDaemon=True` "
        "pour les démons, puis `schedule` avec `IsDaemon=True` pour les crons) : "
        + json.dumps({"daemons": active_daemons, "schedules": active_schedules}, ensure_ascii=False)
    )
    print(json.dumps({"injectSteps": [{"systemMessage": {"systemMessage": msg}}]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
