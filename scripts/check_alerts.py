#!/usr/bin/env python3
"""
Check Photon alert queue and output pending alerts.
Designed to run as a Claude Code UserPromptSubmit hook.

- Outputs to stdout → Claude sees it as context
- Writes to ~/.photon_alerts.log → user can tail it
- Sends macOS notification on high-priority alerts
"""
import sqlite3
import os
import subprocess
from pathlib import Path
from datetime import datetime

DB_PATH = Path(os.environ.get("PHOTON_DB", Path.home() / "Code" / "photon" / "photon.db"))
LOG_PATH = Path.home() / ".photon_alerts.log"

ICONS = {
    "staircase": "📈",
    "spring": "🔋",
    "surge": "⚡",
    "spring_ignition": "🔥",
    "classification_change": "🔄",
    "concentrating": "⚠️",
    "velocity_crash": "📉",
    "active_trap": "🪤",
    "developing": "🌱",
    "danger": "💀",
}

# Alert types worth a macOS notification
HIGH_PRIORITY = {"staircase", "spring", "surge", "spring_ignition", "classification_change"}

def notify_macos(title, message):
    """Send a macOS notification via osascript."""
    try:
        subprocess.run(
            ["osascript", "-e", f'display notification "{message}" with title "{title}"'],
            timeout=3,
            capture_output=True,
        )
    except Exception:
        pass

def check_alerts():
    if not DB_PATH.exists():
        return

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=1)
        conn.execute("PRAGMA journal_mode=wal")

        count = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE acknowledged = 0"
        ).fetchone()[0]

        if count == 0:
            return

        rows = conn.execute(
            """SELECT alert_type, message, confidence
               FROM alerts WHERE acknowledged = 0
               ORDER BY confidence DESC LIMIT 5""",
        ).fetchall()

        type_counts = {}
        for row in conn.execute(
            "SELECT alert_type, COUNT(*) FROM alerts WHERE acknowledged = 0 GROUP BY alert_type"
        ).fetchall():
            type_counts[row[0]] = row[1]

        conn.close()

        # Format output for Claude
        lines = [f"[Photon] {count} pending alerts:"]
        for atype, acount in sorted(type_counts.items(), key=lambda x: -x[1]):
            icon = ICONS.get(atype, "•")
            lines.append(f"  {icon} {atype}: {acount}")

        interesting = [r for r in rows if r[0] != "danger"][:3]
        if interesting:
            lines.append("")
            for atype, msg, conf in interesting:
                short = msg[:120] + "..." if len(msg) > 120 else msg
                lines.append(f"  [{conf}] {short}")

        output = "\n".join(lines)

        # Print to stdout → Claude sees it
        print(output)

        # Append to log file → user can tail -f ~/.photon_alerts.log
        with open(LOG_PATH, "a") as f:
            f.write(f"\n--- {datetime.now().strftime('%H:%M:%S')} ---\n")
            f.write(output + "\n")

        # macOS notification for high-priority alerts
        high_priority_alerts = [r for r in rows if r[0] in HIGH_PRIORITY]
        if high_priority_alerts:
            best = high_priority_alerts[0]
            short_msg = best[1][:80]
            notify_macos(f"Photon [{best[2]}]", short_msg)

    except Exception:
        pass

if __name__ == "__main__":
    check_alerts()
