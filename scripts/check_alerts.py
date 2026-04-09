#!/usr/bin/env python3
"""
Check Photon alert queue and output pending alerts.
Designed to run as a Claude Code UserPromptSubmit hook.
Outputs to stdout — Claude sees the output as context before responding.
"""
import sqlite3
import sys
import os
import json
from pathlib import Path

DB_PATH = Path(os.environ.get("PHOTON_DB", Path.home() / "Code" / "photon" / "photon.db"))

def check_alerts():
    if not DB_PATH.exists():
        return

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=1)
        conn.execute("PRAGMA journal_mode=wal")

        # Count pending alerts
        count = conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE acknowledged = 0"
        ).fetchone()[0]

        if count == 0:
            return

        # Get top alerts by type
        rows = conn.execute(
            """SELECT alert_type, message, confidence
               FROM alerts
               WHERE acknowledged = 0
               ORDER BY confidence DESC
               LIMIT 5""",
        ).fetchall()

        # Group by type
        type_counts = {}
        for row in conn.execute(
            "SELECT alert_type, COUNT(*) FROM alerts WHERE acknowledged = 0 GROUP BY alert_type"
        ).fetchall():
            type_counts[row[0]] = row[1]

        conn.close()

        # Format output
        lines = [f"[Photon] {count} pending alerts:"]

        for atype, acount in sorted(type_counts.items(), key=lambda x: -x[1]):
            icon = {
                "staircase": "📈",
                "spring": "🔋",
                "surge": "⚡",
                "classification_change": "🔄",
                "concentrating": "⚠️",
                "velocity_crash": "📉",
                "active_trap": "🪤",
                "danger": "💀",
            }.get(atype, "•")
            lines.append(f"  {icon} {atype}: {acount}")

        # Show top 3 non-danger alerts
        interesting = [r for r in rows if r[0] != "danger"][:3]
        if interesting:
            lines.append("")
            for atype, msg, conf in interesting:
                # Truncate message
                short = msg[:120] + "..." if len(msg) > 120 else msg
                lines.append(f"  [{conf}] {short}")

        print("\n".join(lines))

    except Exception:
        # Silent failure — don't break the hook chain
        pass

if __name__ == "__main__":
    check_alerts()
