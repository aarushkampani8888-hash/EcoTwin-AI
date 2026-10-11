"""
EcoTwin AI: start everything in one process (used for hosting, works locally too).

    python3 run_all.py

Runs, side by side:
  * the dashboard (FastAPI) on 0.0.0.0:$PORT  (PORT is set by hosts like Render; default 8000)
  * the detection engine (ecotwin_core.run) in a background thread
  * the sensor simulator in headless mode in a background thread

Each background worker restarts itself if it crashes, so a temporary
database or network hiccup doesn't take the site down.
"""

from __future__ import annotations

import logging
import os
import threading
import time

from dotenv import load_dotenv

load_dotenv()
# A server has no speakers: the dashboard plays alerts in the browser instead.
os.environ.setdefault("AUTOPLAY", "false")

import uvicorn  # noqa: E402

import dashboard  # noqa: E402
import ecotwin_core  # noqa: E402
import simulate  # noqa: E402

log = logging.getLogger("ecotwin.run_all")


def supervise(name: str, target) -> threading.Thread:
    def loop() -> None:
        while True:
            try:
                target()
                log.warning("%s exited; restarting in 10s", name)
            except SystemExit as exc:
                log.error("%s stopped: %s (retrying in 10s)", name, exc)
            except Exception:
                log.exception("%s crashed; restarting in 10s", name)
            time.sleep(10)

    thread = threading.Thread(target=loop, name=name, daemon=True)
    thread.start()
    dashboard.WORKERS[name] = thread
    return thread


def prepare_database() -> None:
    """Create/upgrade tables once, before the workers start, so they never race each other."""
    import psycopg

    with psycopg.connect(ecotwin_core.DATABASE_URL) as conn:
        ecotwin_core.ensure_schema(conn)
        simulate.ensure_demo_table(conn)
    ecotwin_core.memory.ensure_assistant()


def main() -> None:
    node = int(os.getenv("DASHBOARD_NODE", "402"))
    try:
        prepare_database()
    except Exception:
        log.exception("Database preparation failed; workers will retry on their own")
    supervise("engine", lambda: ecotwin_core.run())
    supervise("simulator", lambda: simulate.run_headless(
        node_id=node,
        auto_spike_minutes=float(os.getenv("AUTO_SPIKE_MINUTES", "0") or 0),
        auto_spike_seconds=float(os.getenv("SPIKE_SECONDS", "45")),
    ))
    port = int(os.getenv("PORT", "8000"))
    log.info("EcoTwin AI on http://0.0.0.0:%s", port)
    uvicorn.run(dashboard.app, host="0.0.0.0", port=port, log_level="warning", proxy_headers=True,
                forwarded_allow_ips="*")


if __name__ == "__main__":
    main()
