"""
EcoTwin AI: live sensor simulator for demos.

Streams water-turbidity readings for one node into Tiger Data, a few seconds
apart, like a real sensor would.

Interactive (in a Terminal window):
    python3 simulate.py --reset                  keys: s = spike, n = normal, q = quit
    python3 simulate.py --reset --spike-after 20 spike starts by itself after 20s

Headless (used by run_all.py when hosted): no keyboard. A spike runs whenever the
dashboard's "Trigger contamination spike" button sets one in the demo_controls
table, and optionally on a timer (AUTO_SPIKE_MINUTES).

The dashboard button works in interactive mode too.

Every simulated row is tagged raw_payload = {"source": "simulator"} so
simulated data is always distinguishable from real sensor data.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import select
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

load_dotenv()
log = logging.getLogger("ecotwin.sim")

NORMAL_LEVEL = 13.5   # NTU, typical clean runoff
LIMIT = 15.0          # matches SENSOR_PROFILES["water_runoff"]["max"] in ecotwin_core.py
RAMP = 0.35           # fraction of the gap closed each tick, so changes look gradual

ZONE = "Downtown Industrial Runoff Basin"
DEFAULT_RESOURCES = ["Rapid-response mobile field crew", "Public alert broadcast system",
                     "SMS resident notification"]

DEMO_CONTROLS_SQL = """
CREATE TABLE IF NOT EXISTS demo_controls (
    node_id      INT PRIMARY KEY,
    spike_until  TIMESTAMPTZ,
    requested_at TIMESTAMPTZ,
    requested_by TEXT
);
"""


def ensure_demo_table(conn: psycopg.Connection) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(DEMO_CONTROLS_SQL)
        conn.commit()
    except psycopg.errors.UniqueViolation:
        conn.rollback()  # another process created it at the same moment: fine


def setup(conn: psycopg.Connection, node_id: int, reset: bool) -> None:
    ensure_demo_table(conn)
    with conn.cursor() as cur:
        # Make sure the node exists (keeps any response_resources you already set).
        cur.execute(
            """
            INSERT INTO sensor_nodes (node_id, zone_name, latitude, longitude, sensor_type)
            VALUES (%s, %s, 43.073000, -89.401000, 'water_runoff')
            ON CONFLICT (node_id) DO NOTHING
            """,
            (node_id, ZONE),
        )
        cur.execute(
            "SELECT setval('sensor_nodes_node_id_seq', GREATEST((SELECT max(node_id) FROM sensor_nodes), 1))"
        )
        # Give the AI real resources to plan with, if none are set yet.
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'sensor_nodes' AND column_name = 'response_resources'
            """
        )
        if cur.fetchone():
            cur.execute(
                "UPDATE sensor_nodes SET response_resources = %s WHERE node_id = %s AND response_resources IS NULL",
                (DEFAULT_RESOURCES, node_id),
            )
        cur.execute("SELECT zone_name FROM sensor_nodes WHERE node_id = %s", (node_id,))
        zone = cur.fetchone()[0]

        if reset:
            cur.execute("DELETE FROM sensor_readings WHERE node_id = %s", (node_id,))
            cur.execute("DELETE FROM environmental_incidents WHERE zone_name = %s", (zone,))
            cur.execute("DELETE FROM demo_controls WHERE node_id = %s", (node_id,))
            log.info("Reset: cleared readings for node %s and incidents for '%s'.", node_id, zone)

        # The detector compares the last 10 minutes against the hour before,
        # so it needs history. Backfill an hour of normal readings if missing
        # (also covers a hosted service waking up after a pause).
        cur.execute(
            "SELECT count(*) FROM sensor_readings WHERE node_id = %s AND time > now() - interval '70 minutes'",
            (node_id,),
        )
        if cur.fetchone()[0] < 30:
            cur.execute(
                """
                INSERT INTO sensor_readings (time, node_id, metric_value, raw_payload)
                SELECT t, %s, 12 + random() * 3, %s
                FROM generate_series(now() - interval '70 minutes', now() - interval '1 minute',
                                     interval '1 minute') AS t
                """,
                (node_id, Jsonb({"source": "simulator", "backfill": True})),
            )
            log.info("Backfilled 70 minutes of normal baseline readings.")
    conn.commit()


def remote_spike_active(conn: psycopg.Connection, node_id: int) -> bool:
    """True while the dashboard button (or the auto timer) has a spike running."""
    with conn.cursor() as cur:
        cur.execute("SELECT spike_until > now() FROM demo_controls WHERE node_id = %s", (node_id,))
        row = cur.fetchone()
    conn.commit()
    return bool(row and row[0])


def schedule_spike(conn: psycopg.Connection, node_id: int, seconds: float, by: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO demo_controls (node_id, spike_until, requested_at, requested_by)
            VALUES (%s, now() + make_interval(secs => %s), now(), %s)
            ON CONFLICT (node_id) DO UPDATE
              SET spike_until = EXCLUDED.spike_until, requested_at = EXCLUDED.requested_at,
                  requested_by = EXCLUDED.requested_by
            """,
            (node_id, seconds, by),
        )
    conn.commit()


def clear_spike(conn: psycopg.Connection, node_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE demo_controls SET spike_until = now() WHERE node_id = %s", (node_id,))
    conn.commit()


class Stream:
    """Generates one smoothly-varying reading per tick and writes it to Tiger Data."""

    def __init__(self, conn: psycopg.Connection, node_id: int, spike_level: float) -> None:
        self.conn, self.node_id, self.spike_level = conn, node_id, spike_level
        self.level = NORMAL_LEVEL

    def tick(self, state: str) -> float:
        target = self.spike_level if state == "spike" else NORMAL_LEVEL
        self.level += (target - self.level) * RAMP
        noise = random.gauss(0, 3.0 if state == "spike" else 0.6)
        value = round(max(0.0, self.level + noise), 2)
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sensor_readings (time, node_id, metric_value, raw_payload) VALUES (now(), %s, %s, %s)",
                (self.node_id, value, Jsonb({"source": "simulator", "state": state})),
            )
        self.conn.commit()
        return value


# ---------------------------------------------------------------------------
# Headless mode (hosted)
# ---------------------------------------------------------------------------
def run_headless(node_id: int = 402, interval: float = 2.0, spike_level: float = 80.0,
                 auto_spike_minutes: float = 0.0, auto_spike_seconds: float = 45.0,
                 stop: threading.Event | None = None) -> None:
    url = os.getenv("TIGER_DATABASE_URL")
    if not url:
        raise SystemExit("TIGER_DATABASE_URL is missing")
    stop = stop or threading.Event()
    with psycopg.connect(url) as conn:
        setup(conn, node_id, reset=False)
        stream = Stream(conn, node_id, spike_level)
        next_auto = time.monotonic() + auto_spike_minutes * 60 if auto_spike_minutes > 0 else None
        log.info("Simulator streaming node %s every %ss (auto spike: %s)", node_id, interval,
                 f"every {auto_spike_minutes:g} min" if next_auto else "off")
        last_state = None
        while not stop.is_set():
            if next_auto and time.monotonic() >= next_auto:
                schedule_spike(conn, node_id, auto_spike_seconds, "auto-timer")
                next_auto = time.monotonic() + auto_spike_minutes * 60
            state = "spike" if remote_spike_active(conn, node_id) else "normal"
            if state != last_state:
                log.info("Simulator state: %s", state)
                last_state = state
            stream.tick(state)
            stop.wait(interval)


# ---------------------------------------------------------------------------
# Interactive mode (Terminal)
# ---------------------------------------------------------------------------
@contextmanager
def keypress_mode():
    """Read single keys without waiting for Enter."""
    import termios
    import tty

    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        yield
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


def read_key(timeout: float) -> str | None:
    ready, _, _ = select.select([sys.stdin], [], [], timeout)
    return sys.stdin.read(1).lower() if ready else None


def run(node_id: int, interval: float, spike_level: float, reset: bool, spike_after: float | None = None) -> None:
    if not sys.stdin.isatty():
        raise SystemExit("Run this directly in a Terminal window so it can read keypresses "
                         "(or use run_all.py, which runs the simulator headless).")
    url = os.getenv("TIGER_DATABASE_URL")
    if not url:
        raise SystemExit("TIGER_DATABASE_URL is missing from .env")

    with psycopg.connect(url) as conn:
        setup(conn, node_id, reset)
        stream = Stream(conn, node_id, spike_level)
        print(f"\nStreaming node {node_id} every {interval:g}s.  [s] spike   [n] normal   [q] quit"
              "\n(The dashboard's 'Trigger contamination spike' button also works.)\n")

        manual = "normal"
        next_tick = time.monotonic()
        auto_spike_at = time.monotonic() + spike_after if spike_after else None
        if auto_spike_at:
            print(f"Auto-spike scheduled in {spike_after:g}s.")
        with keypress_mode():
            while True:
                key = read_key(max(0.0, next_tick - time.monotonic()))
                if key == "q":
                    break
                if auto_spike_at and time.monotonic() >= auto_spike_at:
                    key, auto_spike_at = "s", None
                if key == "s" and manual != "spike":
                    manual = "spike"
                    print(">>> Contamination event started")
                elif key == "n":
                    manual = "normal"
                    clear_spike(conn, node_id)
                    print(">>> Returning to normal")
                if time.monotonic() < next_tick:
                    continue  # a key arrived early; wait for the actual tick
                next_tick += interval

                state = "spike" if manual == "spike" or remote_spike_active(conn, node_id) else "normal"
                value = stream.tick(state)
                bar = "#" * min(40, int(value / 2))
                flag = "  !! over limit" if value > LIMIT else ""
                stamp = datetime.now().strftime("%H:%M:%S")
                print(f"{stamp}  {state.upper():7} {value:6.1f} NTU  {bar}{flag}")

    print("Simulator stopped.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser(description="EcoTwin AI live sensor simulator")
    ap.add_argument("--node", type=int, default=402)
    ap.add_argument("--interval", type=float, default=2.0, help="seconds between readings")
    ap.add_argument("--spike-level", type=float, default=80.0, help="NTU the spike ramps toward")
    ap.add_argument("--spike-after", type=float, default=None,
                    help="start the spike automatically after this many seconds (no keypress needed)")
    ap.add_argument("--reset", action="store_true",
                    help="clear this node's readings and its zone's incidents first (clean demo)")
    ap.add_argument("--headless", action="store_true",
                    help="no keyboard: spikes come from the dashboard button or AUTO_SPIKE_MINUTES")
    args = ap.parse_args()
    if args.headless:
        run_headless(args.node, args.interval, args.spike_level,
                     float(os.getenv("AUTO_SPIKE_MINUTES", "0") or 0))
    else:
        run(args.node, args.interval, args.spike_level, args.reset, args.spike_after)
