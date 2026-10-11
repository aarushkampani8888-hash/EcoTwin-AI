"""
EcoTwin AI: live sensor simulator for demos.

Streams water-turbidity readings for one node into Tiger Data, a few seconds
apart, like a real sensor would. You control what the "river" does:

    s   start a contamination spike (ramps up to ~80 NTU)
    n   return to normal (ramps back down to ~13 NTU)
    q   quit

Demo setup (two Terminal windows, both in the ecotwin-ai folder):
    Window 1:  python3 simulate.py --reset
    Window 2:  python3 ecotwin_core.py

Every simulated row is tagged raw_payload = {"source": "simulator"} so
simulated data is always distinguishable from real sensor data.
"""

from __future__ import annotations

import argparse
import os
import random
import select
import sys
import termios
import time
import tty
from contextlib import contextmanager
from datetime import datetime

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

load_dotenv()

NORMAL_LEVEL = 13.5   # NTU, typical clean runoff
LIMIT = 15.0          # matches SENSOR_PROFILES["water_runoff"]["max"] in ecotwin_core.py
RAMP = 0.35           # fraction of the gap closed each tick, so changes look gradual

ZONE = "Downtown Industrial Runoff Basin"


def setup(conn: psycopg.Connection, node_id: int, reset: bool) -> None:
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
        cur.execute("SELECT zone_name FROM sensor_nodes WHERE node_id = %s", (node_id,))
        zone = cur.fetchone()[0]

        if reset:
            cur.execute("DELETE FROM sensor_readings WHERE node_id = %s", (node_id,))
            cur.execute("DELETE FROM environmental_incidents WHERE zone_name = %s", (zone,))
            print(f"Reset: cleared readings for node {node_id} and incidents for '{zone}'.")

        # The detector compares the last 10 minutes against the hour before,
        # so it needs history. Backfill an hour of normal readings if missing.
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
            print("Backfilled 70 minutes of normal baseline readings.")
    conn.commit()


@contextmanager
def keypress_mode():
    """Read single keys without waiting for Enter."""
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
        raise SystemExit("Run this directly in a Terminal window so it can read keypresses.")
    url = os.getenv("TIGER_DATABASE_URL")
    if not url:
        raise SystemExit("TIGER_DATABASE_URL is missing from .env")

    with psycopg.connect(url) as conn:
        setup(conn, node_id, reset)
        print(f"\nStreaming node {node_id} every {interval:g}s.  [s] spike   [n] normal   [q] quit\n")

        state, level = "normal", NORMAL_LEVEL
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
                if key == "s" and state != "spike":
                    state = "spike"
                    print(">>> Contamination event started")
                elif key == "n" and state != "normal":
                    state = "normal"
                    print(">>> Returning to normal")
                if time.monotonic() < next_tick:
                    continue  # a key arrived early; wait for the actual tick
                next_tick += interval

                target = spike_level if state == "spike" else NORMAL_LEVEL
                level += (target - level) * RAMP
                noise = random.gauss(0, 3.0 if state == "spike" else 0.6)
                value = round(max(0.0, level + noise), 2)

                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO sensor_readings (time, node_id, metric_value, raw_payload) "
                        "VALUES (now(), %s, %s, %s)",
                        (node_id, value, Jsonb({"source": "simulator", "state": state})),
                    )
                conn.commit()

                bar = "#" * min(40, int(value / 2))
                flag = "  !! over limit" if value > LIMIT else ""
                stamp = datetime.now().strftime("%H:%M:%S")
                print(f"{stamp}  {state.upper():7} {value:6.1f} NTU  {bar}{flag}")

    print("Simulator stopped.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="EcoTwin AI live sensor simulator")
    ap.add_argument("--node", type=int, default=402)
    ap.add_argument("--interval", type=float, default=2.0, help="seconds between readings")
    ap.add_argument("--spike-level", type=float, default=80.0, help="NTU the spike ramps toward")
    ap.add_argument("--spike-after", type=float, default=None,
                    help="start the spike automatically after this many seconds (no keypress needed)")
    ap.add_argument("--reset", action="store_true",
                    help="clear this node's readings and its zone's incidents first (clean demo)")
    args = ap.parse_args()
    run(args.node, args.interval, args.spike_level, args.reset, args.spike_after)
