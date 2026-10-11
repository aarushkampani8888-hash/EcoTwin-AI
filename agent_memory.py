"""
EcoTwin AI: long-term agent memory stored in Tiger Data.

A dedicated `agent_memory` table holds one row per incident and per
resolution, tagged by node. It is deliberately separate from
environmental_incidents: resetting the demo (simulate.py --reset) clears the
incident log, but the agent keeps remembering each node's track record,
which is what lets it call out repeat offenders as systemic patterns.

Same interface as backboard_memory.BackboardMemory, so either can be used.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

log = logging.getLogger("ecotwin.memory")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS agent_memory (
    memory_id      BIGSERIAL PRIMARY KEY,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    node_id        INT NOT NULL,
    zone_name      TEXT,
    kind           TEXT NOT NULL,          -- 'incident' or 'resolution'
    incident_id    INT,
    severity       TEXT,
    classification TEXT,
    content        TEXT NOT NULL,          -- human-readable memory line
    metadata       JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS agent_memory_node_time ON agent_memory (node_id, occurred_at DESC);
"""


class TigerMemory:
    name = "agent-memory"
    enabled = True

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._conn: psycopg.Connection | None = None

    @property
    def ready(self) -> bool:
        return True

    def _db(self) -> psycopg.Connection:
        # Own autocommit connection, so memory writes never tangle with the
        # detection loop's transactions.
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self.database_url, autocommit=True, connect_timeout=5)
        return self._conn

    def ensure_assistant(self) -> None:
        with self._db().cursor() as cur:
            cur.execute(SCHEMA_SQL)

    # -- write ------------------------------------------------------------
    def remember_incident(self, *, node_id: int, zone: str, incident_id: int, detected_at: datetime,
                          severity: str, cause: str, classification: str) -> None:
        content = (f"Incident #{incident_id} at node {node_id} ({zone}): {severity}, "
                   f"{classification.lower()}. Cause: {cause or 'not recorded'}")
        with self._db().cursor() as cur:
            cur.execute(
                """INSERT INTO agent_memory
                   (occurred_at, node_id, zone_name, kind, incident_id, severity, classification, content, metadata)
                   VALUES (%s, %s, %s, 'incident', %s, %s, %s, %s, %s)""",
                (detected_at, node_id, zone, incident_id, severity, classification, content,
                 Jsonb({"cause": cause})),
            )

    def remember_resolution(self, *, node_id: int, zone: str, incident_id: int, minutes_open: float) -> None:
        content = f"Incident #{incident_id} at node {node_id} resolved after {minutes_open:.0f} min at safe levels"
        with self._db().cursor() as cur:
            cur.execute(
                """INSERT INTO agent_memory (node_id, zone_name, kind, incident_id, content, metadata)
                   VALUES (%s, %s, 'resolution', %s, %s, %s)""",
                (node_id, zone, incident_id, content, Jsonb({"minutes_open": round(minutes_open, 1)})),
            )

    # -- read -------------------------------------------------------------
    def node_history(self, node_id: int, zone: str, since: datetime) -> list[dict]:
        with self._db().cursor(row_factory=dict_row) as cur:
            cur.execute(
                """SELECT m.incident_id, m.occurred_at, m.severity, m.metadata->>'cause' AS cause,
                          EXISTS (SELECT 1 FROM agent_memory r WHERE r.kind = 'resolution'
                                  AND r.node_id = m.node_id AND r.incident_id = m.incident_id) AS resolved
                   FROM agent_memory m
                   WHERE m.kind = 'incident' AND m.node_id = %s AND m.occurred_at >= %s
                   ORDER BY m.occurred_at DESC LIMIT 20""",
                (node_id, since),
            )
            rows = cur.fetchall()
        return [{"incident_id": r["incident_id"], "detected_at": r["occurred_at"], "severity": r["severity"],
                 "cause": r["cause"], "status": "RESOLVED" if r["resolved"] else None, "source": self.name}
                for r in rows]

    def forget(self, node_id: int | None = None) -> int:
        """Wipe memory for one node (or all nodes). Used to start a demo from a clean slate."""
        with self._db().cursor() as cur:
            if node_id is None:
                cur.execute("DELETE FROM agent_memory")
            else:
                cur.execute("DELETE FROM agent_memory WHERE node_id = %s", (node_id,))
            return cur.rowcount
