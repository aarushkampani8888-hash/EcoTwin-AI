"""
EcoTwin AI: long-term incident memory via Backboard (https://backboard.io).

Every incident (and its resolution) is written to a Backboard assistant's
memory, tagged with its node. Before Gemini reasons about a new anomaly,
EcoTwin recalls that node's past incidents so it can tell a one-off event
from a systemic pattern. Backboard memory survives restarts and database
resets, so the agent keeps "knowing" a node's track record.

If BACKBOARD_API_KEY isn't set or Backboard is unreachable, callers fall back
to the incident history stored in Tiger Data, so the pipeline never blocks.

REST API used (base https://app.backboard.io/api, header X-API-Key):
    POST /assistants                                 create the EcoTwin memory assistant (once)
    POST /assistants/{assistant_id}/memories         add a memory {content, metadata}
    POST /assistants/{assistant_id}/memories/search  search memories {query, limit}
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger("ecotwin.memory")

BASE_URL = os.getenv("BACKBOARD_BASE_URL", "https://app.backboard.io/api")
API_KEY = os.getenv("BACKBOARD_API_KEY", "").strip()
TIMEOUT = float(os.getenv("BACKBOARD_TIMEOUT", "4"))  # seconds; memory must never stall an alert
ID_FILE = Path(__file__).with_name(".backboard_assistant_id")

# One line per incident. A fixed format means we can read records back reliably.
RECORD_RE = re.compile(r"EcoTwin incident record \| (.+)")


def _fields(text: str) -> dict:
    m = RECORD_RE.search(text or "")
    if not m:
        return {}
    out = {}
    for part in m.group(1).split(" | "):
        key, sep, value = part.partition("=")
        if sep:
            out[key.strip()] = value.strip()
    return out


class BackboardMemory:
    def __init__(self) -> None:
        self.enabled = bool(API_KEY)
        saved = ID_FILE.read_text().strip() if ID_FILE.exists() else ""
        self.assistant_id = os.getenv("BACKBOARD_ASSISTANT_ID", "").strip() or saved

    # -- plumbing ---------------------------------------------------------
    def _post(self, path: str, body: dict) -> dict:
        resp = requests.post(
            f"{BASE_URL}{path}",
            headers={"X-API-Key": API_KEY, "Content-Type": "application/json"},
            json=body,
            timeout=TIMEOUT,
        )
        if not resp.ok:
            raise RuntimeError(f"Backboard {resp.status_code} on {path}: {resp.text[:300]}")
        return resp.json() if resp.content else {}

    def ensure_assistant(self) -> None:
        """Create the EcoTwin memory assistant on first run and remember its id locally."""
        if not self.enabled or self.assistant_id:
            return
        data = self._post("/assistants", {
            "name": "EcoTwin AI incident memory",
            "system_prompt": "Long-term memory of environmental incidents per sensor node for EcoTwin AI.",
        })
        self.assistant_id = data["assistant_id"]
        ID_FILE.write_text(self.assistant_id)
        log.info("Created Backboard assistant %s (saved to %s)", self.assistant_id, ID_FILE.name)

    @property
    def ready(self) -> bool:
        return self.enabled and bool(self.assistant_id)

    # -- write ------------------------------------------------------------
    def remember_incident(self, *, node_id: int, zone: str, incident_id: int, detected_at: datetime,
                          severity: str, cause: str, classification: str) -> None:
        if not self.ready:
            return
        content = (
            "EcoTwin incident record | type=incident | node={node} | zone={zone} | incident={iid} | "
            "date={date} | severity={sev} | pattern={cls} | cause={cause}"
        ).format(node=node_id, zone=zone, iid=incident_id, date=detected_at.astimezone(timezone.utc).isoformat(),
                 sev=severity, cls=classification, cause=(cause or "").replace("|", "/")[:200])
        self._post(f"/assistants/{self.assistant_id}/memories", {
            "content": content,
            "metadata": {"source": "ecotwin", "type": "incident", "node_id": node_id,
                         "incident_id": incident_id, "severity": severity, "pattern": classification},
        })

    def remember_resolution(self, *, node_id: int, zone: str, incident_id: int, minutes_open: float) -> None:
        if not self.ready:
            return
        content = (
            f"EcoTwin incident record | type=resolution | node={node_id} | zone={zone} | "
            f"incident={incident_id} | date={datetime.now(timezone.utc).isoformat()} | "
            f"resolved_after_min={minutes_open:.0f}"
        )
        self._post(f"/assistants/{self.assistant_id}/memories", {
            "content": content,
            "metadata": {"source": "ecotwin", "type": "resolution", "node_id": node_id, "incident_id": incident_id},
        })

    # -- read -------------------------------------------------------------
    def node_history(self, node_id: int, zone: str, since: datetime) -> list[dict]:
        """Past incidents for one node since `since`, newest first."""
        if not self.ready:
            return []
        data = self._post(f"/assistants/{self.assistant_id}/memories/search", {
            "query": f"EcoTwin incident record node={node_id} zone={zone}",
            "limit": 50,
        })
        incidents: dict[str, dict] = {}
        for mem in data.get("memories", []):
            f = _fields(mem.get("content", ""))
            if f.get("type") != "incident" or f.get("node") != str(node_id):
                continue  # semantic search can return neighbours; keep exact node matches only
            try:
                when = datetime.fromisoformat(f["date"])
            except (KeyError, ValueError):
                continue
            if when >= since:
                incidents[f.get("incident", f["date"])] = {
                    "incident_id": int(f["incident"]) if f.get("incident", "").isdigit() else None,
                    "detected_at": when,
                    "severity": f.get("severity"),
                    "cause": f.get("cause"),
                    "source": "backboard",
                }
        return sorted(incidents.values(), key=lambda r: r["detected_at"], reverse=True)
