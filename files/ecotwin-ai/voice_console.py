"""
EcoTwin AI: voice command console.

Ask EcoTwin about the city in plain language ("what's the status of the
Downtown Industrial Basin, and do we have field crews nearby?") and get a
short spoken answer grounded in live Tiger Data.

Flow:
  1. The browser turns speech into text (Web Speech API), or you type.
  2. build_snapshot() reads a fixed, read-only picture of the system from
     Tiger Data: every node's latest reading vs its limit, open incidents,
     recent history, response resources, pattern memory and bounties.
  3. Gemini answers ONLY from that snapshot (no AI-written SQL, nothing invented).
  4. The dashboard has ElevenLabs speak the answer.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Callable

import requests

from ecotwin_core import (
    GEMINI_API_KEY,
    GEMINI_MODEL,
    GEMINI_THINKING_LEVEL,
    GEMINI_URL,
    SENSOR_PROFILES,
    normalize_resources,
)

log = logging.getLogger("ecotwin.console")

Query = Callable[..., list[dict]]

NODES_SQL = """
SELECT n.node_id, n.zone_name, n.sensor_type, n.latitude, n.longitude, n.response_resources,
       lr.metric_value AS latest_value,
       round((extract(epoch FROM now() - lr.time) / 60)::numeric, 1) AS latest_minutes_ago,
       (SELECT round(avg(r.metric_value)::numeric, 1) FROM sensor_readings r
         WHERE r.node_id = n.node_id AND r.time > now() - interval '10 minutes') AS avg_last_10_min,
       (SELECT round(avg(r.metric_value)::numeric, 1) FROM sensor_readings r
         WHERE r.node_id = n.node_id AND r.time > now() - interval '70 minutes'
           AND r.time <= now() - interval '10 minutes') AS baseline_prior_hour
FROM sensor_nodes n
LEFT JOIN LATERAL (SELECT metric_value, time FROM sensor_readings r
                   WHERE r.node_id = n.node_id ORDER BY time DESC LIMIT 1) lr ON true
ORDER BY n.node_id;
"""

INCIDENTS_SQL = """
SELECT i.incident_id, i.node_id, i.zone_name, i.severity_level AS severity, i.status, i.classification,
       round((extract(epoch FROM now() - i.detected_at) / 60)::numeric, 0) AS minutes_ago,
       round((extract(epoch FROM i.resolved_at - i.detected_at) / 60)::numeric, 0) AS minutes_to_resolve,
       i.ai_response->>'root_cause_hypothesis' AS root_cause,
       i.ai_response->>'action_plan' AS action_plan,
       i.ai_response->>'pattern_rationale' AS pattern_rationale,
       b.amount_sol AS bounty_sol, b.network AS bounty_network, b.status AS bounty_status
FROM environmental_incidents i
LEFT JOIN bounty_payouts b USING (incident_id)
ORDER BY i.detected_at DESC
LIMIT 12;
"""

MEMORY_SQL = """
SELECT node_id, count(*) AS incidents_last_30_days
FROM agent_memory
WHERE kind = 'incident' AND occurred_at > now() - interval '30 days'
GROUP BY node_id;
"""


def _num(v):
    return float(v) if v is not None else None


def build_snapshot(query: Query) -> dict:
    nodes = []
    for n in query(NODES_SQL):
        prof = SENSOR_PROFILES.get(n["sensor_type"], {"label": n["sensor_type"], "unit": "", "max": None})
        latest = _num(n["latest_value"])
        nodes.append({
            "node_id": n["node_id"],
            "zone": n["zone_name"],
            "metric": prof["label"],
            "unit": prof["unit"],
            "safety_limit": prof["max"],
            "latest_reading": round(latest, 1) if latest is not None else None,
            "latest_reading_minutes_ago": _num(n["latest_minutes_ago"]),
            "avg_last_10_min": _num(n["avg_last_10_min"]),
            "baseline_prior_hour": _num(n["baseline_prior_hour"]),
            "over_limit_now": (latest is not None and prof["max"] is not None and latest > prof["max"]),
            "response_resources_on_file": normalize_resources(n["response_resources"]),
        })

    incidents = []
    for i in query(INCIDENTS_SQL):
        incidents.append({k: (_num(v) if k in ("minutes_ago", "minutes_to_resolve", "bounty_sol") else v)
                          for k, v in i.items()})

    try:
        memory = {r["node_id"]: r["incidents_last_30_days"] for r in query(MEMORY_SQL)}
    except Exception:  # memory table not created yet
        memory = {}
    for n in nodes:
        n["incidents_remembered_last_30_days"] = memory.get(n["node_id"], 0)

    return {
        "as_of": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z"),
        "nodes": nodes,
        "recent_incidents": incidents,
        "notes": [
            "Crew GPS locations and travel times are NOT tracked; only which response resources are on file per zone.",
            "Incident status: OPEN (active), RESOLVED (readings safe for the resolution window), "
            "SUPERSEDED (replaced by a newer event before resolving).",
            "Bounties: network 'mock' = simulated record; 'devnet' = real Solana test-network transaction.",
        ],
    }


PROMPT = """You are EcoTwin, the voice of a city's environmental monitoring console, answering the operator
out loud during a live briefing.

LIVE DATA (read from Tiger Data seconds ago):
{snapshot}

RECENT CONVERSATION (oldest first, may be empty):
{history}

OPERATOR JUST ASKED: "{question}"

Rules:
- Answer ONLY from LIVE DATA. Never invent readings, incidents, crews, locations, distances or times.
- If the data can't answer part of the question, say so in a few words and give what you do know.
  For example, crew locations aren't tracked: say which crews are on file for the zone, never "nearby" or a distance.
- Match zone names loosely ("the downtown basin" = "Downtown Industrial Runoff Basin").
- Lead with the direct answer. Mention the reading vs its limit with units when status is asked.
- This will be spoken aloud: 2 to 3 short sentences, at most 60 words, natural and calm-but-urgent when
  something is wrong. No lists, no markdown, no emojis. Round numbers sensibly.
- 'node_ids': the node ids your answer is about (empty if none).
"""

RESPONSE_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "answer": {"type": "STRING"},
        "node_ids": {"type": "ARRAY", "items": {"type": "INTEGER"}},
    },
    "required": ["answer", "node_ids"],
}


def answer(question: str, snapshot: dict, history: list[dict] | None = None) -> dict:
    turns = "\n".join(f"Operator: {h.get('q', '')}\nEcoTwin: {h.get('a', '')}" for h in (history or [])[-3:])
    body = {
        "contents": [{"role": "user", "parts": [{"text": PROMPT.format(
            snapshot=json.dumps(snapshot, default=str, indent=1),
            history=turns or "(none)",
            question=question.replace('"', "'"),
        )}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": RESPONSE_SCHEMA,
            "temperature": 0.3,
            **({"thinkingConfig": {"thinkingLevel": GEMINI_THINKING_LEVEL}} if GEMINI_THINKING_LEVEL else {}),
        },
    }
    t0 = time.time()
    resp = requests.post(
        GEMINI_URL.format(model=GEMINI_MODEL),
        headers={"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"Gemini {resp.status_code}: {resp.text[:300]}")
    candidates = resp.json().get("candidates") or []
    if not candidates:
        raise RuntimeError("Gemini returned no answer")
    out = json.loads(candidates[0]["content"]["parts"][0]["text"])
    out["gemini_s"] = round(time.time() - t0, 1)
    log.info("Console: %r -> %r (%.1fs)", question, out.get("answer"), out["gemini_s"])
    return out
