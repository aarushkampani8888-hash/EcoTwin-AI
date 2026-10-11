"""
EcoTwin AI: core integration module.

Pipeline:
    Tiger Data hypertable  ->  sliding-window anomaly check
        ->  Gemini multimodal reasoning (sensor slice + camera frame)
        ->  ElevenLabs voice broadcast from `elevenlabs_broadcast_script`
        ->  audit row in environmental_incidents

Incident lifecycle (status column):
    OPEN        created when a zone first crosses its limit
    RESOLVED    readings stayed at safe levels for RESOLUTION_WINDOW -> mock Solana bounty
    SUPERSEDED  a new event hit the same node before this one resolved (no bounty)

Memory: past incidents per node come from the agent_memory table in Tiger Data
(agent_memory.py), or from Backboard if BACKBOARD_API_KEY is set, merged with the
incident log. 2+ prior incidents in PATTERN_WINDOW = SYSTEMIC pattern.

Run:
    python ecotwin_core.py            # poll forever
    python ecotwin_core.py --once     # one detection pass, then exit
    python ecotwin_core.py --dry-run  # detect + build payloads, skip Gemini/ElevenLabs
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import json
import logging
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

load_dotenv()  # before importing modules that read env vars at import time

from agent_memory import TigerMemory  # noqa: E402
from backboard_memory import API_KEY as BACKBOARD_API_KEY, BackboardMemory  # noqa: E402
from bounty import check_confirmation, issue_resolution_bounty  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ecotwin")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("TIGER_DATABASE_URL", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
# gemini-1.5-pro has been retired; set a model your key can access.
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
# Optional latency knob for Gemini models that "think" before answering, e.g. "low".
GEMINI_THINKING_LEVEL = os.getenv("GEMINI_THINKING_LEVEL", "").strip()
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "")
ELEVENLABS_MODEL_ID = os.getenv("ELEVENLABS_MODEL_ID", "eleven_flash_v2_5")  # low-latency voice model

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
RECENT_WINDOW = os.getenv("RECENT_WINDOW", "10 minutes")      # the "now" window
BASELINE_WINDOW = os.getenv("BASELINE_WINDOW", "1 hour")      # what "normal" looks like
INCIDENT_COOLDOWN = os.getenv("INCIDENT_COOLDOWN", "15 minutes")
FRAMES_DIR = Path(os.getenv("FRAMES_DIR", "frames"))
AUDIO_DIR = Path(os.getenv("AUDIO_DIR", "audio"))
AUTOPLAY = os.getenv("AUTOPLAY", "true").lower() in ("1", "true", "yes")

# Closed loop: an OPEN incident is RESOLVED after this long at safe levels.
RESOLUTION_WINDOW = os.getenv("RESOLUTION_WINDOW", "10 minutes")
# Pattern memory: this many earlier incidents at a node within the window = SYSTEMIC.
PATTERN_WINDOW_DAYS = int(os.getenv("PATTERN_WINDOW_DAYS", "30"))
PATTERN_MIN_PRIOR = int(os.getenv("PATTERN_MIN_PRIOR", "2"))

Z_SCORE_LIMIT = 3.0       # recent avg is 3 std devs above baseline
SPIKE_PCT_LIMIT = 200.0   # recent avg is 3x baseline (a +200% change)
MIN_BASELINE_POINTS = 10  # don't trust stats from a near-empty baseline

# Absolute limits per sensor_type. The schema has no units/threshold columns,
# so they live here for now.
SENSOR_PROFILES = {
    "water_runoff": {"label": "Water Turbidity", "unit": "NTU", "max": 15.0},
    "air_quality": {"label": "PM2.5", "unit": "ug/m3", "max": 35.0},
    "grid_load": {"label": "Grid Load", "unit": "% capacity", "max": 90.0},
}

# ---------------------------------------------------------------------------
# 1. Tiger Data: sliding-window anomaly detection
# ---------------------------------------------------------------------------
# Compares each node's last RECENT_WINDOW against the BASELINE_WINDOW right
# before it. Both windows slide forward every poll because they are anchored
# to now(). last() is a TimescaleDB hyperfunction.
ANOMALY_SQL = """
WITH recent AS (
    SELECT node_id,
           avg(metric_value)        AS recent_avg,
           max(metric_value)        AS recent_peak,
           last(metric_value, time) AS latest_value,
           max(time)                AS latest_time,
           count(*)                 AS n_recent
    FROM sensor_readings
    WHERE time > now() - %(recent)s::interval
    GROUP BY node_id
),
baseline AS (
    SELECT node_id,
           avg(metric_value)          AS base_avg,
           stddev_samp(metric_value)  AS base_std,
           count(*)                   AS n_base
    FROM sensor_readings
    WHERE time >  now() - %(recent)s::interval - %(baseline)s::interval
      AND time <= now() - %(recent)s::interval
    GROUP BY node_id
)
SELECT n.node_id, n.zone_name, n.sensor_type, n.latitude, n.longitude, n.response_resources,
       r.latest_value, r.recent_avg, r.recent_peak, r.latest_time, r.n_recent,
       b.base_avg, b.base_std, COALESCE(b.n_base, 0) AS n_base,
       CASE WHEN b.base_avg > 0
            THEN (r.recent_avg - b.base_avg) / b.base_avg * 100 END AS pct_change,
       CASE WHEN b.base_std > 0
            THEN (r.recent_avg - b.base_avg) / b.base_std END       AS z_score
FROM recent r
JOIN sensor_nodes n USING (node_id)
LEFT JOIN baseline b USING (node_id);
"""

# Compact trend slice sent to Gemini: 5-minute buckets over the last hour.
TREND_SQL = """
SELECT time_bucket('5 minutes', time) AS bucket,
       round(avg(metric_value)::numeric, 2) AS avg_value,
       round(max(metric_value)::numeric, 2) AS max_value
FROM sensor_readings
WHERE node_id = %(node_id)s AND time > now() - interval '1 hour'
GROUP BY bucket
ORDER BY bucket;
"""

INSERT_INCIDENT_SQL = """
INSERT INTO environmental_incidents
    (zone_name, severity_level, ai_summary, action_triggered, node_id, ai_response, audio_file, frame_file,
     status, classification)
VALUES (%(zone)s, %(severity)s, %(summary)s, %(action)s, %(node_id)s, %(ai_response)s, %(audio)s, %(frame)s,
        'OPEN', %(classification)s)
RETURNING incident_id, detected_at;
"""

# Extra columns and tables EcoTwin and the dashboard use. Safe to run every startup.
# Older incidents keep status NULL, so they never get resolved or paid retroactively.
SCHEMA_UPGRADE_SQL = """
ALTER TABLE environmental_incidents
    ADD COLUMN IF NOT EXISTS node_id INT,
    ADD COLUMN IF NOT EXISTS ai_response JSONB,
    ADD COLUMN IF NOT EXISTS audio_file TEXT,
    ADD COLUMN IF NOT EXISTS frame_file TEXT,
    ADD COLUMN IF NOT EXISTS status TEXT,
    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS classification TEXT;
CREATE TABLE IF NOT EXISTS bounty_payouts (
    payout_id   SERIAL PRIMARY KEY,
    incident_id INT UNIQUE REFERENCES environmental_incidents(incident_id) ON DELETE CASCADE,
    node_id     INT,
    recipient   TEXT,
    amount_sol  NUMERIC(12, 4),
    network     TEXT,
    signature   TEXT,
    transaction JSONB,
    created_at  TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE bounty_payouts
    ADD COLUMN IF NOT EXISTS status TEXT,
    ADD COLUMN IF NOT EXISTS explorer_url TEXT;
"""


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_UPGRADE_SQL)
    conn.commit()


@dataclass
class Anomaly:
    node_id: int
    zone_name: str
    sensor_type: str
    latitude: float
    longitude: float
    latest_value: float
    recent_avg: float
    recent_peak: float
    base_avg: float | None
    pct_change: float | None
    z_score: float | None
    reasons: list[str] = field(default_factory=list)
    trend: list[dict] = field(default_factory=list)
    resources: list[str] = field(default_factory=list)

    @property
    def profile(self) -> dict:
        return SENSOR_PROFILES.get(
            self.sensor_type, {"label": self.sensor_type, "unit": "", "max": None}
        )


def evaluate_breach(row: dict) -> list[str]:
    """Return the reasons a window row counts as an anomaly (empty list = normal)."""
    reasons = []
    limit = SENSOR_PROFILES.get(row["sensor_type"], {}).get("max")
    if limit is not None and row["latest_value"] > limit:
        reasons.append(f"latest {row['latest_value']:.1f} exceeds absolute limit {limit}")

    trusted = row["n_base"] >= MIN_BASELINE_POINTS
    if trusted and row["z_score"] is not None and row["z_score"] >= Z_SCORE_LIMIT:
        reasons.append(f"z-score {row['z_score']:.1f} vs baseline")
    if trusted and row["pct_change"] is not None and row["pct_change"] >= SPIKE_PCT_LIMIT:
        reasons.append(f"{row['pct_change']:.0f}% above baseline average")
    return reasons


DEFAULT_RESOURCES = ["Public alert broadcast system"]


def normalize_resources(value) -> list[str]:
    """response_resources may be TEXT[] (list) or plain TEXT; fall back to a safe default."""
    if not value:
        return list(DEFAULT_RESOURCES)
    if isinstance(value, str):
        value = value.strip("{}").split(",")
    return [str(v).strip().strip('"') for v in value if str(v).strip()]


def detect_anomalies(conn: psycopg.Connection) -> list[Anomaly]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(ANOMALY_SQL, {"recent": RECENT_WINDOW, "baseline": BASELINE_WINDOW})
        rows = cur.fetchall()

    anomalies = []
    for row in rows:
        reasons = evaluate_breach(row)
        if not reasons:
            continue
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(TREND_SQL, {"node_id": row["node_id"]})
            trend = [
                {"t": r["bucket"].isoformat(), "avg": float(r["avg_value"]), "max": float(r["max_value"])}
                for r in cur.fetchall()
            ]
        anomalies.append(
            Anomaly(
                node_id=row["node_id"],
                zone_name=row["zone_name"],
                sensor_type=row["sensor_type"],
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
                latest_value=row["latest_value"],
                recent_avg=row["recent_avg"],
                recent_peak=row["recent_peak"],
                base_avg=row["base_avg"],
                pct_change=row["pct_change"],
                z_score=row["z_score"],
                reasons=reasons,
                trend=trend,
                resources=normalize_resources(row.get("response_resources")),
            )
        )
    return anomalies


def has_open_incident(conn: psycopg.Connection, node_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM environmental_incidents WHERE node_id = %s AND status = 'OPEN' LIMIT 1", (node_id,)
        )
        return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# Incident memory: agent_memory table in Tiger Data (or Backboard, if a key is set),
# merged with the incident log
# ---------------------------------------------------------------------------
memory = BackboardMemory() if BACKBOARD_API_KEY else TigerMemory(DATABASE_URL)
if isinstance(memory, BackboardMemory):
    memory.name = "backboard"

DB_HISTORY_SQL = """
SELECT incident_id, detected_at, severity_level, ai_response->>'root_cause_hypothesis' AS cause, status
FROM environmental_incidents
WHERE node_id = %(node)s AND detected_at > now() - make_interval(days => %(days)s)
ORDER BY detected_at DESC LIMIT 20;
"""


def node_history(conn: psycopg.Connection, a: "Anomaly") -> tuple[list[dict], str]:
    """Past incidents at this node (newest first) and where they came from."""
    since = datetime.now(timezone.utc) - timedelta(days=PATTERN_WINDOW_DAYS)
    merged: dict[str, dict] = {}
    sources = []
    if memory.ready:
        try:
            for r in memory.node_history(a.node_id, a.zone_name, since):
                merged[str(r["incident_id"] or r["detected_at"])] = r
            sources.append(memory.name)
        except Exception as exc:  # memory is helpful, never critical
            log.warning("Memory recall failed (%s), using the incident log only: %s", memory.name, exc)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(DB_HISTORY_SQL, {"node": a.node_id, "days": PATTERN_WINDOW_DAYS})
        for r in cur.fetchall():
            key = str(r["incident_id"])
            entry = merged.setdefault(key, {"incident_id": r["incident_id"], "detected_at": r["detected_at"],
                                            "severity": r["severity_level"], "cause": r["cause"]})
            entry["status"] = r["status"]
    sources.append("incident-log")
    history = sorted(merged.values(), key=lambda r: r["detected_at"], reverse=True)
    return history, "+".join(sources)


def classify(history: list[dict]) -> str:
    return "SYSTEMIC" if len(history) >= PATTERN_MIN_PRIOR else "ISOLATED"


# ---------------------------------------------------------------------------
# 2. Gemini: multimodal reasoning
# ---------------------------------------------------------------------------
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

# responseSchema makes Gemini return exactly these keys instead of us hoping
# the prompt is followed.
GEMINI_RESPONSE_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "hazard_detected": {"type": "BOOLEAN"},
        "severity": {"type": "STRING", "enum": ["LOW", "MODERATE", "CRITICAL"]},
        "root_cause_hypothesis": {"type": "STRING"},
        "action_plan": {"type": "STRING"},
        "elevenlabs_broadcast_script": {"type": "STRING"},
        "pattern_rationale": {"type": "STRING"},
    },
    "required": [
        "hazard_detected", "severity", "root_cause_hypothesis",
        "action_plan", "elevenlabs_broadcast_script", "pattern_rationale",
    ],
}


FRAME_TYPES = ((".jpg", "image/jpeg"), (".jpeg", "image/jpeg"), (".png", "image/png"))


def find_frame(node_id: int) -> tuple[Path, str] | None:
    """Latest camera/drone frame for a node. Swap this for your live feed grabber."""
    for ext, mime in FRAME_TYPES:
        path = FRAMES_DIR / f"{node_id}{ext}"
        if path.exists():
            return path, mime
    return None


def load_frame(node_id: int) -> tuple[bytes, str] | None:
    found = find_frame(node_id)
    return (found[0].read_bytes(), found[1]) if found else None


def history_block(history: list[dict], classification: str) -> str:
    if not history:
        lines = "- No prior incidents recorded at this node."
    else:
        lines = "\n".join(
            f"- {h['detected_at'].astimezone(timezone.utc):%Y-%m-%d %H:%M} UTC · {h.get('severity') or '?'}"
            f" · {h.get('cause') or 'cause not recorded'}" + (f" · {h['status']}" if h.get("status") else "")
            for h in history[:8]
        )
    if classification == "SYSTEMIC":
        rule = (f"- This node has {len(history)} prior incidents in {PATTERN_WINDOW_DAYS} days: treat this as a "
                "SYSTEMIC PATTERN, not an isolated event. The action plan must address the recurring source "
                "(e.g. escalate for source investigation or enforcement), still using only the listed resources.\n"
                "- 'pattern_rationale': one sentence on what the history suggests is recurring.\n")
    else:
        rule = ("- Classification: ISOLATED EVENT (too few prior incidents to call it a pattern).\n"
                "- 'pattern_rationale': one sentence on why this looks like a one-off, or what to watch for.\n")
    return f"INCIDENT HISTORY FOR THIS NODE (agent memory, last {PATTERN_WINDOW_DAYS} days):\n{lines}\n{rule}\n"


def build_prompt(a: Anomaly, has_frame: bool, history: list[dict] | None = None,
                 classification: str = "ISOLATED") -> str:
    p = a.profile
    baseline = f"{a.base_avg:.1f} {p['unit']}" if a.base_avg is not None else "insufficient history"
    limit = f"< {p['max']} {p['unit']}" if p["max"] is not None else "not configured"
    change = f"{a.pct_change:+.0f}%" if a.pct_change is not None else "n/a"
    visual = (
        "(2) a live camera frame of the affected urban zone"
        if has_frame
        else "(2) NO camera frame is available, so reason from sensor data only and lower confidence accordingly"
    )
    step1 = (
        "1. Cross-reference the visual frame with the numeric spike.\n"
        if has_frame
        else "1. Judge the numeric spike and trend on their own; do not describe an image.\n"
    )
    return (
        "SYSTEM ROLE: You are EcoTwin AI, an autonomous municipal environmental governance agent. "
        "Analyze the provided multimodal inputs consisting of (1) real-time time-series sensor anomalies "
        f"from Tiger Data and {visual}. Determine if this represents an active pollution or ecological hazard.\n\n"
        "CONTEXT DATA FROM TIGER DATA:\n"
        f"- Node ID: {a.node_id}\n"
        f"- Zone: {a.zone_name} ({a.latitude}, {a.longitude})\n"
        f"- Metric Type: {p['label']}\n"
        f"- Current Reading: {a.latest_value:.1f} {p['unit']} (Baseline threshold is {limit})\n"
        f"- Last {RECENT_WINDOW} average: {a.recent_avg:.1f}, peak {a.recent_peak:.1f}\n"
        f"- Prior {BASELINE_WINDOW} baseline average: {baseline} (change {change})\n"
        f"- Detector triggers: {'; '.join(a.reasons)}\n"
        f"- 5-minute buckets, last hour: {json.dumps(a.trend)}\n\n"
        f"{history_block(history or [], classification)}"
        "CONSTRAINTS:\n"
        f"- The ONLY response resources available in this zone are: {'; '.join(a.resources)}.\n"
        "- Build 'action_plan' exclusively from those resources. Do NOT invent infrastructure, "
        "equipment, or teams that are not listed (e.g. automated valves, containment gates, drones).\n"
        "- If the situation needs something beyond them, say 'escalate to <agency>' instead of assuming it exists.\n\n"
        "Instructions:\n"
        f"{step1}"
        "2. Respond with JSON containing:\n"
        "   - 'hazard_detected': boolean\n"
        "   - 'severity': 'LOW', 'MODERATE', or 'CRITICAL'\n"
        "   - 'root_cause_hypothesis': short string\n"
        "   - 'action_plan': brief steps for municipal response\n"
        "   - 'elevenlabs_broadcast_script': a concise, urgent, natural-sounding voice script "
        "(max 2 sentences) warning residents in this specific zone to avoid the area. "
        "If no hazard is detected, return an empty string.\n"
        "   - 'pattern_rationale': as described in the incident history section"
    )


def build_gemini_request(a: Anomaly, history: list[dict] | None = None, classification: str = "ISOLATED") -> dict:
    frame = load_frame(a.node_id)
    parts: list[dict] = [{"text": build_prompt(a, has_frame=frame is not None,
                                               history=history, classification=classification)}]
    if frame:
        data, mime = frame
        parts.append({"inline_data": {"mime_type": mime, "data": base64.b64encode(data).decode()}})
    return {
        "contents": [{"role": "user", "parts": parts}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": GEMINI_RESPONSE_SCHEMA,
            "temperature": 0.2,
            **({"thinkingConfig": {"thinkingLevel": GEMINI_THINKING_LEVEL}} if GEMINI_THINKING_LEVEL else {}),
        },
    }


def call_gemini(body: dict) -> dict:
    resp = requests.post(
        GEMINI_URL.format(model=GEMINI_MODEL),
        headers={"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"},
        json=body,
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"Gemini {resp.status_code} for model '{GEMINI_MODEL}': {resp.text[:500]}")
    payload = resp.json()
    candidates = payload.get("candidates") or []
    if not candidates:
        raise RuntimeError(f"Gemini returned no candidates: {payload.get('promptFeedback')}")
    text = candidates[0]["content"]["parts"][0]["text"]
    return json.loads(text)


# ---------------------------------------------------------------------------
# 3. ElevenLabs: voice broadcast
# ---------------------------------------------------------------------------
ELEVENLABS_URL = "https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"


def synthesize_broadcast(script: str, out_path: Path) -> Path:
    resp = requests.post(
        ELEVENLABS_URL.format(voice_id=ELEVENLABS_VOICE_ID),
        params={"output_format": "mp3_44100_128"},
        headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
        json={"text": script, "model_id": ELEVENLABS_MODEL_ID},
        timeout=60,
    )
    resp.raise_for_status()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(resp.content)
    return out_path


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
_player: subprocess.Popen | None = None


def play_audio(path: Path) -> None:
    """Play the broadcast out loud without blocking the detection loop."""
    global _player
    if not AUTOPLAY:
        return
    if _player is not None and _player.poll() is None:
        _player.terminate()  # never let two warnings talk over each other
    player = shutil.which("afplay")  # built into macOS
    if player is None:
        log.warning("Autoplay skipped: 'afplay' not found (it ships with macOS)")
        return
    _player = subprocess.Popen([player, str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def record_incident(
    conn: psycopg.Connection,
    a: "Anomaly",
    verdict: dict,
    action: bool,
    audio_file: str | None = None,
    frame_file: str | None = None,
    classification: str = "ISOLATED",
) -> tuple[int, datetime]:
    zone = a.zone_name
    summary = (
        f"[{verdict.get('severity')}] {verdict.get('root_cause_hypothesis')}\n"
        f"Action plan: {verdict.get('action_plan')}\n"
        f"Broadcast: {verdict.get('elevenlabs_broadcast_script') or '(none)'}"
    )
    with conn.cursor() as cur:
        cur.execute(
            INSERT_INCIDENT_SQL,
            {
                "zone": zone,
                "severity": verdict.get("severity"),
                "summary": summary,
                "action": action,
                "node_id": a.node_id,
                "ai_response": Jsonb({**verdict, "detector_triggers": a.reasons,
                                      "reading_at_detection": a.latest_value}),
                "audio": audio_file,
                "frame": frame_file,
                "classification": classification,
            },
        )
        incident_id, detected_at = cur.fetchone()
    conn.commit()
    return incident_id, detected_at


# Alerting is edge-triggered: one alert when a zone goes normal -> anomalous,
# then silence until its readings recover. _alerted holds zones mid-episode.
_alerted: set[str] = set()
_seen_zones: set[str] = set()

BREACH_START_SQL = """
SELECT min(time) AS breach_at FROM sensor_readings
WHERE node_id = %(node)s AND metric_value > %(limit)s
  AND time > COALESCE((SELECT max(time) FROM sensor_readings
                       WHERE node_id = %(node)s AND metric_value <= %(limit)s), '-infinity');
"""

UPDATE_AUDIO_SQL = """
UPDATE environmental_incidents
SET audio_file = %(audio)s, action_triggered = %(action)s,
    ai_response = ai_response || %(extra)s
WHERE incident_id = %(id)s;
"""


def breach_started_at(conn: psycopg.Connection, a: Anomaly):
    limit = a.profile.get("max")
    if limit is None:
        return None
    with conn.cursor() as cur:
        cur.execute(BREACH_START_SQL, {"node": a.node_id, "limit": limit})
        row = cur.fetchone()
    return row[0] if row else None


RECOVERY_RATIO = 0.9  # hysteresis: "recovered" means back under 90% of the limit


def has_recovered(a: Anomaly | None) -> bool:
    if a is None:
        return True  # no longer anomalous at all
    limit = a.profile.get("max")
    return limit is not None and a.latest_value < limit * RECOVERY_RATIO


def process_anomaly(conn: psycopg.Connection, a: Anomaly, dry_run: bool = False) -> None:
    if a.zone_name in _alerted:
        return  # same ongoing event, already alerted
    limit = a.profile.get("max")
    if limit is not None and a.latest_value <= limit:
        # Statistics still elevated but the live reading is back under the limit
        # (e.g. the tail of an event that already ended). Not a new event.
        return
    if a.zone_name not in _seen_zones:
        _seen_zones.add(a.zone_name)
        # Just restarted mid-event? Don't re-alert an episode alerted before the restart.
        if not dry_run and has_open_incident(conn, a.node_id):
            log.info("Zone %s already alerted before restart; waiting for it to recover", a.zone_name)
            _alerted.add(a.zone_name)
            return
    log.info("Anomaly at node %s (%s): %s", a.node_id, a.zone_name, "; ".join(a.reasons))
    t_detect = time.time()
    breach_at = breach_started_at(conn, a)

    tm = time.time()
    history, memory_source = node_history(conn, a)
    classification = classify(history)
    t_memory = time.time() - tm
    log.info("Memory (%s, %.1fs): %d prior incident(s) at node %s -> %s",
             memory_source, t_memory, len(history), a.node_id, classification)

    body = build_gemini_request(a, history, classification)
    if dry_run:
        preview = json.loads(json.dumps(body))
        for part in preview["contents"][0]["parts"]:
            if "inline_data" in part:
                part["inline_data"]["data"] = f"<{len(part['inline_data']['data'])} base64 chars>"
        print(json.dumps(preview, indent=2))
        return

    t0 = time.time()
    verdict = call_gemini(body)
    t_gemini = time.time() - t0
    log.info("Gemini verdict (%.1fs): %s", t_gemini, verdict)
    _alerted.add(a.zone_name)  # from here on, this event counts as alerted

    detect_lag = max(0.0, t_detect - breach_at.timestamp()) if breach_at else None
    timing = {"breach_at": breach_at.isoformat() if breach_at else None,
              "detect_s": round(detect_lag, 1) if detect_lag is not None else None,
              "memory_s": round(t_memory, 1),
              "gemini_s": round(t_gemini, 1)}

    # A new event at a node that never resolved: close the old one, no bounty.
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE environmental_incidents SET status = 'SUPERSEDED', resolved_at = now() "
            "WHERE node_id = %s AND status = 'OPEN' RETURNING incident_id", (a.node_id,)
        )
        superseded = [r[0] for r in cur.fetchall()]
    if superseded:
        log.info("Superseded open incident(s) %s at node %s", superseded, a.node_id)

    # Log the verdict right away so the dashboard shows the reasoning while the voice renders.
    frame = find_frame(a.node_id)
    pattern = {"classification": classification, "prior_incidents": len(history),
               "memory_source": memory_source, "window_days": PATTERN_WINDOW_DAYS}
    incident_id, detected_at = record_incident(
        conn, a, {**verdict, "timing": timing, "pattern": pattern}, action=False,
        audio_file=None, frame_file=frame[0].name if frame else None, classification=classification,
    )
    log.info("Logged incident #%s (%s)", incident_id, classification)

    script = (verdict.get("elevenlabs_broadcast_script") or "").strip()
    if verdict.get("hazard_detected") and script:
        try:
            t1 = time.time()
            path = synthesize_broadcast(script, AUDIO_DIR / f"node{a.node_id}_{int(time.time() * 1000)}.mp3")
            timing["tts_s"] = round(time.time() - t1, 1)
            if breach_at:
                timing["total_s"] = round(time.time() - breach_at.timestamp(), 1)
            play_audio(path)
            with conn.cursor() as cur:
                cur.execute(UPDATE_AUDIO_SQL, {"audio": path.name, "action": True,
                                               "extra": Jsonb({"timing": timing}), "id": incident_id})
            conn.commit()
            log.info("Broadcast ready: %s", path)
        except requests.HTTPError as exc:
            log.error("ElevenLabs failed: %s", exc)

    log.info(
        "Timing: detected %ss after breach, memory %ss, Gemini %ss, voice %ss, total %ss after breach",
        timing.get("detect_s"), timing.get("memory_s"), timing.get("gemini_s"),
        timing.get("tts_s"), timing.get("total_s"),
    )

    # Write to long-term memory last: it's off the alert's critical path.
    try:
        memory.remember_incident(node_id=a.node_id, zone=a.zone_name, incident_id=incident_id,
                                 detected_at=detected_at, severity=verdict.get("severity"),
                                 cause=verdict.get("root_cause_hypothesis"), classification=classification)
    except Exception as exc:
        log.warning("Backboard write failed (incident still logged in Tiger Data): %s", exc)


# ---------------------------------------------------------------------------
# Closed loop: OPEN -> RESOLVED -> mock Solana bounty
# ---------------------------------------------------------------------------
# An incident resolves when its node has been at safe levels for the whole
# RESOLUTION_WINDOW: 95% of readings within the limit (tolerates sensor noise),
# nothing more than 20% over it (no partial re-spike), and enough readings to
# prove the sensor was actually reporting.
RESOLUTION_CANDIDATES_SQL = """
SELECT i.incident_id, i.node_id, i.zone_name, i.severity_level, i.detected_at,
       COALESCE((i.ai_response->>'hazard_detected')::boolean, false) AS hazard, n.sensor_type
FROM environmental_incidents i
JOIN sensor_nodes n ON n.node_id = i.node_id
WHERE i.status = 'OPEN' AND i.detected_at <= now() - %(window)s::interval;
"""

WINDOW_STATS_SQL = """
SELECT count(*) AS n,
       max(metric_value) AS peak,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY metric_value) AS p95,
       min(time) <= now() - (%(window)s::interval * 0.8) AS covered
FROM sensor_readings
WHERE node_id = %(node)s AND time > now() - %(window)s::interval;
"""

MIN_WINDOW_READINGS = 5


def resolve_incidents(conn: psycopg.Connection) -> None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(RESOLUTION_CANDIDATES_SQL, {"window": RESOLUTION_WINDOW})
        candidates = cur.fetchall()

    for inc in candidates:
        limit = SENSOR_PROFILES.get(inc["sensor_type"], {}).get("max")
        if limit is None:
            continue
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(WINDOW_STATS_SQL, {"node": inc["node_id"], "window": RESOLUTION_WINDOW})
            w = cur.fetchone()
        safe = (w["n"] >= MIN_WINDOW_READINGS and w["covered"]
                and w["p95"] <= limit and w["peak"] <= limit * 1.2)
        if not safe:
            continue

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE environmental_incidents SET status = 'RESOLVED', resolved_at = now() "
                "WHERE incident_id = %s AND status = 'OPEN' RETURNING resolved_at", (inc["incident_id"],)
            )
            row = cur.fetchone()
            if row is None:
                conn.commit()
                continue  # someone else changed it first
            resolved_at = row[0]
            minutes_open = (resolved_at - inc["detected_at"]).total_seconds() / 60
            log.info("Incident #%s RESOLVED after %.0f min at safe levels (node %s)",
                     inc["incident_id"], minutes_open, inc["node_id"])

            if inc["hazard"]:
                tx = issue_resolution_bounty(incident_id=inc["incident_id"], node_id=inc["node_id"],
                                             zone=inc["zone_name"], severity=inc["severity_level"],
                                             resolved_at=resolved_at)
                cur.execute(
                    """
                    INSERT INTO bounty_payouts (incident_id, node_id, recipient, amount_sol, network, signature,
                                                transaction, status, explorer_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (incident_id) DO NOTHING
                    """,
                    (inc["incident_id"], inc["node_id"], tx["recipient"], tx["amount_sol"],
                     tx["network"], tx["signature"], Jsonb(tx), tx.get("status"), tx.get("explorer_url")),
                )
        conn.commit()

        try:
            memory.remember_resolution(node_id=inc["node_id"], zone=inc["zone_name"],
                                       incident_id=inc["incident_id"], minutes_open=minutes_open)
        except Exception as exc:
            log.warning("Backboard write failed for resolution of #%s: %s", inc["incident_id"], exc)


CONFIRM_TIMEOUT = "3 minutes"  # a devnet tx not seen by then is marked expired
_last_confirm_check = 0.0


def confirm_bounties(conn: psycopg.Connection) -> None:
    """Follow submitted devnet bounties until the network confirms them."""
    global _last_confirm_check
    if time.time() - _last_confirm_check < 5:  # no need to ask the network every 2s poll
        return
    _last_confirm_check = time.time()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT payout_id, signature, incident_id, created_at < now() - %s::interval FROM bounty_payouts "
            "WHERE network = 'devnet' AND status IN ('submitted', 'processed') ORDER BY payout_id LIMIT 10",
            (CONFIRM_TIMEOUT,),
        )
        pending = cur.fetchall()
    conn.commit()
    for payout_id, sig, incident_id, too_old in pending:
        try:
            status = check_confirmation(sig)
        except Exception as exc:
            log.warning("Could not check bounty %s: %s", sig[:12], exc)
            return  # network trouble: try again next round
        if status is None and too_old:
            status = "expired"
        if status and status != "submitted":
            with conn.cursor() as cur:
                cur.execute("UPDATE bounty_payouts SET status = %s WHERE payout_id = %s", (status, payout_id))
            conn.commit()
            if status in ("confirmed", "finalized"):
                log.info("Bounty for incident #%s confirmed on devnet", incident_id)
            elif status in ("failed", "expired"):
                log.warning("Bounty for incident #%s %s on devnet (%s)", incident_id, status, sig)


def run(once: bool = False, dry_run: bool = False) -> None:
    missing = [k for k, v in {
        "TIGER_DATABASE_URL": DATABASE_URL,
        "GEMINI_API_KEY": GEMINI_API_KEY or dry_run,
        "ELEVENLABS_API_KEY": ELEVENLABS_API_KEY or dry_run,
        "ELEVENLABS_VOICE_ID": ELEVENLABS_VOICE_ID or dry_run,
    }.items() if not v]
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")

    # Only one live detector at a time: two copies would each call Gemini and
    # each produce (and play) their own warning for the same spike.
    lock_file = None
    if not dry_run:
        lock_file = open(Path(__file__).with_name(".ecotwin_core.lock"), "w")
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(
                "Another ecotwin_core.py is already running. Stop it (Ctrl+C in its window) before starting a new one."
            )

    try:
        memory.ensure_assistant()
        log.info("Pattern memory: %s", memory.name)
    except Exception as exc:
        log.warning("Pattern memory (%s) unavailable, using the incident log only: %s", memory.name, exc)

    with psycopg.connect(DATABASE_URL) as conn:
        ensure_schema(conn)
        while True:
            try:
                anomalies = detect_anomalies(conn)
                by_zone = {x.zone_name: x for x in anomalies}
                for zone in list(_alerted):
                    if has_recovered(by_zone.get(zone)):
                        log.info("Zone %s back to normal; the next spike will alert again", zone)
                        _alerted.discard(zone)
                for anomaly in anomalies:
                    try:
                        process_anomaly(conn, anomaly, dry_run=dry_run)
                    except Exception:
                        log.exception("Failed to process node %s", anomaly.node_id)
                        conn.rollback()
                conn.commit()  # close the read transaction so now() moves forward
                if not dry_run:
                    try:
                        resolve_incidents(conn)
                        confirm_bounties(conn)
                    except Exception:
                        log.exception("Resolution check failed")
                        conn.rollback()
            except psycopg.Error:
                log.exception("Database error during poll")
                conn.rollback()
            if once:
                break
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="EcoTwin AI core loop")
    ap.add_argument("--once", action="store_true", help="run a single detection pass")
    ap.add_argument("--dry-run", action="store_true", help="skip Gemini/ElevenLabs, print payloads")
    ap.add_argument("--forget-memory", nargs="?", const="all", metavar="NODE_ID",
                    help="wipe the agent's pattern memory (one node, or all) and exit")
    args = ap.parse_args()
    if args.forget_memory:
        if not isinstance(memory, TigerMemory):
            raise SystemExit("--forget-memory works with the Tiger Data memory (no BACKBOARD_API_KEY set).")
        memory.ensure_assistant()
        n = memory.forget(None if args.forget_memory == "all" else int(args.forget_memory))
        raise SystemExit(f"Forgot {n} memory record(s).")
    run(once=args.once, dry_run=args.dry_run)
