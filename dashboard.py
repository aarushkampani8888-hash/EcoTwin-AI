"""
EcoTwin AI: live operations dashboard.

A single-file FastAPI app that reads the same Tiger Data tables the pipeline
writes to, and serves a one-page dashboard that refreshes every 2 seconds:
live sensor chart with the safety limit, latest Gemini incident with the camera
frame, and the generated voice broadcast.

Run (from the ecotwin-ai folder):
    pip install -r requirements.txt
    python3 dashboard.py
Then open http://127.0.0.1:8000

The dashboard only reads data. Detection still runs in ecotwin_core.py.
"""

from __future__ import annotations

import hmac
import os
import threading
import time
import uuid
from collections import OrderedDict, defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import psycopg
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

# Reuse the pipeline's config so limits, folders and DB URL never drift apart.
from ecotwin_core import (
    AUDIO_DIR,
    DATABASE_URL,
    FRAMES_DIR,
    SCHEMA_UPGRADE_SQL,
    RESOLUTION_WINDOW,
    SENSOR_PROFILES,
    synthesize_broadcast,
)
import voice_console  # noqa: E402

HOST = os.getenv("DASHBOARD_HOST", "127.0.0.1")
PORT = int(os.getenv("DASHBOARD_PORT", "8000"))
DEFAULT_NODE = int(os.getenv("DASHBOARD_NODE", "402"))

# Public-site protection. Every spike costs a Gemini call + a voice render, and
# every console question costs another pair, so both are capped.
DEMO_PASSCODE = os.getenv("DEMO_PASSCODE", "").strip()      # optional; empty = open to everyone
SPIKE_SECONDS = float(os.getenv("SPIKE_SECONDS", "45"))       # how long a triggered spike lasts
SPIKE_COOLDOWN_S = int(os.getenv("SPIKE_COOLDOWN_SECONDS", "180"))
DAILY_INCIDENT_LIMIT = int(os.getenv("DAILY_INCIDENT_LIMIT", "30"))
ASK_DAILY_LIMIT = int(os.getenv("ASK_DAILY_LIMIT", "40"))
ASK_MIN_INTERVAL_S = float(os.getenv("ASK_MIN_INTERVAL_SECONDS", "8"))

WORKERS: dict[str, threading.Thread] = {}  # filled in by run_all.py
CHART_MINUTES = int(os.getenv("DASHBOARD_MINUTES", "15"))
BASE_DIR = Path(__file__).resolve().parent
LOCAL_CHART_JS = BASE_DIR / "static" / "chart.umd.min.js"


# ---------------------------------------------------------------------------
# Database: one shared connection, reopened automatically if it drops.
# ---------------------------------------------------------------------------
_conn: psycopg.Connection | None = None
_lock = threading.Lock()


def query(sql: str, params: dict | tuple | None = None) -> list[dict]:
    global _conn
    with _lock:
        for attempt in (1, 2):
            try:
                if _conn is None or _conn.closed:
                    _conn = psycopg.connect(DATABASE_URL, autocommit=True, row_factory=dict_row)
                with _conn.cursor() as cur:
                    cur.execute(sql, params)
                    return cur.fetchall() if cur.description else []
            except psycopg.OperationalError:
                _conn = None  # stale connection (Wi-Fi blip, laptop sleep): retry once
                if attempt == 2:
                    raise
    return []


@asynccontextmanager
async def lifespan(_app: FastAPI):
    if not DATABASE_URL:
        raise SystemExit("TIGER_DATABASE_URL is missing from .env")
    query(SCHEMA_UPGRADE_SQL)  # adds the incident columns the dashboard reads, if missing
    yield


app = FastAPI(title="EcoTwin AI Dashboard", lifespan=lifespan)


def iso(value):
    return value.isoformat() if isinstance(value, datetime) else value


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True, "workers": {name: t.is_alive() for name, t in WORKERS.items()}}


def client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    return fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "?")


def require_passcode(request: Request) -> None:
    if DEMO_PASSCODE and not hmac.compare_digest(request.headers.get("x-demo-passcode", ""), DEMO_PASSCODE):
        raise HTTPException(401, "Demo passcode required")


@app.get("/api/nodes")
def nodes() -> list[dict]:
    return query("SELECT node_id, zone_name, sensor_type FROM sensor_nodes ORDER BY node_id")


@app.get("/api/state")
def state(node_id: int = Query(DEFAULT_NODE), minutes: int = Query(CHART_MINUTES, ge=1, le=180)) -> dict:
    node_rows = query(
        "SELECT node_id, zone_name, sensor_type, latitude, longitude, response_resources "
        "FROM sensor_nodes WHERE node_id = %s",
        (node_id,),
    )
    if not node_rows:
        raise HTTPException(404, f"Node {node_id} not found in sensor_nodes")
    node = node_rows[0]
    profile = SENSOR_PROFILES.get(node["sensor_type"], {"label": node["sensor_type"], "unit": "", "max": None})

    readings = query(
        """
        SELECT time, metric_value FROM sensor_readings
        WHERE node_id = %s AND time > now() - make_interval(mins => %s)
        ORDER BY time
        """,
        (node_id, minutes),
    )

    incidents = query(
        """
        SELECT i.incident_id, i.detected_at, i.severity_level, i.ai_summary, i.action_triggered,
               i.ai_response, i.audio_file, i.frame_file, i.status, i.resolved_at, i.classification,
               i.status = 'OPEN' AS active,
               b.amount_sol, b.recipient, b.signature, b.network,
               b.status AS bounty_status, b.explorer_url
        FROM environmental_incidents i
        LEFT JOIN bounty_payouts b USING (incident_id)
        WHERE i.zone_name = %s
        ORDER BY i.detected_at DESC
        LIMIT 6
        """,
        (node["zone_name"],),
    )

    latest = readings[-1]["metric_value"] if readings else None
    limit = profile["max"]

    demo_rows = query(
        """
        SELECT spike_until > now() AS spike_active,
               GREATEST(0, extract(epoch FROM spike_until - now())) AS spike_left,
               GREATEST(0, extract(epoch FROM requested_at + make_interval(secs => %s) - now())) AS cooldown_left
        FROM demo_controls WHERE node_id = %s
        """,
        (SPIKE_COOLDOWN_S, node_id),
    )
    d = demo_rows[0] if demo_rows else {}
    resolution = None
    if incidents and incidents[0]["status"] == "OPEN" and limit is not None:
        r = query(
            """
            SELECT extract(epoch FROM now() - COALESCE(
                       (SELECT max(time) FROM sensor_readings WHERE node_id = %s AND metric_value > %s), now()))
                       AS safe_s,
                   extract(epoch FROM %s::interval) AS window_s
            """,
            (node_id, limit, RESOLUTION_WINDOW),
        )[0]
        resolution = {"safe_s": round(float(r["safe_s"])), "window_s": round(float(r["window_s"]))}
    return {
        "demo": {"spike_active": bool(d.get("spike_active")),
                 "spike_left": round(float(d.get("spike_left") or 0)),
                 "cooldown_left": round(float(d.get("cooldown_left") or 0)),
                 "passcode_required": bool(DEMO_PASSCODE)},
        "resolution": resolution,
        "server_time": datetime.now().astimezone().isoformat(),
        "node": {**node, "latitude": float(node["latitude"]), "longitude": float(node["longitude"])},
        "metric": {"label": profile["label"], "unit": profile["unit"], "limit": limit},
        "latest": latest,
        "over_limit": latest is not None and limit is not None and latest > limit,
        "readings": [{"t": iso(r["time"]), "v": round(r["metric_value"], 2)} for r in readings],
        "incidents": [{**i, "detected_at": iso(i["detected_at"]), "resolved_at": iso(i["resolved_at"]),
                       "amount_sol": float(i["amount_sol"]) if i["amount_sol"] is not None else None}
                      for i in incidents],
    }


def safe_file(folder: Path, name: str) -> Path:
    path = (folder / Path(name).name).resolve()  # Path(name).name strips any ../ tricks
    if not path.is_file() or path.parent != folder.resolve():
        raise HTTPException(404, "File not found")
    return path


@app.get("/audio/{name}")
def audio(name: str):
    return FileResponse(safe_file(AUDIO_DIR if AUDIO_DIR.is_absolute() else BASE_DIR / AUDIO_DIR, name))


@app.get("/frames/{name}")
def frame(name: str):
    return FileResponse(safe_file(FRAMES_DIR if FRAMES_DIR.is_absolute() else BASE_DIR / FRAMES_DIR, name))


@app.get("/static/chart.js")
def chart_js():
    if not LOCAL_CHART_JS.is_file():
        raise HTTPException(404, "No local Chart.js copy")
    return FileResponse(LOCAL_CHART_JS, media_type="application/javascript")


@app.get("/api/wallet")
def wallet_state() -> dict:
    """The response crew's view: bounties received, newest first."""
    rows = query(
        """
        SELECT b.payout_id, b.incident_id, b.amount_sol, b.network, b.status, b.signature, b.explorer_url,
               b.recipient, b.created_at, i.zone_name, i.severity_level, i.node_id
        FROM bounty_payouts b
        LEFT JOIN environmental_incidents i USING (incident_id)
        ORDER BY b.payout_id DESC
        LIMIT 30
        """
    )
    payouts = [{**r, "amount_sol": float(r["amount_sol"] or 0), "created_at": iso(r["created_at"])} for r in rows]
    counted = [p for p in payouts if p["status"] not in ("failed", "expired")]
    return {
        "server_time": datetime.now().astimezone().isoformat(),
        "address": payouts[0]["recipient"] if payouts else None,
        "total_sol": round(sum(p["amount_sol"] for p in counted), 4),
        "count": len(counted),
        "payouts": payouts,
    }


# ---------------------------------------------------------------------------
# Voice command console
# ---------------------------------------------------------------------------
@app.post("/api/demo/spike")
def trigger_spike(request: Request, node_id: int = Query(DEFAULT_NODE)) -> dict:
    """The 'Trigger contamination spike' button. One event at a time, with a cooldown and a daily cap."""
    require_passcode(request)
    d = query("SELECT spike_until > now() AS active, "
              "GREATEST(0, extract(epoch FROM requested_at + make_interval(secs => %s) - now())) AS cooldown "
              "FROM demo_controls WHERE node_id = %s", (SPIKE_COOLDOWN_S, node_id))
    if d and d[0]["active"]:
        raise HTTPException(409, "A spike is already running")
    if query("SELECT 1 FROM environmental_incidents WHERE node_id = %s AND status = 'OPEN' LIMIT 1", (node_id,)):
        raise HTTPException(409, "An incident is still open. Wait for it to auto-resolve first")
    if d and float(d[0]["cooldown"] or 0) > 0:
        raise HTTPException(429, f"Next spike available in {round(float(d[0]['cooldown']))}s")
    today = query("SELECT count(*) AS n FROM environmental_incidents WHERE detected_at > now() - interval '1 day'")
    if today and today[0]["n"] >= DAILY_INCIDENT_LIMIT:
        raise HTTPException(429, "Daily demo limit reached. Please try again tomorrow")
    query(
        """
        INSERT INTO demo_controls (node_id, spike_until, requested_at, requested_by)
        VALUES (%s, now() + make_interval(secs => %s), now(), %s)
        ON CONFLICT (node_id) DO UPDATE SET spike_until = EXCLUDED.spike_until,
            requested_at = EXCLUDED.requested_at, requested_by = EXCLUDED.requested_by
        """,
        (node_id, SPIKE_SECONDS, "dashboard:" + client_ip(request)),
    )
    return {"ok": True, "spike_seconds": SPIKE_SECONDS}


# ---------------------------------------------------------------------------
# Voice command console
# ---------------------------------------------------------------------------
class AskBody(BaseModel):
    question: str = Field(..., max_length=400)
    history: list[dict] = Field(default_factory=list, max_length=6)


class SpeakBody(BaseModel):
    answer_id: str = Field(..., max_length=64)


# One question at a time: every answer costs a Gemini call and a voice render.
_ask_lock = threading.Lock()
_speak_lock = threading.Lock()
_last_ask: dict[str, float] = defaultdict(float)
_asks_today: deque = deque()                     # timestamps of answered questions, last 24h
# Only answers EcoTwin generated can be spoken, so the voice endpoint can't be
# used as a free text-to-speech service for arbitrary text.
_answers: OrderedDict[str, dict] = OrderedDict()


@app.post("/api/ask")
def ask(body: AskBody, request: Request) -> dict:
    require_passcode(request)
    question = body.question.strip()
    if not question:
        raise HTTPException(400, "Empty question")
    ip, now = client_ip(request), time.time()
    if now - _last_ask[ip] < ASK_MIN_INTERVAL_S:
        raise HTTPException(429, f"One question every {ASK_MIN_INTERVAL_S:g} seconds, please")
    while _asks_today and now - _asks_today[0] > 86400:
        _asks_today.popleft()
    if len(_asks_today) >= ASK_DAILY_LIMIT:
        raise HTTPException(429, "The voice console has reached today's limit")
    if not _ask_lock.acquire(blocking=False):
        raise HTTPException(429, "Still answering the previous question")
    try:
        _last_ask[ip] = now
        snapshot = voice_console.build_snapshot(query)
        result = voice_console.answer(question, snapshot, body.history[-3:])
        _asks_today.append(now)
        answer_id = uuid.uuid4().hex
        _answers[answer_id] = {"text": (result.get("answer") or "")[:600], "audio": None}
        while len(_answers) > 50:
            _answers.popitem(last=False)
        return {"question": question, "answer": result.get("answer", ""), "answer_id": answer_id,
                "node_ids": result.get("node_ids", []), "gemini_s": result.get("gemini_s")}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"EcoTwin couldn't answer: {exc}") from exc
    finally:
        _ask_lock.release()


@app.post("/api/speak")
def speak(body: SpeakBody, request: Request) -> dict:
    require_passcode(request)
    entry = _answers.get(body.answer_id)
    if not entry or not entry["text"].strip():
        raise HTTPException(404, "Unknown answer")
    if entry["audio"]:
        return {"audio": entry["audio"]}  # already rendered: never pay twice
    if not _speak_lock.acquire(blocking=False):
        raise HTTPException(429, "Already speaking")
    try:
        folder = AUDIO_DIR if AUDIO_DIR.is_absolute() else BASE_DIR / AUDIO_DIR
        path = synthesize_broadcast(entry["text"], folder / f"console_{int(datetime.now().timestamp() * 1000)}.mp3")
        entry["audio"] = f"/audio/{path.name}"
        return {"audio": entry["audio"]}
    except Exception as exc:
        raise HTTPException(502, f"Voice unavailable: {exc}") from exc
    finally:
        _speak_lock.release()


@app.get("/wallet", response_class=HTMLResponse)
def wallet_page() -> str:
    return WALLET_PAGE


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    chart_src = (
        "/static/chart.js"
        if LOCAL_CHART_JS.is_file()
        else "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"
    )
    return PAGE.replace("{{CHART_SRC}}", chart_src).replace("{{NODE}}", str(DEFAULT_NODE))


# ---------------------------------------------------------------------------
# Frontend (single page, no build step)
# ---------------------------------------------------------------------------
PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>EcoTwin AI</title>
<style>
  :root { color-scheme: light dark; }
  :root {
    --bg: #f4f6f5; --panel: #ffffff; --ink: #17201c; --muted: #5f6b66; --line: #dde3e0;
    --ok: #1f8a5b; --warn: #c27c0e; --crit: #c62f2f; --accent: #2563eb; --limit: #c62f2f;
  }
  @media (prefers-color-scheme: dark) {
    :root { --bg: #0f1412; --panel: #171e1b; --ink: #e6ece9; --muted: #93a19b; --line: #2a3430;
            --ok: #3cc488; --warn: #e3a33a; --crit: #ef5b5b; --accent: #6b9bff; --limit: #ef5b5b; }
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--ink);
         font: 15px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif; }
  header { display: flex; align-items: center; gap: 16px; flex-wrap: wrap;
           padding: 16px 24px; border-bottom: 1px solid var(--line); background: var(--panel); }
  header h1 { font-size: 18px; margin: 0; letter-spacing: .2px; }
  header .zone { color: var(--muted); }
  header .spacer { flex: 1; }
  .pill { display: inline-flex; align-items: center; gap: 6px; padding: 4px 12px; border-radius: 999px;
          font-weight: 600; font-size: 13px; border: 1px solid currentColor; }
  .pill.ok { color: var(--ok); } .pill.crit { color: var(--crit); } .pill.warn { color: var(--warn); }
  .pill.muted { color: var(--muted); }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: currentColor; }
  .crit .dot { animation: pulse 1s infinite; }
  @keyframes pulse { 50% { opacity: .25; } }
  main { display: grid; grid-template-columns: minmax(0, 1.6fr) minmax(0, 1fr); gap: 16px; padding: 16px 24px; }
  @media (max-width: 900px) { main { grid-template-columns: 1fr; padding: 16px; } header { padding: 14px 16px; } }
  .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 12px; padding: 18px; }
  .panel h2 { margin: 0 0 12px; font-size: 13px; text-transform: uppercase; letter-spacing: .8px; color: var(--muted); }
  .reading { display: flex; align-items: baseline; gap: 10px; margin-bottom: 8px; }
  .reading .value { font-size: 44px; font-weight: 700; font-variant-numeric: tabular-nums; }
  .reading .unit { color: var(--muted); }
  .reading .limit { margin-left: auto; color: var(--muted); font-size: 13px; }
  .over { color: var(--crit); }
  .live { display: flex; flex-direction: column; }
  .chart-wrap { position: relative; flex: 1; min-height: 340px; }
  .empty { color: var(--muted); padding: 24px 0; }
  .sev { font-size: 22px; font-weight: 700; margin: 0 0 4px; }
  .sev.CRITICAL { color: var(--crit); } .sev.MODERATE { color: var(--warn); } .sev.LOW { color: var(--ok); }
  .meta { color: var(--muted); font-size: 13px; margin-bottom: 14px; }
  dl { margin: 0; } dt { font-size: 12px; text-transform: uppercase; letter-spacing: .6px; color: var(--muted); margin-top: 12px; }
  dd { margin: 4px 0 0; }
  .frame { width: 100%; border-radius: 8px; border: 1px solid var(--line); margin-top: 14px; display: block; }
  .noframe { margin-top: 14px; padding: 14px; border: 1px dashed var(--line); border-radius: 8px; color: var(--muted); font-size: 13px; }
  .broadcast { margin-top: 16px; padding: 14px; border-radius: 8px; background: var(--bg); }
  .broadcast q { display: block; font-style: italic; margin-bottom: 10px; }
  audio { width: 100%; }
  .row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-top: 8px; font-size: 13px; color: var(--muted); }
  button { font: inherit; padding: 6px 12px; border-radius: 8px; border: 1px solid var(--line); background: var(--panel); color: var(--ink); cursor: pointer; }
  button.on { border-color: var(--accent); color: var(--accent); }
  .history { margin-top: 16px; }
  .history li { color: var(--muted); font-size: 13px; margin: 4px 0; }
  ul { padding-left: 18px; margin: 0; }
  .demo-row { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--line); }
  .primary { background: var(--crit); color: #fff; border-color: var(--crit); font-weight: 600; padding: 9px 14px; }
  .primary:disabled { background: var(--bg); color: var(--muted); border-color: var(--line); cursor: default; }
  .demo-state { color: var(--muted); font-size: 13px; flex: 1; min-width: 200px; }
  .progress { height: 6px; border-radius: 3px; background: var(--line); overflow: hidden; margin-top: 6px; }
  .progress > div { height: 100%; background: var(--ok); width: 0; transition: width .5s; }
  .console { display: flex; gap: 12px; align-items: flex-start; padding: 12px 24px; border-bottom: 1px solid var(--line); background: var(--panel); flex-wrap: wrap; }
  @media (max-width: 900px) { .console { padding: 12px 16px; } }
  .mic { width: 48px; height: 48px; border-radius: 50%; border: 1px solid var(--line); background: var(--bg); color: var(--ink); font-size: 20px; cursor: pointer; flex: none; display: grid; place-items: center; }
  .mic.listening { background: var(--crit); color: #fff; border-color: var(--crit); animation: ring 1.2s infinite; }
  @keyframes ring { 0% { box-shadow: 0 0 0 0 rgba(198,47,47,.5); } 100% { box-shadow: 0 0 0 14px rgba(198,47,47,0); } }
  .askbox { flex: 1 1 320px; display: flex; flex-direction: column; gap: 8px; min-width: 0; }
  .askrow { display: flex; gap: 8px; }
  .askrow input { flex: 1; min-width: 0; font: inherit; padding: 10px 12px; border-radius: 10px; border: 1px solid var(--line); background: var(--bg); color: var(--ink); }
  .askrow button { flex: none; }
  .chips { display: flex; gap: 6px; flex-wrap: wrap; }
  .chip { font-size: 12px; padding: 4px 10px; border-radius: 999px; }
  .reply { flex: 2 1 380px; min-width: 0; font-size: 15px; }
  .reply .you { color: var(--muted); font-size: 13px; margin-bottom: 4px; }
  .reply .bot { font-weight: 500; }
  .reply .state { color: var(--muted); font-size: 12px; margin-top: 4px; }
  #error { display: none; background: var(--crit); color: #fff; padding: 8px 24px; font-size: 14px; }
  .tags { display: flex; gap: 6px; flex-wrap: wrap; }
  .badges { display: flex; gap: 8px; flex-wrap: wrap; margin: 2px 0 10px; }
  .badge { font-size: 12px; font-weight: 700; letter-spacing: .4px; padding: 3px 9px; border-radius: 6px; border: 1px solid currentColor; }
  .badge.OPEN { color: var(--crit); } .badge.RESOLVED { color: var(--ok); } .badge.SUPERSEDED { color: var(--muted); }
  .badge.SYSTEMIC { color: #fff; background: var(--crit); border-color: var(--crit); } .badge.ISOLATED { color: var(--muted); }
  .bounty { margin-top: 14px; padding: 12px 14px; border-radius: 8px; border: 1px solid var(--ok); font-size: 14px; }
  .bounty a { color: var(--accent); font-weight: 600; }
  .bounty code { font-size: 12px; word-break: break-all; color: var(--muted); }
  .tag { font-size: 12px; padding: 2px 8px; border-radius: 6px; background: var(--bg); border: 1px solid var(--line); }
</style>
</head>
<body>
<div id="error"></div>
<header>
  <h1>EcoTwin AI</h1>
  <span class="zone" id="zone">Loading...</span>
  <span class="spacer"></span>
  <span class="pill muted" id="status"><span class="dot"></span><span>Connecting</span></span>
  <span class="zone" id="updated"></span>
</header>

<section class="console" aria-label="Ask EcoTwin">
  <button class="mic" id="mic" type="button" title="Ask EcoTwin by voice" aria-label="Ask EcoTwin by voice">🎙</button>
  <div class="askbox">
    <form class="askrow" id="askform">
      <input id="askinput" autocomplete="off" placeholder="Ask EcoTwin… e.g. what's the status of the downtown basin?">
      <button type="submit">Ask</button>
    </form>
    <div class="chips">
      <button type="button" class="chip">Status of the downtown basin, and do we have field crews nearby?</button>
      <button type="button" class="chip">Which zones need attention right now?</button>
      <button type="button" class="chip">Is this a repeat problem?</button>
    </div>
  </div>
  <div class="reply" id="reply"><div class="state">Press the mic and speak, or type a question.</div></div>
  <audio id="voice" preload="auto" hidden></audio>
</section>

<main>
  <section class="panel live">
    <h2 id="metric-title">Live sensor</h2>
    <div class="reading">
      <span class="value" id="latest">--</span><span class="unit" id="unit"></span>
      <span class="limit" id="limit"></span>
    </div>
    <div class="chart-wrap"><canvas id="chart"></canvas></div>
    <div class="row">Resources on file: <span class="tags" id="resources"></span></div>
    <div class="demo-row">
      <button id="spikebtn" class="primary" type="button">⚡ Trigger contamination spike</button>
      <div class="demo-state"><span id="demostate">Simulated sensor · press to start a contamination event at the outfall</span>
        <div class="progress" id="progress" hidden><div id="progressbar"></div></div></div>
    </div>
  </section>

  <section class="panel" id="incident-panel">
    <h2>Latest incident</h2>
    <div class="row" style="margin:-4px 0 12px">
      <button id="autoplay" type="button">Auto-play alerts: off</button>
      <span>Click once before the demo. Browsers only allow sound after a click.</span>
    </div>
    <div id="incident"><p class="empty">No incidents logged for this zone yet.</p></div>
    <div class="broadcast" id="broadcast" hidden>
      <q id="script"></q>
      <audio id="player" controls preload="auto"></audio>
    </div>
    <div class="history"><h2>Recent</h2><ul id="history"><li>None yet</li></ul></div>
  </section>
</main>

<script src="{{CHART_SRC}}"></script>
<script>
const NODE = new URLSearchParams(location.search).get("node") || "{{NODE}}";
const REFRESH_MS = 2000;
const SILENT = "data:audio/wav;base64,UklGRrQBAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YZABAACAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICA";
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const $ = id => document.getElementById(id);
const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const clock = t => new Date(t).toLocaleTimeString([], {hour: "2-digit", minute: "2-digit", second: "2-digit"});

let chart = null, limit = null, autoplay = false, firstLoad = true;

function buildChart() {
  if (!window.Chart) { $("metric-title").textContent += " (chart library failed to load)"; return; }
  chart = new Chart($("chart"), {
    type: "line",
    data: { labels: [], datasets: [
      { label: "Reading", data: [], borderWidth: 2, pointRadius: 0, tension: .25, borderColor: css("--accent"),
        segment: { borderColor: c => (limit !== null && c.p1.parsed.y > limit) ? css("--crit") : css("--accent") } },
      { label: "Safety limit", data: [], borderWidth: 1.5, borderDash: [6, 5], pointRadius: 0, borderColor: css("--limit") }
    ]},
    options: {
      animation: false, responsive: true, maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { labels: { color: css("--muted"), boxWidth: 14 } } },
      scales: {
        x: { ticks: { color: css("--muted"), maxTicksLimit: 8, maxRotation: 0 }, grid: { color: css("--line") } },
        y: { beginAtZero: true, ticks: { color: css("--muted") }, grid: { color: css("--line") } }
      }
    }
  });
}

function setStatus(cls, text) {
  $("status").className = "pill " + cls;
  $("status").innerHTML = '<span class="dot"></span><span>' + esc(text) + "</span>";
}

function renderIncident(inc) {
  if (!inc) {
    $("incident").innerHTML = '<p class="empty">No incidents logged for this zone yet.</p>';
    $("broadcast").hidden = true;
    return;
  }
  const ai = inc.ai_response || {};
  const sev = ai.severity || inc.severity_level || "UNKNOWN";
  const triggers = (ai.detector_triggers || []).map(t => "<li>" + esc(t) + "</li>").join("");
  const frame = inc.frame_file
    ? '<img class="frame" alt="Camera frame Gemini analyzed" src="/frames/' + encodeURIComponent(inc.frame_file) + '">'
    : '<div class="noframe">No camera frame was attached. Gemini reasoned from sensor data only.</div>';
  const body = inc.ai_response ? `
      <dl>
        <dt>Root cause hypothesis</dt><dd>${esc(ai.root_cause_hypothesis)}</dd>
        <dt>Action plan</dt><dd>${esc(ai.action_plan)}</dd>
        ${triggers ? "<dt>Why the detector fired</dt><dd><ul>" + triggers + "</ul></dd>" : ""}
      </dl>`
    : `<dl><dt>Summary</dt><dd style="white-space:pre-wrap">${esc(inc.ai_summary)}</dd></dl>`;
  const pat = ai.pattern || {};
  const cls = inc.classification || pat.classification;
  const badges = [
    inc.status ? `<span class="badge ${esc(inc.status)}">${esc(inc.status)}</span>` : "",
    cls ? `<span class="badge ${esc(cls)}">${cls === "SYSTEMIC" ? "SYSTEMIC PATTERN" : "ISOLATED EVENT"}</span>` : "",
  ].join("");
  const patternInfo = cls ? `<dt>Pattern memory</dt><dd>${esc(pat.prior_incidents ?? "?")} prior incident(s) at this node in ${esc(pat.window_days ?? "?")} days
      <span class="meta">(${esc(pat.memory_source || "")})</span>${ai.pattern_rationale ? "<br>" + esc(ai.pattern_rationale) : ""}</dd>` : "";
  const resolved = inc.status === "RESOLVED" && inc.resolved_at
    ? `<div class="meta">Resolved at ${clock(inc.resolved_at)} after metrics held at safe levels</div>` : "";
  const bountyNote = inc.network === "devnet"
    ? ({submitted: "sent to Solana devnet, confirming…", processed: "processing on Solana devnet…",
        confirmed: "confirmed on Solana devnet", finalized: "finalized on Solana devnet",
        failed: "failed on Solana devnet", expired: "not confirmed on Solana devnet"}[inc.bounty_status]
       || "Solana devnet") + " · test SOL"
    : "mock transaction, not submitted";
  const bounty = inc.signature ? `<div class="bounty">Bounty <b>${inc.amount_sol} SOL</b> to response crew
      <span class="meta">(${esc(bountyNote)})</span><br>
      ${inc.explorer_url ? `<a href="${esc(inc.explorer_url)}" target="_blank" rel="noopener">View transaction on Solana Explorer ↗</a><br>` : ""}
      <code>${esc(inc.signature)}</code></div>` : "";
  $("incident").innerHTML = `
      <p class="sev ${esc(sev)}">${esc(sev)}${ai.hazard_detected === false ? " (no hazard)" : ""}</p>
      <div class="badges">${badges}</div>
      <div class="meta">Incident #${inc.incident_id} at ${clock(inc.detected_at)}</div>
      ${resolved}
      ${timingLine(ai.timing) ? `<div class="meta">${timingLine(ai.timing)}</div>` : ""}
      ${body.replace("</dl>", patternInfo + "</dl>")}${bounty}${frame}`;

  const script = ai.elevenlabs_broadcast_script;
  $("broadcast").hidden = !script && !inc.audio_file;
  $("script").textContent = script || "";
  const src = inc.audio_file ? "/audio/" + encodeURIComponent(inc.audio_file) : "";
  if (src && !$("player").src.endsWith(src)) $("player").src = src;
}

function renderHistory(list) {
  $("history").innerHTML = list.length
    ? list.map(i => `<li>#${i.incident_id} · ${clock(i.detected_at)} · ${esc(i.severity_level || "?")}` +
        `${i.status ? " · " + esc(i.status) : ""}${i.classification === "SYSTEMIC" ? " · systemic" : ""}` +
        `${i.amount_sol ? " · " + i.amount_sol + " SOL" + (i.network === "devnet" ? " (devnet)" : " (mock)") : ""}</li>`).join("")
    : "<li>None yet</li>";
}

function loadPlayed() {
  try { return JSON.parse(localStorage.getItem("ecotwin-played") || "[]"); } catch (e) { return playedFallback; }
}
function savePlayed(list) {
  playedFallback = list.slice(-50);
  try { localStorage.setItem("ecotwin-played", JSON.stringify(playedFallback)); } catch (e) {}
}
let playedFallback = [];

function timingLine(t) {
  if (!t || t.total_s == null) return t && t.gemini_s != null ? `Gemini ${t.gemini_s}s · generating voice…` : "";
  return `Alert ready <b>${t.total_s}s</b> after the limit was crossed ` +
         `(detect ${t.detect_s ?? "?"}s · ${t.memory_s != null ? "memory " + t.memory_s + "s · " : ""}Gemini ${t.gemini_s}s · voice ${t.tts_s ?? "?"}s)`;
}

async function refresh() {
  try {
    const res = await fetch("/api/state?node_id=" + encodeURIComponent(NODE), { cache: "no-store" });
    if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
    const s = await res.json();
    $("error").style.display = "none";

    limit = s.metric.limit;
    $("zone").textContent = "Node " + s.node.node_id + " · " + s.node.zone_name;
    $("metric-title").textContent = s.metric.label + " (last " + Math.round(s.readings.length ? (Date.now() - new Date(s.readings[0].t)) / 60000 : 0) + " min)";
    $("latest").textContent = s.latest === null ? "--" : s.latest.toFixed(1);
    $("latest").className = "value" + (s.over_limit ? " over" : "");
    $("unit").textContent = s.metric.unit;
    $("limit").textContent = limit === null ? "" : "Limit " + limit + " " + s.metric.unit;
    $("updated").textContent = "Updated " + clock(s.server_time);
    const res_list = Array.isArray(s.node.response_resources) ? s.node.response_resources
                   : (s.node.response_resources ? [s.node.response_resources] : []);
    $("resources").innerHTML = res_list.length ? res_list.map(r => '<span class="tag">' + esc(r) + "</span>").join("") : "none set";

    const latestInc = s.incidents[0];
    if (latestInc && latestInc.active && latestInc.severity_level === "CRITICAL") setStatus("crit", "Critical incident");
    else if (latestInc && latestInc.active) setStatus("warn", "Incident: " + (latestInc.severity_level || "open"));
    else if (s.over_limit) setStatus("warn", "Over limit, analyzing");
    else setStatus("ok", "Normal");

    if (chart) {
      chart.data.labels = s.readings.map(r => clock(r.t));
      chart.data.datasets[0].data = s.readings.map(r => r.v);
      chart.data.datasets[1].data = s.readings.map(() => limit);
      chart.update("none");
    }

    renderIncident(latestInc);
    renderDemo(s);
    renderHistory(s.incidents);

    // Play each incident's broadcast exactly once, as soon as its audio exists.
    // The played list is shared across tabs, so only one tab ever plays it.
    if (latestInc && latestInc.audio_file) {
      const played = loadPlayed();
      const id = String(latestInc.incident_id);
      if (!played.includes(id)) {
        savePlayed(played.concat(id));
        if (!firstLoad && autoplay) {
          $("player").currentTime = 0;
          $("player").play().catch(() => {});
          document.title = "ALERT · EcoTwin AI";
        }
      }
    }
    firstLoad = false;
  } catch (err) {
    $("error").textContent = "Can't reach the data: " + err.message + ". Retrying...";
    $("error").style.display = "block";
    setStatus("muted", "Disconnected");
  }
}

$("autoplay").addEventListener("click", () => {
  autoplay = !autoplay;
  $("autoplay").textContent = "Auto-play alerts: " + (autoplay ? "on" : "off");
  $("autoplay").classList.toggle("on", autoplay);
  if (autoplay) {
    // Play a silent clip inside the click so the browser unlocks sound for later alerts.
    const p = $("player"), keep = p.getAttribute("src");
    p.src = SILENT;
    p.play().catch(() => {}).finally(() => { p.pause(); if (keep) p.src = keep; else p.removeAttribute("src"); });
  }
});

// ---- Calls that cost credits (optional demo passcode) -----------------------
async function api(path, body) {
  for (let attempt = 0; attempt < 2; attempt++) {
    let pass = ""; try { pass = localStorage.getItem("ecotwin-pass") || ""; } catch (e) {}
    const r = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json", "X-Demo-Passcode": pass },
                                  body: JSON.stringify(body || {}) });
    if (r.status === 401 && attempt === 0) {
      const p = window.prompt("This demo needs a passcode (see the project submission):");
      if (!p) throw new Error("Passcode required");
      try { localStorage.setItem("ecotwin-pass", p.trim()); } catch (e) {}
      continue;
    }
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.detail || r.statusText);
    return data;
  }
}

function renderDemo(s) {
  const d = s.demo || {}, res = s.resolution, btn = $("spikebtn"), latest = s.incidents[0];
  let text, ready = false, pct = null;
  if (d.spike_active) text = `Contamination spike running · ${d.spike_left}s left · watch EcoTwin detect it`;
  else if (latest && latest.status === "OPEN" && res) {
    if (res.safe_s < 2) text = "Incident open · waiting for readings to return to safe levels";
    else { pct = Math.min(100, Math.round(100 * res.safe_s / res.window_s));
           text = `Readings safe for ${Math.min(res.safe_s, res.window_s)}s of ${res.window_s}s · auto-resolving…`; }
  }
  else if (d.cooldown_left > 0) text = `Next spike available in ${d.cooldown_left}s`;
  else { ready = true; text = "Simulated sensor · press to start a contamination event at the outfall"; }
  btn.disabled = !ready;
  $("demostate").textContent = text;
  $("progress").hidden = pct === null;
  if (pct !== null) $("progressbar").style.width = pct + "%";
}

$("spikebtn").addEventListener("click", async () => {
  $("spikebtn").disabled = true;
  try { await api("/api/demo/spike"); $("demostate").textContent = "Spike starting…"; }
  catch (err) { $("demostate").textContent = err.message; }
});

// ---- Voice command console ----------------------------------------------
const convo = [];
let asking = false, recognizer = null;

function setReply(q, a, state) {
  $("reply").innerHTML = (q ? `<div class="you">You: ${esc(q)}</div>` : "") +
    (a ? `<div class="bot">${esc(a)}</div>` : "") + (state ? `<div class="state">${esc(state)}</div>` : "");
}

async function askEcoTwin(question) {
  question = (question || "").trim();
  if (!question || asking) return;
  asking = true; $("askinput").value = "";
  setReply(question, "", "Checking live data…");
  try {
    const data = await api("/api/ask", { question, history: convo.slice(-3) });
    convo.push({ q: question, a: data.answer });
    setReply(question, data.answer, `Answered from live Tiger Data in ${data.gemini_s}s · generating voice…`);
    const vd = await api("/api/speak", { answer_id: data.answer_id });
    $("voice").src = vd.audio;
    await $("voice").play().catch(() => {});
    setReply(question, data.answer, `Answered from live Tiger Data in ${data.gemini_s}s`);
  } catch (err) {
    setReply(question, convo.length && convo[convo.length - 1].q === question ? convo[convo.length - 1].a : "",
             "Problem: " + err.message);
  } finally { asking = false; }
}

$("askform").addEventListener("submit", e => { e.preventDefault(); askEcoTwin($("askinput").value); });
document.querySelectorAll(".chip").forEach(c => c.addEventListener("click", () => askEcoTwin(c.textContent)));

const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
if (!SR) {
  $("mic").disabled = true; $("mic").title = "Voice input needs Chrome or Safari; type your question instead";
} else {
  $("mic").addEventListener("click", () => {
    if (recognizer) { recognizer.stop(); return; }
    $("voice").pause();
    recognizer = new SR(); recognizer.lang = "en-US"; recognizer.interimResults = true; recognizer.continuous = false;
    let finalText = "";
    recognizer.onresult = e => {
      const text = Array.from(e.results).map(r => r[0].transcript).join(" ");
      finalText = text; setReply(text, "", "Listening…");
    };
    recognizer.onerror = e => setReply("", "", e.error === "not-allowed"
      ? "Microphone blocked: allow it in the browser's address bar, or type instead." : "Didn't catch that (" + e.error + "). Try again or type.");
    recognizer.onend = () => {
      $("mic").classList.remove("listening"); recognizer = null;
      if (finalText) askEcoTwin(finalText.replace(/^\s*(hey\s+)?eco\s*twin[,\s]*/i, ""));
    };
    $("mic").classList.add("listening"); setReply("", "", "Listening…");
    recognizer.start();
  });
}

buildChart();
refresh();
setInterval(refresh, REFRESH_MS);
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Simulated response-crew wallet (phone-sized page at /wallet)
# ---------------------------------------------------------------------------
WALLET_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#0e1a16">
<title>Crew Wallet · EcoTwin</title>
<style>
  :root { --bg:#0e1a16; --card:#16251f; --ink:#eef4f1; --muted:#8fa69c; --line:#24372f; --ok:#3cc488; --warn:#e3a33a; --bad:#ef5b5b; --accent:#7fd6ae; }
  * { box-sizing: border-box; }
  html, body { margin: 0; background: #08100d; color: var(--ink);
    font: 15px/1.4 -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif; }
  /* On a laptop the page sits in a phone-sized frame; on a phone it fills the screen. */
  .phone { max-width: 400px; min-height: 100vh; margin: 0 auto; background: var(--bg); display: flex; flex-direction: column; }
  @media (min-width: 600px) {
    body { padding: 28px 0; }
    .phone { min-height: 780px; border-radius: 36px; border: 10px solid #1d2a25; overflow: hidden; box-shadow: 0 20px 60px rgba(0,0,0,.5); }
  }
  .sim { background: #2a3a33; color: var(--muted); font-size: 11px; text-align: center; padding: 6px 12px; letter-spacing: .3px; }
  header { padding: 22px 22px 6px; display: flex; align-items: center; gap: 12px; }
  .avatar { width: 40px; height: 40px; border-radius: 12px; background: linear-gradient(135deg, #2e7d5b, #7fd6ae); display: grid; place-items: center; font-weight: 800; color: #08100d; }
  .who b { display: block; font-size: 16px; }
  .who span { color: var(--muted); font-size: 12px; font-family: ui-monospace, Menlo, monospace; }
  .net { margin-left: auto; font-size: 11px; font-weight: 700; padding: 4px 8px; border-radius: 6px; border: 1px solid var(--warn); color: var(--warn); }
  .balance { text-align: center; padding: 28px 20px 18px; }
  .balance .lbl { color: var(--muted); font-size: 13px; }
  .balance .amt { font-size: 46px; font-weight: 800; font-variant-numeric: tabular-nums; margin: 4px 0; transition: color .4s; }
  .balance .amt.bump { color: var(--ok); }
  .balance .sub { color: var(--muted); font-size: 13px; }
  .sound { margin: 0 22px 14px; padding: 10px; border-radius: 12px; border: 1px dashed var(--line); background: none; color: var(--muted); font: inherit; width: calc(100% - 44px); cursor: pointer; }
  .sound.on { border-style: solid; border-color: var(--ok); color: var(--ok); }
  h2 { font-size: 12px; text-transform: uppercase; letter-spacing: .8px; color: var(--muted); margin: 6px 22px 8px; }
  ul { list-style: none; margin: 0; padding: 0 14px 24px; }
  li { background: var(--card); border: 1px solid var(--line); border-radius: 14px; padding: 12px 14px; margin-bottom: 8px; display: grid; grid-template-columns: 1fr auto; gap: 2px 10px; }
  li.new { animation: flash 2.4s ease-out; }
  @keyframes flash { 0% { border-color: var(--ok); box-shadow: 0 0 0 3px rgba(60,196,136,.35); } 100% { border-color: var(--line); box-shadow: none; } }
  .t { font-weight: 600; }
  .a { font-weight: 800; color: var(--ok); text-align: right; font-variant-numeric: tabular-nums; }
  .m { color: var(--muted); font-size: 12px; }
  .st { font-size: 11px; font-weight: 700; text-align: right; }
  .st.ok { color: var(--ok); } .st.pending { color: var(--warn); } .st.bad { color: var(--bad); } .st.mockst { color: var(--muted); }
  li a { color: var(--accent); font-size: 12px; grid-column: 1 / -1; text-decoration: none; }
  .empty { color: var(--muted); text-align: center; padding: 30px 20px; }
  .toast { position: fixed; left: 50%; top: 18px; transform: translate(-50%, -140%); width: min(360px, calc(100% - 24px));
    background: #f4faf7; color: #0e1a16; border-radius: 18px; padding: 12px 14px; display: flex; gap: 12px; align-items: center;
    box-shadow: 0 12px 30px rgba(0,0,0,.4); transition: transform .45s cubic-bezier(.2,.9,.3,1.2); z-index: 10; }
  .toast.show { transform: translate(-50%, 0); }
  .toast .ic { width: 36px; height: 36px; border-radius: 10px; background: #2e7d5b; color: #fff; display: grid; place-items: center; font-weight: 800; flex: none; }
  .toast b { display: block; }
  .toast span { font-size: 13px; color: #44544d; }
</style>
</head>
<body>
<div class="toast" id="toast"><div class="ic">◎</div><div><b id="toast-title"></b><span id="toast-body"></span></div></div>
<div class="phone">
  <div class="sim">Simulated crew wallet for the EcoTwin demo · not a real wallet app</div>
  <header>
    <div class="avatar">RC</div>
    <div class="who"><b>Response Crew</b><span id="addr">waiting for first bounty</span></div>
    <span class="net" id="net">MOCK</span>
  </header>
  <section class="balance">
    <div class="lbl">Bounties earned</div>
    <div class="amt" id="total">0.00 SOL</div>
    <div class="sub" id="count">No payouts yet</div>
  </section>
  <button class="sound" id="sound" type="button">Tap to turn on payment sounds</button>
  <h2>Activity</h2>
  <ul id="list"><li class="empty" style="display:block">Bounties appear here when EcoTwin resolves an incident.</li></ul>
</div>
<script>
const $ = id => document.getElementById(id);
const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const short = a => a ? a.slice(0, 4) + "…" + a.slice(-4) : "";
const when = t => new Date(t).toLocaleTimeString([], {hour: "2-digit", minute: "2-digit"});
const STATUS = {submitted: ["Confirming…", "pending"], processed: ["Confirming…", "pending"], confirmed: ["Confirmed", "ok"],
                finalized: ["Finalized", "ok"], failed: ["Failed", "bad"], expired: ["Not confirmed", "bad"], mock: ["Simulated", "mockst"]};
let seen = null, audio = null, toastTimer = null;

function chime() {
  if (!audio) return;
  const now = audio.currentTime;
  [[880, 0], [1320, 0.12]].forEach(([f, d]) => {
    const o = audio.createOscillator(), g = audio.createGain();
    o.type = "sine"; o.frequency.value = f; o.connect(g); g.connect(audio.destination);
    g.gain.setValueAtTime(0.0001, now + d); g.gain.exponentialRampToValueAtTime(0.3, now + d + 0.02);
    g.gain.exponentialRampToValueAtTime(0.0001, now + d + 0.35);
    o.start(now + d); o.stop(now + d + 0.4);
  });
}

function notify(p) {
  $("toast-title").textContent = `+${p.amount_sol} SOL received`;
  $("toast-body").textContent = `Bounty for incident #${p.incident_id}${p.zone_name ? " · " + p.zone_name : ""}`;
  $("toast").classList.add("show");
  clearTimeout(toastTimer); toastTimer = setTimeout(() => $("toast").classList.remove("show"), 5000);
  $("total").classList.add("bump"); setTimeout(() => $("total").classList.remove("bump"), 1500);
  chime();
  if (navigator.vibrate) navigator.vibrate([80, 60, 80]);
}

async function refresh() {
  try {
    const w = await (await fetch("/api/wallet", {cache: "no-store"})).json();
    $("total").textContent = w.total_sol.toFixed(2) + " SOL";
    $("count").textContent = w.count ? `${w.count} bount${w.count === 1 ? "y" : "ies"} received` : "No payouts yet";
    if (w.address) $("addr").textContent = short(w.address);
    const devnet = w.payouts.length && w.payouts[0].network === "devnet";
    $("net").textContent = devnet ? "DEVNET" : "MOCK";
    const fresh = seen === null ? [] : w.payouts.filter(p => !seen.has(p.payout_id));
    $("list").innerHTML = w.payouts.length ? w.payouts.map(p => {
      const [label, cls] = STATUS[p.status] || [p.status || "", "mockst"];
      return `<li class="${fresh.includes(p) ? "new" : ""}">
        <span class="t">Incident #${p.incident_id} resolved</span><span class="a">+${p.amount_sol} SOL</span>
        <span class="m">${esc(p.zone_name || "")}${p.severity_level ? " · " + esc(p.severity_level) : ""} · ${when(p.created_at)}</span>
        <span class="st ${cls}">${label}</span>
        ${p.explorer_url ? `<a href="${esc(p.explorer_url)}" target="_blank" rel="noopener">View on Solana Explorer ↗</a>` : ""}
      </li>`;
    }).join("") : '<li class="empty" style="display:block">Bounties appear here when EcoTwin resolves an incident.</li>';
    if (fresh.length) notify(fresh[0]);
    seen = new Set(w.payouts.map(p => p.payout_id));
  } catch (e) { /* keep last view; retry next tick */ }
}

$("sound").addEventListener("click", () => {
  try { audio = audio || new (window.AudioContext || window.webkitAudioContext)(); audio.resume(); } catch (e) {}
  $("sound").textContent = "Payment sounds on"; $("sound").classList.add("on"); chime();
});
refresh(); setInterval(refresh, 2000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    print(f"EcoTwin dashboard: http://{HOST}:{PORT}")
    print(f"Crew wallet:       http://{HOST}:{PORT}/wallet")
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
