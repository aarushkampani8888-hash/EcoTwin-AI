"""
EcoTwin AI: Solana micro-bounty for resolved incidents.

When an incident is resolved, the response crew earns a small bounty scaled by
severity: a SystemProgram transfer plus a Memo naming the incident.

SOLANA_MODE=devnet  sends a real transaction on Solana DEVNET (free test SOL,
                    publicly visible on Solana Explorer). See solana_devnet.py.
SOLANA_MODE=mock    (default) builds the same transaction shape with a
                    deterministic fake signature and sends NOTHING anywhere.

If devnet is selected but unavailable (no wallet, empty treasury, network
down), the bounty falls back to a mock record so the demo never breaks.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger("ecotwin.bounty")

LAMPORTS_PER_SOL = 1_000_000_000
BOUNTY_SOL = {"LOW": 0.05, "MODERATE": 0.10, "CRITICAL": 0.25}
# Devnet bounties are smaller so one faucet top-up covers many demos.
DEVNET_BOUNTY_SOL = {"LOW": 0.01, "MODERATE": 0.02, "CRITICAL": 0.05}
SOLANA_MODE = (os.getenv("SOLANA_MODE") or "mock").strip().lower()

# Mock mode only: obviously-fake placeholder addresses (valid base58 characters).
TREASURY = os.getenv("BOUNTY_TREASURY_WALLET_MOCK") or "EcoTwinTreasury1111111111111111111111111111"
RECIPIENT = os.getenv("BOUNTY_RECIPIENT_WALLET_MOCK") or "ResponseCrew1111111111111111111111111111111"

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58encode(data: bytes) -> str:
    n = int.from_bytes(data, "big")
    out = ""
    while n:
        n, rem = divmod(n, 58)
        out = _B58[rem] + out
    pad = len(data) - len(data.lstrip(b"\0"))
    return "1" * pad + out


def issue_resolution_bounty(*, incident_id: int, node_id: int, zone: str, severity: str | None,
                            resolved_at: datetime) -> dict:
    if SOLANA_MODE == "devnet":
        try:
            return _devnet_bounty(incident_id=incident_id, node_id=node_id, zone=zone,
                                  severity=severity, resolved_at=resolved_at)
        except Exception as exc:
            log.warning("Devnet bounty failed, recording a mock instead: %s", exc)
            tx = _mock_bounty(incident_id=incident_id, node_id=node_id, zone=zone,
                              severity=severity, resolved_at=resolved_at)
            tx["fallback_reason"] = str(exc)[:300]
            return tx
    return _mock_bounty(incident_id=incident_id, node_id=node_id, zone=zone,
                        severity=severity, resolved_at=resolved_at)


def _devnet_bounty(*, incident_id: int, node_id: int, zone: str, severity: str | None,
                   resolved_at: datetime) -> dict:
    import solana_devnet as sd  # imported lazily: mock mode needs no Solana packages

    sol = DEVNET_BOUNTY_SOL.get((severity or "").upper(), DEVNET_BOUNTY_SOL["LOW"])
    memo = f"EcoTwin bounty: incident #{incident_id} resolved at node {node_id} ({zone})"
    sent = sd.send_bounty(int(sol * LAMPORTS_PER_SOL), memo)
    log.info("[SOLANA DEVNET] %.2f SOL to %s for incident #%s | %s",
             sol, sent["to"][:8] + "…", incident_id, sent["explorer_url"])
    return {
        "network": "devnet",
        "submitted": True,
        "status": "submitted",
        "fee_payer": sent["from"],
        "recipient": sent["to"],
        "amount_sol": sol,
        "memo": memo,
        "signature": sent["signature"],
        "explorer_url": sent["explorer_url"],
        "created_at": resolved_at.astimezone(timezone.utc).isoformat(),
    }


def _mock_bounty(*, incident_id: int, node_id: int, zone: str, severity: str | None,
                 resolved_at: datetime) -> dict:
    sol = BOUNTY_SOL.get((severity or "").upper(), BOUNTY_SOL["LOW"])
    memo = f"EcoTwin bounty: incident #{incident_id} resolved at node {node_id} ({zone})"
    tx = {
        "network": "mock",
        "submitted": False,
        "fee_payer": TREASURY,
        "instructions": [
            {"program": "SystemProgram", "type": "transfer", "from": TREASURY, "to": RECIPIENT,
             "lamports": int(sol * LAMPORTS_PER_SOL)},
            {"program": "Memo", "data": memo},
        ],
        "amount_sol": sol,
        "recipient": RECIPIENT,
        "memo": memo,
        "created_at": resolved_at.astimezone(timezone.utc).isoformat(),
    }
    # Deterministic stand-in for a 64-byte ed25519 signature, so the same
    # incident always maps to the same mock "transaction id".
    digest = hashlib.sha512(json.dumps(tx, sort_keys=True).encode()).digest()
    tx["signature"] = b58encode(digest)
    tx["status"] = "mock"
    tx["explorer_url"] = None
    log.info("[MOCK SOLANA] %.2f SOL to %s for incident #%s | sig %s… | not submitted",
             sol, RECIPIENT[:8] + "…", incident_id, tx["signature"][:16])
    return tx


def check_confirmation(signature: str) -> str | None:
    """Devnet only: current status of a submitted bounty transaction."""
    import solana_devnet as sd
    return sd.signature_status(signature)
