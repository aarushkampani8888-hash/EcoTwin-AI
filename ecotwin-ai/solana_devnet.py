"""
EcoTwin AI: real bounty payments on Solana DEVNET (free test SOL, no real money).

Uses `solders` (official Solana types) to build and sign a transaction with
two instructions, a SystemProgram transfer and a Memo, and sends it over
plain JSON-RPC. Every payment is publicly visible on Solana Explorer.

Safety:
  * Devnet only. Refuses any RPC URL containing "mainnet" and verifies the
    cluster's genesis hash is devnet's before signing anything.
  * The treasury key lives in .solana_treasury.json next to this file
    (gitignored, owner-read-only). It is a throwaway devnet key: never put
    a real wallet's key here.

One-time setup: python3 solana_setup.py
"""

from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path

import requests
from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.message import Message
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction

log = logging.getLogger("ecotwin.solana")

RPC_URL = os.getenv("SOLANA_RPC_URL") or "https://api.devnet.solana.com"
DEVNET_GENESIS_HASH = "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG"
MEMO_PROGRAM_ID = Pubkey.from_string("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr")
EXPLORER_TX = "https://explorer.solana.com/tx/{sig}?cluster=devnet"
EXPLORER_ADDR = "https://explorer.solana.com/address/{addr}?cluster=devnet"
LAMPORTS_PER_SOL = 1_000_000_000

HERE = Path(__file__).resolve().parent
TREASURY_FILE = HERE / ".solana_treasury.json"
CREW_FILE = HERE / ".solana_crew.json"


class DevnetError(RuntimeError):
    pass


# -- JSON-RPC -----------------------------------------------------------------
def rpc(method: str, params: list | None = None, timeout: float = 8) -> object:
    resp = requests.post(RPC_URL, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []},
                         timeout=timeout)
    resp.raise_for_status()
    body = resp.json()
    if "error" in body:
        raise DevnetError(f"{method}: {body['error'].get('message', body['error'])}")
    return body["result"]


_verified = False


def ensure_devnet() -> None:
    """Hard stop unless we are provably talking to devnet."""
    global _verified
    if _verified:
        return
    if "mainnet" in RPC_URL.lower():
        raise DevnetError(f"Refusing to use a mainnet RPC URL: {RPC_URL}")
    genesis = rpc("getGenesisHash")
    if genesis != DEVNET_GENESIS_HASH:
        raise DevnetError(f"RPC at {RPC_URL} is not Solana devnet (genesis {genesis}); refusing to sign")
    _verified = True


# -- keys ---------------------------------------------------------------------
def save_keypair(path: Path, kp: Keypair) -> None:
    path.write_text(json.dumps(list(bytes(kp))))  # same format as the Solana CLI
    path.chmod(0o600)


def load_keypair(path: Path) -> Keypair:
    return Keypair.from_bytes(bytes(json.loads(path.read_text())))


def treasury() -> Keypair:
    if not TREASURY_FILE.exists():
        raise DevnetError("No devnet treasury yet. Run: python3 solana_setup.py")
    return load_keypair(TREASURY_FILE)


def recipient() -> Pubkey:
    wallet = (os.getenv("BOUNTY_RECIPIENT_WALLET") or "").strip()
    if wallet:
        try:
            return Pubkey.from_string(wallet)
        except ValueError as exc:
            raise DevnetError(f"BOUNTY_RECIPIENT_WALLET is not a valid Solana address: {wallet}") from exc
    if CREW_FILE.exists():
        return load_keypair(CREW_FILE).pubkey()
    raise DevnetError("No bounty recipient. Run solana_setup.py or set BOUNTY_RECIPIENT_WALLET")


def configured() -> bool:
    return TREASURY_FILE.exists()


# -- chain ops ------------------------------------------------------------------
def balance_lamports(pubkey: Pubkey) -> int:
    return int(rpc("getBalance", [str(pubkey), {"commitment": "confirmed"}])["value"])


def request_airdrop(pubkey: Pubkey, lamports: int) -> str:
    ensure_devnet()
    return str(rpc("requestAirdrop", [str(pubkey), lamports]))


def send_bounty(lamports: int, memo: str) -> dict:
    """Sign and submit transfer + memo. Returns as soon as the cluster accepts it;
    confirmation is checked later with signature_status()."""
    ensure_devnet()
    payer = treasury()
    to = recipient()
    blockhash = Hash.from_string(rpc("getLatestBlockhash", [{"commitment": "confirmed"}])["value"]["blockhash"])
    instructions = [
        transfer(TransferParams(from_pubkey=payer.pubkey(), to_pubkey=to, lamports=lamports)),
        Instruction(MEMO_PROGRAM_ID, memo.encode(), [AccountMeta(payer.pubkey(), True, False)]),
    ]
    tx = Transaction([payer], Message.new_with_blockhash(instructions, payer.pubkey(), blockhash), blockhash)
    # Preflight simulation is on, so an empty treasury fails here with a clear error.
    sig = rpc("sendTransaction", [base64.b64encode(bytes(tx)).decode(),
                                  {"encoding": "base64", "preflightCommitment": "confirmed"}])
    return {"signature": str(sig), "from": str(payer.pubkey()), "to": str(to),
            "explorer_url": EXPLORER_TX.format(sig=sig)}


def signature_status(sig: str) -> str | None:
    """'processed' | 'confirmed' | 'finalized' | 'failed', or None if not seen yet."""
    value = rpc("getSignatureStatuses", [[sig], {"searchTransactionHistory": True}])["value"][0]
    if value is None:
        return None
    if value.get("err"):
        return "failed"
    return value.get("confirmationStatus") or "processed"
