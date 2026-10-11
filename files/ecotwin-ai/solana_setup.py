"""
One-time setup for EcoTwin's Solana DEVNET bounties.

    python3 solana_setup.py            create wallets (if needed) and show balances
    python3 solana_setup.py --airdrop  also ask devnet for 1 free test SOL

Creates two throwaway devnet wallets next to this file:
    .solana_treasury.json   pays the bounties (fund this one)
    .solana_crew.json       receives them (skipped if BOUNTY_RECIPIENT_WALLET is set)
Both are gitignored. Devnet SOL is free test currency with no value.
"""

from __future__ import annotations

import argparse
import time

from dotenv import load_dotenv

load_dotenv()

import solana_devnet as sd  # noqa: E402
from solders.keypair import Keypair  # noqa: E402


def ensure_wallet(path, label: str) -> Keypair:
    if path.exists():
        kp = sd.load_keypair(path)
        print(f"{label}: {kp.pubkey()}  (existing, {path.name})")
    else:
        kp = Keypair()
        sd.save_keypair(path, kp)
        print(f"{label}: {kp.pubkey()}  (new, saved to {path.name})")
    return kp


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--airdrop", action="store_true", help="request 1 devnet SOL for the treasury")
    args = ap.parse_args()

    print(f"RPC: {sd.RPC_URL}")
    sd.ensure_devnet()
    print("Verified: this is Solana devnet.\n")

    treasury = ensure_wallet(sd.TREASURY_FILE, "Treasury (pays bounties)")
    try:
        crew = sd.recipient()
        if sd.CREW_FILE.exists() and crew == sd.load_keypair(sd.CREW_FILE).pubkey():
            print(f"Response crew (receives):  {crew}  (existing, {sd.CREW_FILE.name})")
        else:
            print(f"Response crew (receives):  {crew}  (from BOUNTY_RECIPIENT_WALLET)")
    except sd.DevnetError:
        crew = ensure_wallet(sd.CREW_FILE, "Response crew (receives)").pubkey()

    if args.airdrop:
        try:
            sig = sd.request_airdrop(treasury.pubkey(), 1 * sd.LAMPORTS_PER_SOL)
            print(f"\nAirdrop requested: {sd.EXPLORER_TX.format(sig=sig)}")
            time.sleep(8)
        except Exception as exc:
            print(f"\nAirdrop refused ({exc}).\nDevnet airdrops are often rate-limited; use the web faucet below.")

    bal = sd.balance_lamports(treasury.pubkey()) / sd.LAMPORTS_PER_SOL
    print(f"\nTreasury balance: {bal:.4f} devnet SOL")
    print(f"Treasury on Explorer: {sd.EXPLORER_ADDR.format(addr=treasury.pubkey())}")
    print(f"Crew on Explorer:     {sd.EXPLORER_ADDR.format(addr=crew)}")
    if bal < 0.2:
        print("\nNext: fund the treasury with free test SOL.")
        print("  1. Open https://faucet.solana.com")
        print(f"  2. Paste the treasury address: {treasury.pubkey()}")
        print("  3. Choose devnet, request SOL, then run this script again to check the balance.")
    print("\nThen set SOLANA_MODE=devnet in .env and restart ecotwin_core.py.")


if __name__ == "__main__":
    try:
        main()
    except sd.DevnetError as exc:
        raise SystemExit(f"Stopped: {exc}")
    except Exception as exc:  # e.g. no network
        raise SystemExit(f"Could not reach Solana devnet at {sd.RPC_URL}: {exc}")
