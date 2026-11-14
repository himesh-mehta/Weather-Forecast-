"""
Pull the latest forecasts published by the GitHub Actions workflow (the `data` branch) into this
folder, so a local dashboard shows them without fetching anything itself.

    python sync_data.py            # once
    python sync_data.py --every 15 # keep syncing every 15 minutes (Ctrl+C to stop)

Uses `git show`, so nothing is staged or committed on your current branch. The running dashboard
picks up the new files automatically (it reloads when the file changes).
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
FILES = ["latest_forecasts.json", "live_conditions.json"]


def git(*args) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=BASE_DIR, capture_output=True)


def sync_once(remote: str) -> bool:
    r = git("fetch", "--depth=1", remote, "data")
    if r.returncode != 0:
        print(f"Could not fetch '{remote}/data': {r.stderr.decode(errors='replace').strip()}")
        return False
    for name in FILES:
        r = git("show", f"FETCH_HEAD:{name}")
        if r.returncode != 0:
            print(f"  {name}: not on the data branch (skipped)")
            continue
        tmp = BASE_DIR / f"{name}.tmp"
        tmp.write_bytes(r.stdout)
        os.replace(tmp, BASE_DIR / name)  # atomic: the dashboard never reads a half-written file
        print(f"  {name}: updated ({len(r.stdout) / 1e6:.1f} MB)")
    stamp = git("log", "-1", "--format=%s", "FETCH_HEAD").stdout.decode(errors="replace").strip()
    print(f"Synced: {stamp}")
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--remote", default="origin")
    ap.add_argument("--every", type=float, metavar="MINUTES", help="repeat every N minutes")
    a = ap.parse_args()
    if not a.every:
        return 0 if sync_once(a.remote) else 1
    while True:
        sync_once(a.remote)
        time.sleep(a.every * 60)


if __name__ == "__main__":
    sys.exit(main())
