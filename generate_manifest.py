#!/usr/bin/env python3
"""Build data/fo/manifest.json listing available dates and per-date symbols.

Lets the frontend fetch one small JSON file (via raw.githubusercontent.com)
instead of calling the GitHub contents API, which is rate limited to 60
requests/hour for unauthenticated clients.
"""

import json
import os
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FO_DIR = os.path.join(BASE_DIR, "data", "fo")
MANIFEST_PATH = os.path.join(FO_DIR, "manifest.json")

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def build_manifest():
    dates = sorted(
        name for name in os.listdir(FO_DIR)
        if DATE_RE.match(name) and os.path.isdir(os.path.join(FO_DIR, name))
    )

    symbols_by_date = {}
    for d in dates:
        symbols_by_date[d] = sorted(
            f[:-4] for f in os.listdir(os.path.join(FO_DIR, d)) if f.endswith(".csv")
        )

    return {"dates": dates, "symbols": symbols_by_date}


if __name__ == "__main__":
    manifest = build_manifest()
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, separators=(",", ":"))
    print(f"Wrote {MANIFEST_PATH}: {len(manifest['dates'])} dates")
