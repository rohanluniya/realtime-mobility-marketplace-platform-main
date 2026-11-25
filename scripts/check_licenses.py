"""License compliance verification script auditing all installed Python dependencies."""

from __future__ import annotations

import importlib.metadata as meta
import json
import sys
from pathlib import Path

FORBIDDEN_MARKERS = ("GPL", "AGPL", "SSPL", "BUSL", "BSL", "PROPRIETARY")
ALLOWED_HINTS = (
    "MIT",
    "APACHE",
    "BSD",
    "ISC",
    "MPL",
    "PSF",
    "PYTHON",
    "ZLIB",
    "UNLICENSE",
    "CC0",
    "HPND",
    "LGPL",
    "0BSD",
)


def _extract_license(dist: meta.Distribution) -> str:
    lic = dist.metadata.get("License-Expression") or dist.metadata.get("License") or ""
    if lic and lic.strip().upper() != "UNKNOWN":
        return lic.strip().splitlines()[0][:120]
    for c in dist.metadata.get_all("Classifier") or []:
        if c.startswith("License ::"):
            return c.split("::")[-1].strip()
    return "UNKNOWN"


def main() -> None:
    packages = []
    flagged = []
    for dist in sorted(meta.distributions(), key=lambda d: (d.metadata["Name"] or "").lower()):
        name = dist.metadata["Name"] or "unknown"
        version = dist.version
        lic = _extract_license(dist)
        upper = lic.upper()
        is_copyleft_block = any(m in upper for m in FORBIDDEN_MARKERS) and "LGPL" not in upper
        if is_copyleft_block:
            flagged.append({"package": name, "version": version, "license": lic})
        packages.append({"package": name, "version": version, "license": lic})

    out = {
        "total_packages_audited": len(packages),
        "flagged_incompatible": flagged,
        "status": "PASS" if not flagged else "FAIL",
        "packages": packages,
    }
    out_path = Path(__file__).resolve().parent.parent / "results" / "security" / "license_audit.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps({"total_packages_audited": len(packages), "flagged": len(flagged), "status": out["status"]}, indent=2))
    if flagged:
        sys.exit(1)


if __name__ == "__main__":
    main()
