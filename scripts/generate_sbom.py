"""Generate CycloneDX 1.5 Software Bill of Materials (SBOM) for Python dependencies."""

from __future__ import annotations

import importlib.metadata as meta
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path


def main() -> None:
    components = []
    for dist in sorted(meta.distributions(), key=lambda d: (d.metadata["Name"] or "").lower()):
        name = dist.metadata["Name"] or "unknown"
        version = dist.version
        components.append(
            {
                "type": "library",
                "name": name,
                "version": version,
                "purl": f"pkg:pypi/{name.lower()}@{version}",
            }
        )

    sbom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:{uuid.uuid4()}",
        "version": 1,
        "metadata": {
            "timestamp": datetime.now(UTC).isoformat(),
            "component": {
                "type": "application",
                "name": "realtime-mobility-marketplace-platform",
                "version": "1.0.0",
                "licenses": [{"license": {"id": "Apache-2.0"}}],
            },
        },
        "components": components,
    }
    out_path = Path(__file__).resolve().parent.parent / "results" / "security" / "sbom.cdx.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(sbom, indent=2), encoding="utf-8")
    print(json.dumps({"sbom_path": str(out_path.name), "components": len(components)}, indent=2))


if __name__ == "__main__":
    main()
