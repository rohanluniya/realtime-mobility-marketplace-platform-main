"""Environment capability detector recommending local_native vs docker execution profile."""

from __future__ import annotations

import json
import platform
import shutil
import sys
from pathlib import Path


def main() -> None:
    docker_ok = shutil.which("docker") is not None
    java_ok = shutil.which("java") is not None
    uv_ok = shutil.which("uv") is not None
    profile = "docker_distributed" if docker_ok else "local_native"
    report = {
        "os": f"{platform.system()} {platform.release()}",
        "arch": platform.machine(),
        "python": sys.version.split()[0],
        "tools": {
            "uv": uv_ok,
            "docker": docker_ok,
            "java": java_ok,
            "git": shutil.which("git") is not None,
        },
        "recommended_profile": profile,
        "recommended_command": (
            "docker compose -f docker-compose.standalone.yml up --build"
            if docker_ok
            else "./scripts/run_portable.sh local"
        ),
        "repo_root": str(Path(__file__).resolve().parent.parent),
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
