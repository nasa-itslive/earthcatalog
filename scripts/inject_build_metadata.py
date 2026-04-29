#!/usr/bin/env python3
"""Pre-build hook to inject git commit hash and info into docs."""

import subprocess
from pathlib import Path


def main() -> None:
    root = Path(__file__).parent.parent.resolve()

    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except Exception:
        commit = "dev"

    try:
        date = subprocess.run(
            ["git", "log", "-1", "--format=%ad", "--date=short"],
            cwd=root,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except Exception:
        date = "unknown"

    index_path = root / "docs/site/index.md"
    content = index_path.read_text()
    content = content.replace("{commit}", commit).replace("{date}", date)
    index_path.write_text(content)

    print(f"Injected commit: {commit} ({date})")


if __name__ == "__main__":
    main()
