"""Package version from setuptools-scm (git tags) and git commit."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

try:
    from importlib.metadata import version

    _v = version("earthcatalog")
except Exception:
    _v = "0.0.0"

__version__ = _v

# Extract commit hash from setuptools-scm version string (e.g. "0.6.1.dev0+g3eff032.d20260502")
m = re.search(r"\+g([a-f0-9]+)", _v)
if m:
    __commit__ = m.group(1)
else:
    try:
        repo = Path(__file__).resolve().parent
        __commit__ = (
            subprocess.check_output(
                ["git", "-C", str(repo), "rev-parse", "--short", "HEAD"],
                stderr=subprocess.DEVNULL,
                timeout=2,
            )
            .decode()
            .strip()
        )
    except Exception:
        __commit__ = None

# Embed commit hash in __version__ so ec.__version__ always identifies the commit
if __commit__ and "+" not in _v:
    __version__ = f"{_v}+{__commit__}"

__version_full__ = f"{_v} ({__commit__})" if __commit__ else _v
