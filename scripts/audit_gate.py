"""Apply the scan gate to a pip-audit JSON report.

Shared by dev.sh and dev.ps1 so both apply one rule: a vulnerability with a
fix version is fatal (exit 1); one with no fix warns and passes, because there
is nothing to act on. Usage: python audit_gate.py <report.json>
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def classify(report: dict) -> tuple[list[str], list[str]]:
    """Return (fixable, unfixable) one-line descriptions from a pip-audit report."""
    fixable: list[str] = []
    unfixable: list[str] = []
    for dep in report.get("dependencies", []):
        name = dep.get("name", "?")
        version = dep.get("version", "?")
        for vuln in dep.get("vulns", []):
            fixes = vuln.get("fix_versions") or []
            line = f"{name}=={version}: {vuln.get('id', '?')}"
            if fixes:
                fixable.append(f"{line} -> fixed in {', '.join(fixes)}")
            else:
                unfixable.append(f"{line} (no fix released)")
    return fixable, unfixable


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: audit_gate.py <pip-audit-report.json>", file=sys.stderr)
        return 2
    path = Path(argv[1])
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"error: cannot read pip-audit report {path}: {exc}", file=sys.stderr)
        return 2
    fixable, unfixable = classify(report)
    for line in unfixable:
        print(f"warn: {line}")
    for line in fixable:
        print(f"error: {line}")
    if fixable:
        noun = "vulnerability" if len(fixable) == 1 else "vulnerabilities"
        print(
            f"error: {len(fixable)} fixable {noun}; bump the dependency in pyproject.toml, "
            "run 'uv lock', then re-run the scan task"
        )
        return 1
    print(f"ok: no fixable vulnerabilities ({len(unfixable)} unfixable warned)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
