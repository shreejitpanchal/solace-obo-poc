"""scripts/audit_gate.py is the scan gate shared by dev.sh and dev.ps1."""

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "audit_gate.py"
spec = importlib.util.spec_from_file_location("audit_gate", SCRIPT)
audit_gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit_gate)


def report(*deps):
    return {"dependencies": list(deps)}


def dep(name, version, *vulns):
    return {"name": name, "version": version, "vulns": list(vulns)}


def test_fixable_vulnerability_is_fatal(tmp_path, capsys):
    path = tmp_path / "r.json"
    vuln = {"id": "GHSA-1", "fix_versions": ["1.1"]}
    path.write_text(json.dumps(report(dep("foo", "1.0", vuln))))
    assert audit_gate.main(["audit_gate.py", str(path)]) == 1
    out = capsys.readouterr().out
    assert "error: foo==1.0: GHSA-1 -> fixed in 1.1" in out
    assert "uv lock" in out  # the fix is named


def test_unfixable_vulnerability_warns_and_passes(tmp_path, capsys):
    path = tmp_path / "r.json"
    path.write_text(json.dumps(report(dep("foo", "1.0", {"id": "GHSA-2", "fix_versions": []}))))
    assert audit_gate.main(["audit_gate.py", str(path)]) == 0
    out = capsys.readouterr().out
    assert "warn: foo==1.0: GHSA-2 (no fix released)" in out
    assert "ok:" in out


def test_clean_report_passes(tmp_path):
    path = tmp_path / "r.json"
    path.write_text(json.dumps(report(dep("foo", "1.0"))))
    assert audit_gate.main(["audit_gate.py", str(path)]) == 0


def test_mixed_report_is_fatal_but_lists_both(tmp_path, capsys):
    path = tmp_path / "r.json"
    path.write_text(
        json.dumps(
            report(
                dep("a", "1", {"id": "X", "fix_versions": ["2"]}),
                dep("b", "1", {"id": "Y", "fix_versions": None}),
            )
        )
    )
    assert audit_gate.main(["audit_gate.py", str(path)]) == 1
    out = capsys.readouterr().out
    assert "warn: b==1: Y" in out and "error: a==1: X" in out


@pytest.mark.parametrize("argv", [["audit_gate.py"], ["audit_gate.py", "a", "b"]])
def test_usage_error(argv):
    assert audit_gate.main(argv) == 2


def test_unreadable_report_is_actionable(tmp_path, capsys):
    missing = tmp_path / "missing.json"
    assert audit_gate.main(["audit_gate.py", str(missing)]) == 2
    assert str(missing) in capsys.readouterr().err
