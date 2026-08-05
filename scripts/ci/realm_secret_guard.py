#!/usr/bin/env python3
"""Guard: broker realm YAML carries no secret material (ADR-0003, #2195).

Realm content under deploy/gitops/environments/<env>/keycloak/realms/ is
applied by keycloak-config-cli with environment-variable substitution from a
sealed Secret. Every credential-bearing field in those files must therefore
hold a `$(env:VAR)` placeholder — a literal value is a committed secret.

Checks, per realm file:

1. Every sensitive key (client `secret`, identity-provider `clientSecret`,
   any `password`, LDAP `bindCredential`, and the `value` of a
   `type: password`/`type: secret` credentials entry) resolves to a
   `$(env:VAR)` placeholder, not a literal.
2. No `$(env:` sequence appears inside a YAML comment: config-cli variable
   substitution scans the whole file including comments, and an unresolvable
   placeholder anywhere fails the realm import at deploy time (Phase-0
   finding on #2194).

A file that fails to parse as YAML is an error, never a pass.
"""

# ruff: noqa: T201  — stdout IS this script's CI report (cf. connector_wiring.py).

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

REALM_GLOB = "deploy/gitops/environments/*/keycloak/realms/*.yaml"

# Keys whose value is a credential wherever they appear in the realm-export
# schema. `value` is sensitive only inside a credentials entry, handled below.
SENSITIVE_KEYS = frozenset({"secret", "clientSecret", "password", "bindCredential"})

PLACEHOLDER = re.compile(r"^\$\(env:[A-Za-z_][A-Za-z0-9_]*\)$")


def is_placeholder(value: object) -> bool:
    return isinstance(value, str) and bool(PLACEHOLDER.match(value))


def walk(node: object, path: str, findings: list[str]) -> None:
    if isinstance(node, dict):
        node_type = node.get("type")
        for key, value in node.items():
            key_path = f"{path}.{key}" if path else str(key)
            sensitive = key in SENSITIVE_KEYS or (
                key == "value" and isinstance(node_type, str) and node_type.lower() in {"password", "secret"}
            )
            if sensitive and value is not None and not isinstance(value, (dict, list)):
                if not is_placeholder(value):
                    findings.append(f"{key_path}: literal value in credential field (use an env placeholder)")
            else:
                walk(value, key_path, findings)
    elif isinstance(node, list):
        for i, item in enumerate(node):
            walk(item, f"{path}[{i}]", findings)


def check_file(filename: Path) -> list[str]:
    findings: list[str] = []
    text = filename.read_text(encoding="utf-8")

    for lineno, line in enumerate(text.splitlines(), start=1):
        comment = line.split("#", 1)
        if len(comment) == 2 and "$(env:" in comment[1]:
            findings.append(f"line {lineno}: placeholder syntax inside a YAML comment breaks the config-cli import")

    try:
        documents = list(yaml.safe_load_all(text))
    except yaml.YAMLError as exc:
        findings.append(f"unparseable YAML: {exc}")
        return findings

    for doc in documents:
        walk(doc, "", findings)
    return findings


def main() -> int:
    files = sorted(Path().glob(REALM_GLOB))
    if not files:
        print(f"realm-secret-guard: no realm files match {REALM_GLOB} — nothing to check")
        return 0

    failed = False
    for filename in files:
        findings = check_file(filename)
        if findings:
            failed = True
            for finding in findings:
                print(f"FAIL {filename}: {finding}")
        else:
            print(f"ok   {filename}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
