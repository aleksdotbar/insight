#!/usr/bin/env python3
"""Single-tenant_id contract guard (ADR-0003, insight#2196).

Imports the canonical broker realm into a throwaway Keycloak and asserts the
tenant claim contract the platform relies on (DD-AUTH-04: exactly one string
tenant, or none — never two, never an ambiguous source):

1. no tenant source          -> token carries NO tenant_id (fail closed)
2. one tenant group          -> token tenant_id == that group's value (string)
3. two tenant groups         -> AMBIGUOUS: Keycloak silently emits one of
                                them, so the token cannot reveal the problem;
                                the guard's membership check must flag it
4. user attribute + group    -> AMBIGUOUS: the group value shadows the
                                pinned attribute; flagged the same way

Requires docker and PyYAML. Boots quay.io/keycloak/keycloak, converts the
canonical realm YAML to a realm representation (env placeholders substituted
with synthetic values), creates it via the admin API, and evaluates example
tokens. Exit 0 = contract holds.
"""

# ruff: noqa: T201  — stdout IS this script's CI report.

import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[4]
CANONICAL_REALM = REPO_ROOT / "deploy/gitops/environments/local/keycloak/realms/insight-broker.yaml"
KC_IMAGE = "quay.io/keycloak/keycloak:26.4"
KC_PORT = 18086
BASE = f"http://127.0.0.1:{KC_PORT}"
CONTAINER = "tenant-guard-kc"

PLACEHOLDER = re.compile(r"\$\(env:([A-Za-z_][A-Za-z0-9_]*)\)")
SYNTHETIC = {
    "INSIGHT_AUTHENTICATOR_CLIENT_SECRET": "guard-secret",
    "INSIGHT_AUTHENTICATOR_REDIRECT_URI": "http://localhost/auth/callback",
    "INSIGHT_TENANT_ID": "00000000-0000-0000-0000-00000000feed",
}

TENANT_A = "aaaaaaaa-0000-0000-0000-000000000001"
TENANT_B = "bbbbbbbb-0000-0000-0000-000000000002"
TENANT_ATTR = "cccccccc-0000-0000-0000-000000000003"


def sh(*args: str) -> None:
    subprocess.run(args, check=True, capture_output=True)


def substitute(node):
    if isinstance(node, str):
        return PLACEHOLDER.sub(lambda m: SYNTHETIC.get(m.group(1), f"missing-{m.group(1)}"), node)
    if isinstance(node, list):
        return [substitute(v) for v in node]
    if isinstance(node, dict):
        return {k: substitute(v) for k, v in node.items()}
    return node


class Admin:
    def __init__(self) -> None:
        self.token = self._call(
            "/realms/master/protocol/openid-connect/token",
            method="POST",
            raw=urllib.parse.urlencode(
                {"grant_type": "password", "client_id": "admin-cli", "username": "admin", "password": "admin"}
            ).encode(),
        )["access_token"]

    def _call(self, path: str, method: str = "GET", body=None, raw: bytes | None = None):
        headers = {"Content-Type": "application/x-www-form-urlencoded" if raw else "application/json"}
        if hasattr(self, "token"):
            headers["Authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(
            f"{BASE}{path}",
            method=method,
            headers=headers,
            data=raw if raw is not None else (json.dumps(body).encode() if body is not None else None),
        )
        with urllib.request.urlopen(req) as resp:
            payload = resp.read()
            return json.loads(payload) if payload else None

    def realm(self, path: str, method: str = "GET", body=None):
        return self._call(f"/admin/realms/insight-broker{path}", method, body)


def wait_for_keycloak(timeout_s: int = 180) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(f"{BASE}/realms/master/.well-known/openid-configuration", timeout=3)
            return
        except (urllib.error.URLError, OSError):
            time.sleep(3)
    raise TimeoutError("Keycloak did not come up")


def tenant_claim(admin: Admin, client_id: str, user_id: str):
    token = admin.realm(f"/clients/{client_id}/evaluate-scopes/generate-example-id-token?userId={user_id}&scope=openid")
    return token.get("tenant_id")


def tenant_group_count(admin: Admin, user_id: str) -> int:
    groups = admin.realm(f"/users/{user_id}/groups")
    with_tenant = 0
    for g in groups:
        detail = admin.realm(f"/groups/{g['id']}")
        if (detail.get("attributes") or {}).get("tenant_id"):
            with_tenant += 1
    return with_tenant


def has_pinned_attribute(admin: Admin, user_id: str) -> bool:
    user = admin.realm(f"/users/{user_id}")
    return bool((user.get("attributes") or {}).get("tenant_id"))


def unambiguous_tenant_source(admin: Admin, user_id: str) -> bool:
    """The invariant: exactly one tenant source. Token inspection cannot see
    a violation (Keycloak silently emits one value), so this checks sources."""
    groups = tenant_group_count(admin, user_id)
    attr = has_pinned_attribute(admin, user_id)
    return (groups + (1 if attr else 0)) <= 1


def main() -> int:
    failures: list[str] = []

    def check(name: str, ok: bool, detail: str) -> None:
        print(f"{'ok  ' if ok else 'FAIL'} {name}: {detail}")
        if not ok:
            failures.append(name)

    realm = substitute(yaml.safe_load(CANONICAL_REALM.read_text()))

    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)
    sh(
        "docker",
        "run",
        "-d",
        "--name",
        CONTAINER,
        "-p",
        f"127.0.0.1:{KC_PORT}:8080",
        "-e",
        "KC_BOOTSTRAP_ADMIN_USERNAME=admin",
        "-e",
        "KC_BOOTSTRAP_ADMIN_PASSWORD=admin",
        KC_IMAGE,
        "start-dev",
    )
    try:
        wait_for_keycloak()
        admin = Admin()
        admin._call("/admin/realms", "POST", realm)

        profile = admin.realm("/users/profile")
        profile["unmanagedAttributePolicy"] = "ADMIN_EDIT"
        admin.realm("/users/profile", "PUT", profile)

        admin.realm("/groups", "POST", {"name": "tenant-a", "attributes": {"tenant_id": [TENANT_A]}})
        admin.realm("/groups", "POST", {"name": "tenant-b", "attributes": {"tenant_id": [TENANT_B]}})
        groups = {g["name"]: g["id"] for g in admin.realm("/groups")}
        admin.realm(
            "/users",
            "POST",
            {"username": "guard@example.com", "email": "guard@example.com", "enabled": True, "emailVerified": True},
        )
        user = admin.realm("/users?username=guard@example.com&exact=true")[0]["id"]
        client = admin.realm("/clients?clientId=insight-authenticator")[0]["id"]

        claim = tenant_claim(admin, client, user)
        check("fail-closed", claim is None, f"no tenant source -> claim {claim!r}")

        admin.realm(f"/users/{user}/groups/{groups['tenant-a']}", "PUT")
        claim = tenant_claim(admin, client, user)
        check("group-translation", claim == TENANT_A, f"one group -> claim {claim!r}")
        check("single-source(1 group)", unambiguous_tenant_source(admin, user), "one tenant source")

        admin.realm(f"/users/{user}/groups/{groups['tenant-b']}", "PUT")
        claim = tenant_claim(admin, client, user)
        check(
            "two-groups-detected",
            not unambiguous_tenant_source(admin, user),
            f"two tenant groups; token silently emits {claim!r} — sources check must flag it",
        )

        u = admin.realm(f"/users/{user}")
        u["attributes"] = {"tenant_id": [TENANT_ATTR]}
        admin.realm(f"/users/{user}", "PUT", u)
        claim = tenant_claim(admin, client, user)
        check(
            "mixed-source-detected",
            not unambiguous_tenant_source(admin, user),
            f"attribute + groups; token emits {claim!r} (group shadows pin) — flagged",
        )

        claim = tenant_claim(admin, client, user)
        check("claim-is-scalar", claim is None or isinstance(claim, str), f"claim type {type(claim).__name__}")
    finally:
        subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)

    if failures:
        print(f"\ntenant-translation guard FAILED: {failures}")
        return 1
    print("\ntenant-translation guard OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
