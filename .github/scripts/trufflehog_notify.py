#!/usr/bin/env python3
"""Turn findings the nightly scan reports into issues assigned to their author.

The nightly jobs never fail a run, so without this a finding outside the
baseline is only a line in a summary nobody opens. One issue per fingerprint:
they close independently, and an open one suppresses the next night's duplicate.

Values are never included — the issue names the detector, the path and the
commit, which is what the assignee needs to find it.

    trufflehog_notify.py NEW.json REPO AUTHOR_MAP [--dry-run]
"""
import json
import re
import subprocess
import sys


def gh(*args: str) -> str:
    r = subprocess.run(["gh", *args], capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else ""


def read_author_map(path: str) -> dict[str, str]:
    """{email: login} for addresses GitHub cannot attribute on its own."""
    entries = {}
    try:
        fh = open(path, encoding="utf-8")
    except FileNotFoundError:
        return entries
    with fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                entries[parts[0].lower()] = parts[1]
    return entries


def address_of(raw: str) -> str:
    """The address out of a `Name <addr>` author field."""
    m = re.search(r"<([^>]+)>", raw or "")
    return (m.group(1) if m else raw or "").strip().lower()


def author_of(repo: str, sha: str, email: str, cache: dict[str, str], amap: dict[str, str]) -> str:
    """GitHub login for a commit, or "" when neither route resolves it.

    The commit's own author is only known to GitHub when the address in it is
    attached to an account, which a work address often is not. The pull request
    the commit arrived with names an account either way, so it is the second
    route. The author map is the third, for addresses neither can attribute.
    """
    if not sha:
        return ""
    if sha not in cache:
        login = gh("api", f"repos/{repo}/commits/{sha}", "--jq", ".author.login // empty")
        if not login:
            login = gh("api", f"repos/{repo}/commits/{sha}/pulls", "--jq", ".[0].user.login // empty")
        if not login:
            login = amap.get(address_of(email))
        cache[sha] = login or ""
    return cache[sha]


def already_open(repo: str, fingerprint: str) -> bool:
    found = gh("issue", "list", "--repo", repo, "--state", "open",
               "--search", f"{fingerprint} in:title", "--json", "number", "--jq", ".[0].number // empty")
    return bool(found)


def body_for(f: dict, login: str) -> str:
    who = f"@{login}" if login else (
        f"{f['email']} (no GitHub account linked to that address)" if f.get("email")
        else f"the author of `{f['commit'][:8]}`")
    return "\n".join([
        f"The nightly secret scan reports a finding that is not in `.github/trufflehog-allowlist.txt`.",
        "",
        f"| Detector | Verified | Commit | Path | Fingerprint |",
        f"|---|---|---|---|---|",
        f"| `{f['detector']}` | {'yes' if f['verified'] else 'no'} | `{f['commit'][:8]}` |"
        f" `{f['file']}`{':' + str(f['line']) if f['line'] else ''} | `{f['fp']}` |",
        "",
        f"It arrived with a commit by {who}. The value is deliberately not repeated here;"
        " read it from the commit.",
        "",
        "Two ways to close this:",
        "",
        "- **It is a credential.** Revoke and rotate it. Removing the commit is not remediation —"
        " assume it was compromised the moment it was pushed.",
        "- **It is a false positive.** Add the fingerprint to `.github/trufflehog-allowlist.txt`"
        " with a reason, in a pull request, so the claim is reviewed.",
    ])


def main() -> int:
    new_json, repo, author_map = sys.argv[1], sys.argv[2], sys.argv[3]
    dry = "--dry-run" in sys.argv[4:]

    with open(new_json, encoding="utf-8") as fh:
        findings = json.load(fh)

    cache: dict[str, str] = {}
    amap = read_author_map(author_map)
    created = skipped = 0
    for f in findings:
        if already_open(repo, f["fp"]):
            skipped += 1
            continue
        login = author_of(repo, f.get("commit", ""), f.get("email", ""), cache, amap)
        title = f"Possible secret in {f['file']} ({f['fp']})"
        if dry:
            print(f"would open: {title}  -> assignee: {login or '(none)'}")
            created += 1
            continue
        args = ["issue", "create", "--repo", repo, "--title", title, "--body", body_for(f, login)]
        if login:
            args += ["--assignee", login]
        print(gh(*args) or f"failed to open an issue for {f['fp']}")
        created += 1

    print(f"{created} issue(s) {'would be ' if dry else ''}opened, {skipped} already tracked")
    return 0


if __name__ == "__main__":
    sys.exit(main())
