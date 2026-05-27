#!/bin/sh
set -e

# Reject values containing characters that don't belong in an OIDC issuer:
# whitespace, control chars, anything that breaks safe interpolation in nasty
# ways. If the var has those, fail loudly.
contains_unsafe_chars() {
  printf '%s' "$1" | LC_ALL=C grep -q '[[:cntrl:][:space:]]'
}
if contains_unsafe_chars "${OIDC_ISSUER:-}"; then
  echo "ERROR: OIDC_ISSUER contains whitespace or control characters; refusing to start." >&2
  exit 1
fi

# Render CSP with the actual OIDC issuer origin so connect-src / frame-src can
# be tight (specific host) instead of broad `https:`. Falls back to `https:` if
# OIDC_ISSUER is not set, keeping the build usable without runtime config.
if [ -n "${OIDC_ISSUER:-}" ]; then
  # Strip path/query/fragment to get just `scheme://host[:port]`. Validate
  # that the input actually looks like a URL — if not, fall back to `https:`
  # rather than splatting a malformed value into the CSP header. The
  # character class [^/?#] stops at the first authority terminator, so an
  # OIDC_ISSUER like https://issuer.example.com?foo=bar still yields a
  # valid CSP source (https://issuer.example.com), not a malformed token.
  if echo "$OIDC_ISSUER" | grep -qE '^[A-Za-z][A-Za-z0-9+.-]*://[^[:space:]/?#]+'; then
    # Extraction MUST exclude whitespace too (matches validation). Otherwise
    # an OIDC_ISSUER like `https://issuer.example.com https:` would pass
    # validation (first authority looks fine) but the sed greedy match would
    # extract `https://issuer.example.com https:` — CSP directive values are
    # space-separated, so that smuggles in `https:` as a third allowed CSP
    # source, opening connect-src/frame-src to ALL https origins.
    CSP_REMOTE=$(echo "$OIDC_ISSUER" | sed -E 's|^([A-Za-z][A-Za-z0-9+.-]*://[^[:space:]/?#]+).*|\1|')
  else
    echo "WARN: OIDC_ISSUER='$OIDC_ISSUER' is not a well-formed URL — using https: in CSP"
    CSP_REMOTE="https:"
  fi
else
  CSP_REMOTE="https:"
fi
export CSP_REMOTE

# Substitute placeholders in nginx config template.
envsubst '${CSP_REMOTE}' < /etc/nginx/templates/default.conf.template \
  > /etc/nginx/conf.d/default.conf
echo "CSP connect-src/frame-src remote: $CSP_REMOTE"

exec "$@"
