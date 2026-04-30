#!/bin/sh
set -e

# Escape a value for safe interpolation inside a JavaScript string literal
# wrapped in double quotes. Backslash MUST be first so the other escapes
# we add aren't themselves doubled. Newlines / CR aren't realistic in an
# OIDC issuer URL or client_id, but if anything weird ever creeps in via
# env, we reject the value at validation time below — never silently
# embed a multi-line value.
escape_js() {
  printf '%s' "$1" | sed \
    -e 's|\\|\\\\|g' \
    -e 's|"|\\"|g'
}

# Reject values containing characters that don't belong in an OIDC issuer
# or client_id: whitespace, control chars, anything that breaks a JS
# string literal in nasty ways. If any var has those, fail loudly.
contains_unsafe_chars() {
  printf '%s' "$1" | LC_ALL=C grep -q '[[:cntrl:][:space:]]'
}
if contains_unsafe_chars "${OIDC_ISSUER:-}" || contains_unsafe_chars "${OIDC_CLIENT_ID:-}"; then
  echo "ERROR: OIDC_ISSUER or OIDC_CLIENT_ID contains whitespace or control characters; refusing to start." >&2
  exit 1
fi

# Write runtime OIDC config to an external JS file. We can't inline the script
# in index.html — strict CSP (`script-src 'self'`) would reject it. Always
# write the file so index.html's <script src> tag never 404s; an empty config
# leaves window.__OIDC_CONFIG__ undefined and the SPA falls back to dev mode.
OIDC_CONFIG_FILE=/usr/share/nginx/html/oidc-config.js
if [ -n "$OIDC_ISSUER" ] && [ -n "$OIDC_CLIENT_ID" ]; then
  issuer_js=$(escape_js "$OIDC_ISSUER")
  client_id_js=$(escape_js "$OIDC_CLIENT_ID")
  printf 'window.__OIDC_CONFIG__={issuer_url:"%s",client_id:"%s"};\n' \
    "$issuer_js" "$client_id_js" > "$OIDC_CONFIG_FILE"
  echo "OIDC config written to $OIDC_CONFIG_FILE: issuer=$OIDC_ISSUER client_id=$OIDC_CLIENT_ID"
else
  : > "$OIDC_CONFIG_FILE"
  echo "OIDC config not set — $OIDC_CONFIG_FILE left empty (dev fallback)"
fi

# Inject <script src="/oidc-config.js"> into index.html if not already present.
# Idempotent: only adds the tag once even on container restart.
if ! grep -q 'src="/oidc-config.js"' /usr/share/nginx/html/index.html; then
  sed -i 's|</head>|<script src="/oidc-config.js"></script></head>|' \
    /usr/share/nginx/html/index.html
fi

# Render CSP with the actual OIDC issuer origin so connect-src / frame-src can
# be tight (specific host) instead of broad `https:`. Falls back to `https:` if
# OIDC_ISSUER is not set, keeping the build usable without runtime config.
if [ -n "$OIDC_ISSUER" ]; then
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
