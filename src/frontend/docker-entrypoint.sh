#!/bin/sh
set -e

# The SPA is a pure static bundle served behind the nginx `gateway`, which
# fronts `/api/*`, `/auth/*`, and `/` (this container). Authentication is a
# server-side cookie/BFF flow: the browser hits `/auth/login` (gateway ->
# authenticator -> IdP), gets a `__Host-sid` session cookie, and the SPA calls
# `/api/*` and `/auth/me` same-origin with `credentials: 'include'`. There is
# no client-side OIDC and no runtime config injection anymore, so this
# entrypoint only renders the nginx config template and execs the CMD.

# Place the nginx config. It has no runtime placeholders anymore (the SPA is
# same-origin only, so the CSP needs no injected OIDC issuer origin), but the
# build ships it under /etc/nginx/templates, so copy it into conf.d here.
cp /etc/nginx/templates/default.conf.template /etc/nginx/conf.d/default.conf

exec "$@"
