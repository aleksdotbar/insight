# Preview environments (`insight-preview`)

A path-based FE preview experiment for the presentation layer (epic #1803, sub-issue
#1971). Each experiment is one release of this chart: a `Deployment` + `Service` +
one prefix-strip `Ingress` route object, all named `preview-<experiment>` and served
under `/exp/<experiment>` on a single shared host.

Provisioning is manual — no GitOps controller. Apply with `helm`, remove with
`helm uninstall`. Only the FE image varies per experiment; the backend never does.

## Why path-based on one host

One host means one Entra redirect URI (Entra has no reliable wildcard redirect) and a
same-origin session cookie. The controller merges same-host `Ingress` objects, so
`helm upgrade --install` **adds** the `/exp/<name>` path and `helm uninstall`
**removes** it — no central config is ever rewritten.

The route prefix-strips `/exp/<name>` (`rewrite-target: /$2`) so the FE image — built
with a relative asset base and a runtime router basepath — serves under any prefix.
`/api/...` is an absolute path the FE emits unprefixed, so it is not matched here and
flows to the shared backend route.

## Provision an experiment

```sh
helm upgrade --install preview-<name> deploy/preview \
  --namespace <ns> \
  --set experiment=<name> \
  --set image.tag=<fe-build-tag-or-digest> \
  --set ingress.host=<single-preview-host>
```

`experiment` must be a DNS-1123 label (lowercase alphanumerics and `-`). Then open
`https://<single-preview-host>/exp/<name>/`.

## Remove an experiment

```sh
helm uninstall preview-<name> --namespace <ns>
```

## Follow-ups (separate sub-issues)

- **#1972** — the authenticated return path: login stays the gateway+authenticator's
  job (not FE-side OIDC), extended with a Redis-backed opaque `state` through the
  single fixed callback that `302`s back to `/exp/<name>`. This chart deliberately
  carries no auth env; that wiring lands with #1972.
- **#1973** — pin the shared preview backend to synthetic data only.
- **#1981** — CI-driven provisioning, sequenced after the nginx-to-Envoy move; the
  `pathType: ImplementationSpecific` route becomes a Gateway API `HTTPRoute` then.

See `docs/domain/presentation-layer/specs/DESIGN.md` (Preview Environment Router).
