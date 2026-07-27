# Deploying Insight with Helm (single umbrella chart)

This runbook shows a platform or DevOps engineer how to install the Insight business app on an existing Kubernetes cluster using only `helm` and `kubectl` — no GitOps controller, no CI pipeline. You edit a small set of values, secret, and connector files, then apply them directly with tools already on your workstation. This is the opposite of GitOps: instead of a reconciler (like Argo CD) continuously syncing this repo's manifests, you run each command once, in order, and re-run `helm upgrade` whenever something changes.

## Contents

<!-- toc -->

- [Contents](#contents)
- [Overview](#overview)
- [Prerequisites](#prerequisites)
  - [Cluster and CLI tools](#cluster-and-cli-tools)
  - [Cluster-level dependencies](#cluster-level-dependencies)
  - [Running external infrastructure](#running-external-infrastructure)
- [Step 0 — Collect the values Step 1 needs](#step-0--collect-the-values-step-1-needs)
  - [Generate the tenant ID](#generate-the-tenant-id)
  - [Look up the external service addresses](#look-up-the-external-service-addresses)
  - [Find the Airbyte API URL](#find-the-airbyte-api-url)
  - [Compose the Redpanda brokers string](#compose-the-redpanda-brokers-string)
  - [Get the OIDC client details](#get-the-oidc-client-details)
  - [Read the Argo workflow-controller instance ID](#read-the-argo-workflow-controller-instance-id)
- [Step 1 — Configure values/umbrella.yaml](#step-1--configure-valuesumbrellayaml)
- [Step 2 — Fill the secret files](#step-2--fill-the-secret-files)
  - [secrets/insight-db-creds.yaml](#secretsinsight-db-credsyaml)
  - [secrets/insight-authenticator-signing-keys.yaml](#secretsinsight-authenticator-signing-keysyaml)
- [Step 3 — Create namespace, apply secrets, mirror Airbyte auth](#step-3--create-namespace-apply-secrets-mirror-airbyte-auth)
- [Step 4 — Install with Helm](#step-4--install-with-helm)
- [Step 5 — Verify the install](#step-5--verify-the-install)
- [Step 6 — Configure connectors (optional)](#step-6--configure-connectors-optional)
- [Troubleshooting](#troubleshooting)
- [Appendix — Reference](#appendix--reference)
  - [values/umbrella.yaml placeholders](#valuesumbrellayaml-placeholders)
  - [secrets/insight-db-creds.yaml keys](#secretsinsight-db-credsyaml-keys)
  - [secrets/insight-authenticator-signing-keys.yaml keys](#secretsinsight-authenticator-signing-keysyaml-keys)

<!-- /toc -->

## Overview

Insight reads engineering and collaboration data from your tools (Jira, Slack, GitHub, and so on), pipelines it through ClickHouse, and serves metrics to a dashboard behind an OIDC (OpenID Connect, the login protocol) login. It installs as five first-party services in one Helm "umbrella" chart — bundled sub-charts, so a single `helm install` deploys everything — published at `oci://ghcr.io/constructorfabric/charts/insight` (the chart source also lives in-repo at `charts/insight`, which you can install from directly with `helm install insight ./charts/insight` instead of the OCI form). The five are:

- **Gateway** (`insight-gateway`, alias `gateway`) — the OpenResty edge. It owns the public ingress and is the single entrance to the cluster: it routes `/*` to the Frontend and `/api/*` to Analytics/Identity, performing a cached cookie-to-JWT exchange against the Authenticator's `/internal/authz` endpoint (a per-pod Lua cosocket lookup, not nginx's `auth_request`) and injecting the resulting gateway JWT into upstream requests.
- **Authenticator** (`insight-authenticator`, alias `authenticator`) — a separate pod that performs the OIDC login with your IdP, keeps Redis-backed sessions, and mints the ES256 gateway JWT the Gateway injects downstream.
- **Analytics** (`insight-analytics`, alias `analytics`) — serves metrics from the ClickHouse Gold layer.
- **Identity** (`insight-identity`, alias `identity`) — resolves people and org data from MariaDB; optional (`identity.deploy`, default `false`).
- **Frontend** (`insight-frontend`, alias `frontend`) — the web UI (dashboard); optional (`frontend.deploy`, default `true`).

Two more subcharts exist purely for local development and are off by default: `keycloak` (alias `keycloak`, condition `keycloak.deploy`) and `fakeidp` (alias `fakeidp`, condition `fakeidp.deploy`) — dev-mode OIDC servers with an embedded database and known passwords. Neither is covered here, and neither is the IdP for a stand: this runbook expects the real one from [Prerequisites](#cluster-level-dependencies).

This path assumes your data infrastructure (ClickHouse, MariaDB, Redis, Redpanda, Airbyte, Argo Workflows) already runs and is reachable from the cluster, in another namespace or external. The chart doesn't stand it up; it only wires the services to it. You supply one values file, secret files, and optionally one Secret per connector (see [deploy/CONNECTORS.md](./CONNECTORS.md)). No GitOps repo, CI, or auto-reconciliation — you run the commands yourself.

## Prerequisites

### Cluster and CLI tools

- A Kubernetes cluster you can already reach with `kubectl`, with permission to create namespaces, Secrets, workloads, and Roles/RoleBindings — including in the Airbyte namespace when it differs from the app's (the chart installs a Role there that lets its jobs read Airbyte's auth Secret). That namespace must exist before the umbrella install.
- `helm` ≥ 3.8 (OCI registry support is stable from 3.8 onward, since the chart is pulled as an OCI artifact).
- `kubectl`.
- `jq`, used to mirror the Airbyte auth Secret in Step 3.
- `openssl`, used to generate the authenticator's signing key in Step 2.
- `uuidgen` (or `python3`), used to generate the tenant ID in Step 0.
- `base64`, used when copying existing datastore passwords in Step 2 (most systems ship this by default).

### Cluster-level dependencies

Install all three of these before the chart — it bundles none of them:

- **An ingress controller.** Install ingress-nginx, or override `gateway.ingress.className` / `frontend.ingress.className` to match what you run. Both default to `className: nginx`.
- **A real OIDC identity provider.** OIDC is mandatory in every environment — there is no auth-off switch — so the authenticator needs an IdP to log in against: Entra ID, Okta, Auth0, or your own. **No IdP on the stand? Install Keycloak as its own release and treat it like any other external dependency** — it is the straightforward choice: a real OIDC implementation, a public image, and an admin console for creating the realm and the confidential client. Give it a hostname the browser *and* the authenticator pod both resolve to the same URL, then read its issuer, client ID, and client secret in Step 0. The chart's bundled `keycloak`/`fakeidp` subcharts are not this: they run dev-mode servers with an embedded database and known passwords, for local development only.
- **cert-manager, plus a `ClusterIssuer` of your own.** The authenticator's TLS-discovery sidecar (`authenticator.tlsDiscovery.enabled`, default `true`) creates a `cert-manager.io/v1` `Certificate`, and Analytics and Identity verify the authenticator's JWKS over HTTPS against that CA — load-bearing, not optional. Set `authenticator.tlsDiscovery.issuerRef.name` to whatever your cluster issues from. Do not expect the chart's `local-ca` default to resolve: that issuer belongs to this repo's local k3s sandbox (`deploy/gitops/bootstrap/local/selfsigned-issuer.yaml`, applied by `make bootstrap-cert-manager ENV=local`) and will not exist in a cluster you did not bootstrap that way. Any issuer will do — this certificate is internal-only, trusted through the CA the services mount, so it needs no public chain and is unrelated to the public ingress certificate in `<TLS_SECRET>`.

Confirm the cluster-side pieces (the IdP gets verified in Step 0, once you have its issuer URL):

```sh
kubectl get ingressclass nginx                  # the className both ingress blocks use
kubectl get crd certificates.cert-manager.io    # cert-manager CRDs installed
kubectl get clusterissuer                       # pick one for tlsDiscovery.issuerRef.name
```

### Running external infrastructure

All six systems below must be deployed and reachable from the cluster before you start. The chart never installs any of them — ClickHouse, MariaDB, Redis, and Redpanda are wired in purely by host/credentials; Airbyte and Argo Workflows via `airbyte.apiUrl` and `ingestion.reconcile.argoInstanceId`. Step 0 shows how to read those addresses off your cluster.

| System | Used for |
|--------|----------|
| ClickHouse | Stores the Bronze (raw ingested data), Silver (cleaned/conformed), and Gold (query-ready) data layers; Analytics reads the Gold layer to serve metrics |
| MariaDB | Owns the `identity` database that Identity uses to resolve people and org data |
| Redis | Caching layer used by Analytics and session storage used by the Authenticator |
| Redpanda | Event-streaming backbone (Kafka-compatible) used by the ingestion pipeline |
| Airbyte | Runs the data connectors (Jira, Slack, GitHub, and so on) that load raw data into ClickHouse Bronze |
| Argo Workflows | Runs the dbt transform workflows that turn Bronze into Silver and Gold, and runs the sync workflows Airbyte connections trigger |

Run every command below from the directory holding your `values/` and `secrets/` files — Steps 1 and 2 give you the full contents of both. Connector configuration (the `connectors/` directory) comes later, in [deploy/CONNECTORS.md](./CONNECTORS.md).

## Step 0 — Collect the values Step 1 needs

Generate the tenant ID, then read the rest off the cluster — every dependency is external, and each may sit in its own namespace or outside the cluster entirely.

### Generate the tenant ID

A lowercase UUID, used verbatim for both `global.tenantDefaultId` and `ingestion.reconcile.tenantId`, and never changed after the first sync (local/dev against the compose wizard, the seed generators, or `fakeidp` instead reuses their fixed `00000000-df51-5b42-9538-d2b56b7ee953`).

```sh
uuidgen | tr '[:upper:]' '[:lower:]'    # no uuidgen? python3 -c 'import uuid; print(uuid.uuid4())'
```

### Look up the external service addresses

Every host is `<svc>.<its-own-namespace>.svc.cluster.local` — or any resolvable host or IP for off-cluster infrastructure — and the ClickHouse, MariaDB, and Redis ports are already fixed in the skeleton, so you supply only the host.

```sh
kubectl get svc -A | grep -Ei 'clickhouse|mariadb|redis|redpanda|airbyte'
```

### Find the Airbyte API URL

`airbyte.apiUrl` is the Airbyte **server** Service on its HTTP port, and you must set it whenever Airbyte runs anywhere other than the `insight` namespace — left empty, the chart computes `http://<airbyte.releaseName>-airbyte-server-svc.<release-namespace>.svc.cluster.local:8001`, which only resolves when Airbyte is a release in that same namespace.

```sh
kubectl -n <airbyte-ns> get svc | grep server
  # e.g. http://airbyte-airbyte-server-svc.<airbyte-ns>.svc.cluster.local:8001
```

### Compose the Redpanda brokers string

`redpanda.brokers` takes one comma-separated `host:port` bootstrap string — not the host/port pair the other datastores take — aimed at Redpanda's internal Kafka API listener, so read the port rather than assuming `9093` (that is the `redpanda/redpanda` chart's default; this repo's compose stack uses `9092`).

```sh
kubectl -n <redpanda-ns> get svc <redpanda-svc> -o jsonpath='{range .spec.ports[*]}{.name}={.port}{"\n"}{end}'
  # compose <redpanda-svc>.<redpanda-ns>.svc.cluster.local:<kafka port>
  # e.g. redpanda.insight-infra.svc.cluster.local:9093
```

### Get the OIDC client details

In the IdP from Prerequisites, register a **confidential** client whose redirect URI is `https://<HOST>/auth/callback`, and collect its issuer URL, client ID, and client secret; on Keycloak the issuer is `<keycloak-base-url>/realms/<realm>`, and the client's *Credentials* tab holds the secret.

```sh
# the issuer must be the SAME URL the browser and the authenticator pod resolve, or `iss` won't validate
kubectl run oidc-probe --rm -i --restart=Never --image=curlimages/curl -- \
  curl -sS <OIDC_ISSUER>/.well-known/openid-configuration | head -c 200
```

### Read the Argo workflow-controller instance ID

Set `ingestion.reconcile.argoInstanceId` to the controller's configured `instanceID` only when it is pinned to one; no match means leave it empty — the common case, where the reconcile workflows go unlabelled and any controller accepts them.

```sh
kubectl -n <argo-ns> get cm | grep workflow-controller     # name varies by chart version
kubectl -n <argo-ns> get cm <cm> -o jsonpath='{.data.config}' | grep -i instanceID   # newer charts nest it under `config:`
kubectl -n <argo-ns> get cm <cm> -o jsonpath='{.data.instanceID}{"\n"}'              # older ones use a top-level key
```

## Step 1 — Configure values/umbrella.yaml

Create `values/umbrella.yaml` from the skeleton below and replace every `<...>` placeholder. No passwords here — they go in the Step 2 secret files.

```yaml
## values/umbrella.yaml — the only values file you need.
## Fill every <...> placeholder. Passwords are NEVER here — see the secret files.
credentials:
  deploymentMode: helm               # helm | gitops (gitops forbids autoGenerate:true)
  autoGenerate: true                 # BYO compose; won't overwrite a labelless insight-db-creds

global:
  tenantDefaultId: "<TENANT_ID>"     # the UUID from Step 0; must equal ingestion.reconcile.tenantId
  # storageClass: ""                 # "" = cluster default; e.g. "local-path" locally
  # imagePullSecrets: []             # [{name: my-regcred}] for a private registry

# Datastore wiring — every dep is external; the chart only dials it.
clickhouse:
  host: <CLICKHOUSE_HOST>            # e.g. clickhouse.<its-ns>.svc.cluster.local
  port: 8123
  database: insight
  username: insight
mariadb:
  host: <MARIADB_HOST>
  port: 3306
  database: insight
  username: insight
redis:
  host: <REDIS_HOST>
  port: 6379
redpanda:
  brokers: "<REDPANDA_BROKERS>"      # e.g. redpanda.<its-ns>.svc.cluster.local:9093

# Ingestion — point at existing Airbyte + Argo; install the dbt WorkflowTemplates.
ingestion:
  templates:
    enabled: true
  reconcile:
    tenantId: "<TENANT_ID>"
    destinationName: clickhouse-bronze
    argoInstanceId: "<ARGO_INSTANCE_ID>"     # match the controller's instanceID (Step 0); leave "" if unpinned
airbyte:
  apiUrl: "<AIRBYTE_API_URL>"        # required unless Airbyte runs in the `insight` namespace (Step 0)

analytics:
  replicaCount: 1                    # chart default 2; bump for HA
  resources:
    requests: { cpu: 100m, memory: 128Mi }
    limits:   { cpu: 500m, memory: 512Mi }

gateway:
  replicaCount: 1
  ingress:
    enabled: true
    className: nginx
    host: <HOST>
    tls:
      enabled: true
      secretName: <TLS_SECRET>
  resources:
    requests: { cpu: 100m, memory: 128Mi }
    limits:   { cpu: 500m, memory: 256Mi }

authenticator:
  replicaCount: 1
  # ES256 signing keys — see Step 2. MUST already exist as a Secret before install.
  signingKeysSecret: "insight-authenticator-signing-keys"
  # cert-manager Certificate for the JWKS-discovery sidecar (internal TLS only).
  tlsDiscovery:
    enabled: true
    issuerRef:
      name: <CLUSTER_ISSUER>          # your cluster's ClusterIssuer; the chart's `local-ca`
                                      # default only exists in the local k3s sandbox
  oidc:
    issuerUrl: "<OIDC_ISSUER>"        # MUST be set — your IdP's issuer URL
    clientId: "<OIDC_CLIENT_ID>"
    clientSecret: "<OIDC_CLIENT_SECRET>"
    redirectUri: "https://<HOST>/auth/callback"   # MUST be set — browser-facing callback through the gateway
    scopes: ["openid", "profile", "email"]

identity:
  deploy: true                       # MUST be true (chart default false)
  replicaCount: 1
  databaseName: "identity"
  resources:
    requests: { cpu: 50m,  memory: 96Mi }
    limits:   { cpu: 250m, memory: 384Mi }

frontend:                            # the web UI (dashboard)
  deploy: true
  replicaCount: 1
  ingress:
    enabled: true                    # WITHOUT this the UI pod runs but is never exposed
    className: nginx
    host: <HOST>                     # same FQDN as gateway.ingress.host; /api/* → Gateway routes, /* → UI
  oidc:                              # public values; the browser starts the login here
    issuer: "<OIDC_ISSUER>"          # same IdP as the authenticator
    clientId: "<OIDC_CLIENT_ID>"
    scopes: "openid profile email"   # IdP-specific
```

To start from the chart's full defaults instead of typing the skeleton:

```sh
helm show values oci://ghcr.io/constructorfabric/charts/insight > values/umbrella.yaml
```

Fill each placeholder:

| Placeholder | What it should be |
|-------------|--------------------|
| `<TENANT_ID>` | The tenant UUID you generated in Step 0. Must be the same value in `global.tenantDefaultId` and `ingestion.reconcile.tenantId` |
| `<CLICKHOUSE_HOST>` | ClickHouse HTTP host, in `host:8123` form |
| `<MARIADB_HOST>` | MariaDB host, in `host:3306` form |
| `<REDIS_HOST>` | Redis host, in `host:6379` form |
| `<REDPANDA_BROKERS>` | The bootstrap string you composed in Step 0 — comma-separated `host:port` pointing at the internal Kafka API listener |
| `<AIRBYTE_API_URL>` | The Airbyte server Service URL from Step 0, for example `http://host:8001`. Omit only when Airbyte is a release in the `insight` namespace — the chart then computes it from `airbyte.releaseName` |
| `<ARGO_INSTANCE_ID>` | The `instanceID` your Argo workflow controller is pinned to — read it off the controller config map in Step 0. Leave empty (`""`) if the controller is unpinned, the common case |
| `<HOST>` | Public FQDN for the ingress, shared by the Gateway and Frontend, for example `insight.example.com` |
| `<TLS_SECRET>` | Name of the Kubernetes TLS Secret that covers that domain |
| `<CLUSTER_ISSUER>` | A cert-manager `ClusterIssuer` in your cluster, for the authenticator's internal JWKS certificate. Self-signed is fine; the chart's `local-ca` default exists only in this repo's local sandbox |
| `<OIDC_ISSUER>` | Your IdP's issuer URL. Its `/.well-known/openid-configuration` document must resolve from inside the cluster |
| `<OIDC_CLIENT_ID>` / `<OIDC_CLIENT_SECRET>` | Your OIDC client / application registration credentials |

For infrastructure in the same cluster, use `<service>.<namespace>.svc.cluster.local`. Any resolvable host or IP also works.

Check these four before installing:

- Set `identity.deploy: true`. The chart default is `false`, and without the override Identity — and person resolution for the whole app — never deploys.
- Set real values for `authenticator.oidc.issuerUrl` and `redirectUri`. The chart wraps both in Helm's `required`, and there is no auth-off switch.
- Create the Secret named in `authenticator.signingKeysSecret` before installing (Step 2). The chart does not generate it.
- Point the OIDC fields at the real IdP from Prerequisites. The bundled `keycloak`/`fakeidp` subcharts are dev-mode servers for local development, not a stand's IdP.

## Step 2 — Fill the secret files

### secrets/insight-db-creds.yaml

Create this Secret with all four datastore passwords — the chart fails fast if any key is missing. Use the passwords your datastores already run with.

```yaml
apiVersion: v1
kind: Secret
metadata: { name: insight-db-creds, namespace: insight }
type: Opaque
stringData:
  clickhouse-password:   "CHANGE_ME"   # ClickHouse admin password    -> Analytics
  mariadb-password:      "CHANGE_ME"   # MariaDB app-user password    -> Analytics + Identity
  mariadb-root-password: "CHANGE_ME"   # MariaDB root password (identity-DB init hook) -> Identity
  redis-password:        "CHANGE_ME"   # Redis password               -> Analytics + Authenticator
```

If those passwords already live in Secrets in your infrastructure namespace, copy them across instead of retyping them:

```sh
# each password lives in a Secret in its own datastore's namespace — they need not be the same namespace
kubectl -n <clickhouse-ns> get secret <ch-secret>         -o jsonpath='{.data.<ch-key>}'    | base64 -d; echo   # clickhouse-password
kubectl -n <mariadb-ns>    get secret <maria-secret>      -o jsonpath='{.data.<app-key>}'   | base64 -d; echo   # mariadb-password (app user)
kubectl -n <mariadb-ns>    get secret <maria-root-secret> -o jsonpath='{.data.<root-key>}'  | base64 -d; echo   # mariadb-root-password
kubectl -n <redis-ns>      get secret <redis-secret>      -o jsonpath='{.data.<redis-key>}' | base64 -d; echo   # redis-password
```

Paste each decoded value into the matching field.

> **Never label this Secret `app.kubernetes.io/managed-by: Helm`.** The chart reads the label's *absence* as "bring your own" and composes `insight-analytics-config` and `insight-identity-config` from your values; with the label it takes ownership and may overwrite them with generated passwords.

### secrets/insight-authenticator-signing-keys.yaml

Generate the authenticator's ES256 (EC P-256) gateway-JWT key as PKCS#8 and create the Secret — the chart does not generate it:

```sh
openssl ecparam -name prime256v1 -genkey -noout | openssl pkcs8 -topk8 -nocrypt -out current.pem
kubectl -n insight create secret generic insight-authenticator-signing-keys --from-file=current.pem
```

## Step 3 — Create namespace, apply secrets, mirror Airbyte auth

Create the namespace and apply the secret files:

```sh
kubectl create namespace insight
kubectl -n insight apply -f secrets/

# verify
kubectl -n insight get secret insight-db-creds insight-authenticator-signing-keys   # expect 4 keys / 1 key (current.pem)
```

Mirror Airbyte's auth Secret into `insight` — Analytics needs it to call the Airbyte API:

```sh
NS_AIRBYTE=<the-namespace-airbyte-runs-in>
kubectl -n $NS_AIRBYTE get secret airbyte-auth-secrets -o json \
  | jq 'del(.metadata.uid,.metadata.resourceVersion,.metadata.creationTimestamp,.metadata.ownerReferences,.metadata.annotations,.metadata.labels) | .metadata.namespace="insight"' \
  | kubectl -n insight apply -f -
```

The `jq` filter strips the source object's identity fields (UID, resource version, owner references) and retargets the namespace, so Kubernetes accepts it as a new object.

## Step 4 — Install with Helm

Install the umbrella chart against your values file:

```sh
helm upgrade --install insight oci://ghcr.io/constructorfabric/charts/insight \
  -n insight -f values/umbrella.yaml --wait --timeout 15m
```

- Add `--version <x.y.z>` to pin a chart release; omit it for the latest published one.
- `--wait --timeout 15m` blocks until every resource is ready, giving a pass/fail signal instead of a detached rollout.
- The install also runs the `insight-clickhouse-migrate` hook Job, which applies the ClickHouse gold-view migrations (`src/ingestion/scripts/migrations/*.sql`) using the chart's pinned toolbox image. It fires on **every** upgrade, not just the first install (gated by `clickhouse.runMigrations`, default `true`), and a failing migration fails the whole upgrade. It drops and recreates every gold object each run, so a failure points at Bronze/Silver schema or data, not a stale-object conflict.

## Step 5 — Verify the install

Run all four checks:

```sh
kubectl -n insight get pods
  # expect insight-gateway, -authenticator, -analytics, -identity, -frontend all Running
  # (Identity only with identity.deploy: true; fakeidp/keycloak only with their deploy flag)

kubectl -n insight get secret insight-analytics-config insight-authenticator-config insight-identity-config
  # the chart composes these from insight-db-creds (the identity one only when identity.deploy=true)

kubectl -n insight get jobs -l app.kubernetes.io/component=clickhouse-migrate
kubectl -n insight logs job/insight-clickhouse-migrate
  # the gold-view migration Job must be Complete

kubectl -n insight get cronworkflow
  # expect insight-reconcile-loop (provisions Airbyte sources/connections)
```

Then open `https://<HOST>` — the host from Step 1 — and confirm the login redirect to your OIDC provider.

## Step 6 — Configure connectors (optional)

Configure connectors after the app is up. Each of the 25 connectors is a single Kubernetes Secret; the `insight-reconcile-loop` CronWorkflow discovers it and provisions the Airbyte source automatically, so there is nothing else to run.

See [deploy/CONNECTORS.md](./CONNECTORS.md) for the connector list and a copy-paste Secret for each.

## Troubleshooting

| Problem | What to check |
|---------|-----------------|
| `insight-analytics` / `insight-identity` stuck in `CreateContainerConfigError` | The chart could not compose the `*-config` Secrets. Confirm `insight-db-creds` has all four keys and carries **no** `app.kubernetes.io/managed-by: Helm` label: `kubectl -n insight get secret insight-db-creds -o yaml \| grep managed-by` should return nothing |
| `helm install`/`upgrade` fails with `signingKeysSecret is required` or the authenticator pod won't mount its keys | The Secret named in `authenticator.signingKeysSecret` (default `insight-authenticator-signing-keys`) doesn't exist or is missing `current.pem`. Create it as shown in Step 2 before installing |
| `helm install`/`upgrade` fails with `tlsDiscovery.issuerRef.name is required` or the `insight-authenticator-authn-tls` Certificate never turns `Ready` | cert-manager isn't installed, or the `ClusterIssuer` named in `authenticator.tlsDiscovery.issuerRef.name` doesn't exist in this cluster — the usual cause is leaving the chart's local-sandbox `local-ca` default in place. Confirm with `kubectl get clusterissuer` and `kubectl -n insight describe certificate insight-authenticator-authn-tls` |
| `helm install`/`upgrade` fails with `authenticator.oidc.issuerUrl is required` / `redirectUri is required` | Both fields are mandatory (`charts/insight/templates/secrets.yaml` wraps them in `required`). Set them to your IdP's real values — a real IdP is a prerequisite, and there is no way to disable auth |
| Login fails with an issuer/`iss` mismatch, or discovery 404s | `authenticator.oidc.issuerUrl` must be the issuer the IdP actually advertises, resolving to the *same* URL from the browser and from the authenticator pod — a split-horizon setup (public hostname outside, Service DNS inside) breaks `iss` validation. On Keycloak the issuer is `<base-url>/realms/<realm>`, and the base URL must match what the server advertises. Check with the `oidc-probe` command from Step 0 |
| Dashboards show "no peer data" (the benchmark/comparison panel is empty) | After Gold-layer data has loaded, restart Analytics: `kubectl -n insight rollout restart deploy/insight-analytics` |
| Login breaks after changing the host | Update `authenticator.oidc.redirectUri` (and `frontend.oidc.issuer`/`clientId` if the IdP changed) in values, `helm upgrade`, then restart the gateway: `kubectl -n insight rollout restart deploy/insight-gateway` |

For connector-syncing problems, see the Troubleshooting section of [deploy/CONNECTORS.md](./CONNECTORS.md).

## Appendix — Reference

### values/umbrella.yaml placeholders

| Placeholder | Field(s) | Notes |
|-------------|----------|-------|
| `<TENANT_ID>` | `global.tenantDefaultId`, `ingestion.reconcile.tenantId` | Generated in Step 0; a lowercase UUID, identical across both |
| `<CLICKHOUSE_HOST>` | `clickhouse.host` | Always external; port fixed at `8123` in the file |
| `<MARIADB_HOST>` | `mariadb.host` | Always external; port fixed at `3306` |
| `<REDIS_HOST>` | `redis.host` | Always external; port fixed at `6379` |
| `<REDPANDA_BROKERS>` | `redpanda.brokers` | Always external; a single comma-separated `host:port` string, not a host/port pair. `9093` for the `redpanda/redpanda` chart's internal listener — read yours in Step 0 |
| `<AIRBYTE_API_URL>` | `airbyte.apiUrl` | e.g. `http://host:8001`. Empty falls back to `http://<airbyte.releaseName>-airbyte-server-svc.<release-namespace>:8001`, so it is only safe to omit when Airbyte shares the `insight` namespace |
| `<ARGO_INSTANCE_ID>` | `ingestion.reconcile.argoInstanceId` | Match the controller's configured `instanceID` (Step 0); empty if unpinned |
| `<HOST>` | `gateway.ingress.host`, `frontend.ingress.host` | Public FQDN, shared by the Gateway and Frontend (`/*` → UI, `/api/*` → Gateway routes to Analytics/Identity) |
| `<TLS_SECRET>` | `gateway.ingress.tls.secretName` | Kubernetes TLS Secret name |
| `<CLUSTER_ISSUER>` | `authenticator.tlsDiscovery.issuerRef.name` | A cert-manager `ClusterIssuer` that exists in your cluster; internal cert, so self-signed is fine |
| `<OIDC_ISSUER>` | `authenticator.oidc.issuerUrl`, `frontend.oidc.issuer` | Your IdP's issuer URL |
| `<OIDC_CLIENT_ID>` / `<OIDC_CLIENT_SECRET>` | `authenticator.oidc.clientId`/`clientSecret`, `frontend.oidc.clientId` | Your OIDC client / application registration credentials |

Other notable (non-placeholder) settings in this file:

- Image tags are omitted deliberately. Each subchart renders `image.tag | default .Chart.AppVersion`, and the release pipeline pins those appVersions in lockstep, so a chart release already carries a coherent set of product images. Set `<service>.image.tag` only to pin one service to a different build.
- `credentials.deploymentMode: helm` and `credentials.autoGenerate: true` — this enables the "bring your own" credentials path, where the chart keeps a labelless `insight-db-creds` Secret instead of generating random passwords.
- `identity.deploy: true` — required override; the chart's own default is `false`.
- `authenticator.tlsDiscovery.issuerRef.name` — the cert-manager `ClusterIssuer` the JWKS-discovery Certificate is issued from. Always set this: the chart ships `local-ca`, which is the self-signed root that `make bootstrap-cert-manager ENV=local` creates for the local k3s sandbox, not anything a real cluster has.
- There is no auth-off toggle anywhere in this chart. `authenticator.oidc.issuerUrl` and `authenticator.oidc.redirectUri` are hard `required` fields, so a real IdP is a prerequisite; install Keycloak as a separate release if the stand has none. The bundled `keycloak`/`fakeidp` subcharts are local-development servers (embedded database, known passwords) and not a substitute.

### secrets/insight-db-creds.yaml keys

| Key | Meaning | Consumed by |
|-----|---------|--------------|
| `clickhouse-password` | ClickHouse admin password | Analytics |
| `mariadb-password` | MariaDB app-user password | Analytics + Identity |
| `mariadb-root-password` | MariaDB root password, used by the identity-DB init hook | Identity |
| `redis-password` | Redis password | Analytics + Authenticator |

Recall: this Secret must never carry an `app.kubernetes.io/managed-by: Helm` label.

### secrets/insight-authenticator-signing-keys.yaml keys

One required key, `current.pem`: the active ES256 (EC P-256) signing key, unencrypted PKCS#8 PEM.
