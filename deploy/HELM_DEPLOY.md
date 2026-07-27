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
  - [Compose the Redpanda brokers string](#compose-the-redpanda-brokers-string)
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
  - [values/umbrella.orbstack.yaml (local variant)](#valuesumbrellaorbstackyaml-local-variant)

<!-- /toc -->

## Overview

Insight reads engineering and collaboration data from your tools (Jira, Slack, GitHub, and so on), pipelines it through ClickHouse, and serves metrics to a dashboard behind an OIDC (OpenID Connect, the login protocol) login. It installs as five first-party services in one Helm "umbrella" chart — bundled sub-charts, so a single `helm install` deploys everything — published at `oci://ghcr.io/constructorfabric/charts/insight` (the chart source also lives in-repo at `charts/insight`, which you can install from directly with `helm install insight ./charts/insight` instead of the OCI form). The five are:

- **Gateway** (`insight-gateway`, alias `gateway`) — the OpenResty edge. It owns the public ingress and is the single entrance to the cluster: it routes `/*` to the Frontend and `/api/*` to Analytics/Identity, performing a cached cookie-to-JWT exchange against the Authenticator's `/internal/authz` endpoint (a per-pod Lua cosocket lookup, not nginx's `auth_request`) and injecting the resulting gateway JWT into upstream requests.
- **Authenticator** (`insight-authenticator`, alias `authenticator`) — a separate pod that performs the OIDC login with your IdP, keeps Redis-backed sessions, and mints the ES256 gateway JWT the Gateway injects downstream.
- **Analytics** (`insight-analytics`, alias `analytics`) — serves metrics from the ClickHouse Gold layer.
- **Identity** (`insight-identity`, alias `identity`) — resolves people and org data from MariaDB; optional (`identity.deploy`, default `false`).
- **Frontend** (`insight-frontend`, alias `frontend`) — the web UI (dashboard); optional (`frontend.deploy`, default `true`).

Two more subcharts exist purely for local/dev use and are off by default: `fakeidp` (alias `fakeidp`, condition `fakeidp.deploy`) and `keycloak` (alias `keycloak`, condition `keycloak.deploy`) — both are bundled OIDC providers for a cluster with no real IdP available. `fakeidp` is the one this runbook documents; `keycloak` is a heavier bundled alternative not covered here. Neither is appropriate for a real environment.

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

Install both of these before the chart — it bundles neither:

- **An ingress controller.** Install ingress-nginx, or override `gateway.ingress.className` / `frontend.ingress.className` to match what you run. Both default to `className: nginx`.
- **cert-manager, with a working `ClusterIssuer`.** The authenticator's TLS-discovery sidecar (`authenticator.tlsDiscovery.enabled`, default `true`) creates a `cert-manager.io/v1` `Certificate`, and Analytics and Identity verify the authenticator's JWKS over HTTPS against that CA — load-bearing, not optional. Provision a `ClusterIssuer` named `local-ca` (the chart default) or point `authenticator.tlsDiscovery.issuerRef.name` at your own.

Confirm both are in place:

```sh
kubectl get ingressclass nginx                  # the className both ingress blocks use
kubectl get crd certificates.cert-manager.io    # cert-manager CRDs installed
kubectl get clusterissuer local-ca              # the issuer the chart defaults to
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

Step 1 asks for two kinds of value: the tenant ID, which you generate, and the addresses of the external services, which you look up. Every datastore is external — the chart only dials it — so nothing here creates infrastructure.

### Generate the tenant ID

```sh
uuidgen | tr '[:upper:]' '[:lower:]'    # no uuidgen? python3 -c 'import uuid; print(uuid.uuid4())'
```

- It must be a lowercase UUID. The identity tables type the column `UUID`, and the Silver models pass the string through verbatim (`tenant_id AS insight_tenant_id`), so case has to stay consistent.
- Use the same value for `global.tenantDefaultId` (how the app resolves the tenant) and `ingestion.reconcile.tenantId` (stamped into every ingested row as `insight_tenant_id`). If they diverge, dashboards read one tenant while the pipeline writes another.
- Never change it after the first sync — ingested data is keyed by it.
- Local/dev against the compose wizard, the seed generators, or `fakeidp`: use their fixed tenant `00000000-df51-5b42-9538-d2b56b7ee953` instead of generating one.

### Look up the external service addresses

List the Services in the namespace your infrastructure runs in and read each host and port off it:

```sh
NS_INFRA=<your-infra-namespace>
kubectl -n $NS_INFRA get svc
  # every address in Step 1 is <svc>.$NS_INFRA.svc.cluster.local:<port>
  # ClickHouse 8123, MariaDB 3306, Redis 6379 are already fixed in the skeleton — you only supply the host
  # Airbyte: <AIRBYTE_API_URL> is the server Service, e.g. http://airbyte-airbyte-server-svc.$NS_INFRA.svc.cluster.local:8001
```

Infrastructure outside the cluster works the same way — use any resolvable host or IP instead of the in-cluster DNS name.

### Compose the Redpanda brokers string

`redpanda.brokers` is the exception: one comma-separated `host:port` bootstrap string rather than the host/port pair the other datastores take, and it must point at Redpanda's internal Kafka API listener.

```sh
kubectl -n $NS_INFRA get svc -l app.kubernetes.io/name=redpanda
kubectl -n $NS_INFRA get svc redpanda -o jsonpath='{range .spec.ports[*]}{.name}={.port}{"\n"}{end}'
  # compose <svc>.$NS_INFRA.svc.cluster.local:<kafka port>
  # e.g. redpanda.insight-infra.svc.cluster.local:9093
```

- Read the port instead of assuming it. `9093` is the internal listener of the official `redpanda/redpanda` chart; other setups differ — this repo's compose stack runs it on `9092`.
- One reachable broker bootstraps the client, which then discovers the rest from cluster metadata. Comma-separate more only for resilience.
- The field is `required`, so the chart will not render without it. If you have no Redpanda and are not exercising the authenticator's audit stream, point it at an unroutable placeholder the way the functional-CI overlay does (`brokers: "redpanda-disabled:9093"`).

### Read the Argo workflow-controller instance ID

`ingestion.reconcile.argoInstanceId` must match the `instanceID` the cluster's Argo workflow controller runs with. It stamps the `workflows.argoproj.io/controller-instanceid` label onto the workflows reconcile submits, so a controller pinned to that instance ID picks them up. **If the controller has no `instanceID` configured — the common case — leave this empty** (the label is omitted and an unpinned controller accepts the workflows anyway). Only set it when the controller is pinned.

Read it off the controller's config map, which is the authoritative source:

```sh
# find the controller config map (name varies by chart, e.g. argo-workflows-workflow-controller-configmap)
kubectl -n $NS_INFRA get cm | grep workflow-controller

CM=<the-config-map-name>
# newer charts nest all controller config under a single `config:` YAML key …
kubectl -n $NS_INFRA get cm $CM -o jsonpath='{.data.config}' | grep -i instanceID
# … older ones expose it as a top-level data key:
kubectl -n $NS_INFRA get cm $CM -o jsonpath='{.data.instanceID}{"\n"}'
```

- A non-empty match (e.g. `instanceID: argo-workflows-insight`) → set `argoInstanceId` to that exact string.
- No `instanceID` line / empty output → leave `argoInstanceId` empty (comment the line out). Verify the controller is unpinned by confirming its flags carry no `--instanceid`: `kubectl -n $NS_INFRA get deploy -l app.kubernetes.io/component=workflow-controller -o jsonpath='{.items[0].spec.template.spec.containers[0].args}'`.

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
  host: <CLICKHOUSE_HOST>            # e.g. clickhouse.<infra-ns>.svc.cluster.local
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
  brokers: "<REDPANDA_BROKERS>"      # e.g. redpanda.<infra-ns>.svc.cluster.local:9093

# Ingestion — point at existing Airbyte + Argo; install the dbt WorkflowTemplates.
ingestion:
  templates:
    enabled: true
  # toolboxImage: "<TOOLBOX_IMAGE>" # optional — defaults to the chart's appVersion-pinned toolbox; only set to override
  reconcile:
    tenantId: "<TENANT_ID>"
    destinationName: clickhouse-bronze
    argoInstanceId: "<ARGO_INSTANCE_ID>"     # match the controller's instanceID (Step 0); leave "" if unpinned
airbyte:
  namespace: "<AIRBYTE_NAMESPACE>"   # namespace of the Airbyte release, e.g. <infra-ns>; "" = same as the app
  apiUrl: ""                         # "" = computed from airbyte.releaseName + airbyte.namespace; set only for a non-standard URL

analytics:
  replicaCount: 1                    # chart default 2; bump for HA
  image:
    tag: "<IMAGE_TAG>"               # optional — falls back to the chart's appVersion
  resources:
    requests: { cpu: 100m, memory: 128Mi }
    limits:   { cpu: 500m, memory: 512Mi }

gateway:
  replicaCount: 1
  image:
    tag: "<IMAGE_TAG>"                # optional — falls back to the chart's appVersion
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
  image:
    tag: "<IMAGE_TAG>"
  # ES256 signing keys — see Step 2. MUST already exist as a Secret before install.
  signingKeysSecret: "insight-authenticator-signing-keys"
  # cert-manager Certificate for the JWKS-discovery sidecar. Override
  # issuerRef.name only if your cluster's ClusterIssuer isn't named `local-ca`.
  tlsDiscovery:
    enabled: true
    issuerRef:
      name: local-ca
  oidc:
    issuerUrl: "<OIDC_ISSUER>"        # MUST be set — your IdP's issuer URL
    clientId: "<OIDC_CLIENT_ID>"
    clientSecret: "<OIDC_CLIENT_SECRET>"
    redirectUri: "https://<HOST>/auth/callback"   # MUST be set — browser-facing callback through the gateway
    scopes: ["openid", "profile", "email"]

identity:
  deploy: true                       # MUST be true (chart default false)
  replicaCount: 1
  image:
    tag: "<IMAGE_TAG>"
  databaseName: "identity"
  resources:
    requests: { cpu: 50m,  memory: 96Mi }
    limits:   { cpu: 250m, memory: 384Mi }

frontend:                            # the web UI (dashboard)
  deploy: true
  replicaCount: 1
  image:
    tag: "<IMAGE_TAG>"
  ingress:
    enabled: true                    # WITHOUT this the UI pod runs but is never exposed
    className: nginx
    host: <HOST>                     # same FQDN as gateway.ingress.host; /api/* → Gateway routes, /* → UI
  oidc:                              # public values; the browser starts the login here
    issuer: "<OIDC_ISSUER>"          # same IdP as the authenticator
    clientId: "<OIDC_CLIENT_ID>"
    scopes: "openid profile email"   # IdP-specific

# fakeidp: {deploy: false}           # local/dev-only alternative to a real IdP — see note below
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
| `<TOOLBOX_IMAGE>` | Optional. The ingestion toolbox image (drives the WorkflowTemplates and the ClickHouse gold-view migration Job, Step 4/5). Omit to inherit the chart's default, which is pinned to the chart appVersion; set only to override |
| `<AIRBYTE_API_URL>` | Airbyte server API URL, for example `http://host:8001` |
| `<ARGO_INSTANCE_ID>` | The `instanceID` your Argo workflow controller is pinned to — read it off the controller config map in Step 0. Leave empty (`""`) if the controller is unpinned, the common case |
| `<IMAGE_TAG>` | The Insight product image tag. Optional on all five services — each falls back to its subchart's `Chart.yaml` appVersion — but set them explicitly so every service lands on the same build (see the Appendix) |
| `<HOST>` | Public FQDN for the ingress, shared by the Gateway and Frontend, for example `insight.example.com` |
| `<TLS_SECRET>` | Name of the Kubernetes TLS Secret that covers that domain |
| `<OIDC_ISSUER>` | Your IdP's issuer URL. Its `/.well-known/openid-configuration` document must resolve from inside the cluster |
| `<OIDC_CLIENT_ID>` / `<OIDC_CLIENT_SECRET>` | Your OIDC client / application registration credentials |

For infrastructure in the same cluster, use `<service>.<namespace>.svc.cluster.local`. Any resolvable host or IP also works.

Check these four before installing:

- Set `identity.deploy: true`. The chart default is `false`, and without the override Identity — and person resolution for the whole app — never deploys.
- Set real values for `authenticator.oidc.issuerUrl` and `redirectUri`. The chart wraps both in Helm's `required`, and there is no auth-off switch.
- Create the Secret named in `authenticator.signingKeysSecret` before installing (Step 2). The chart does not generate it.
- No real IdP, local/dev only: set `fakeidp.deploy: true`, point `issuerUrl` at the in-cluster fakeidp FQDN, leave `clientSecret` empty. Its image is not on public GHCR, so build and load it locally (or supply an `imagePullSecret`) or the pod hits `ImagePullBackOff`. Never in a shared cluster.

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
NS_INFRA=<your-infra-namespace>                     # where your L2 services run
kubectl -n $NS_INFRA get secret <ch-secret>         -o jsonpath='{.data.<ch-key>}'        | base64 -d; echo   # clickhouse-password
kubectl -n $NS_INFRA get secret <maria-secret>      -o jsonpath='{.data.<app-key>}'       | base64 -d; echo   # mariadb-password (app user)
kubectl -n $NS_INFRA get secret <maria-root-secret> -o jsonpath='{.data.<root-key>}'      | base64 -d; echo   # mariadb-root-password
kubectl -n $NS_INFRA get secret <redis-secret>      -o jsonpath='{.data.<redis-key>}'     | base64 -d; echo   # redis-password
```

Paste each decoded value into the matching field.

> **Never label this Secret `app.kubernetes.io/managed-by: Helm`.** The chart reads the label's *absence* as "bring your own" and composes `insight-analytics-config` and `insight-identity-config` from your values; with the label it takes ownership and may overwrite them with generated passwords.

### secrets/insight-authenticator-signing-keys.yaml

Generate the authenticator's ES256 (EC P-256) gateway-JWT key as PKCS#8 and create the Secret — the chart does not generate it:

```sh
openssl ecparam -name prime256v1 -genkey -noout | openssl pkcs8 -topk8 -nocrypt -out current.pem
kubectl -n insight create secret generic insight-authenticator-signing-keys --from-file=current.pem
```

To rotate, keep the outgoing key as `previous.pem` beside the new `current.pem` for at least the JWT TTL plus downstream JWKS-cache age (~65 minutes), then roll the authenticator pods:

```sh
kubectl -n insight create secret generic insight-authenticator-signing-keys \
  --from-file=current.pem --from-file=previous.pem \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Step 3 — Create namespace and apply secrets

Create the namespace and apply the secret files:

```sh
kubectl create namespace insight
kubectl -n insight apply -f secrets/

# verify
kubectl -n insight get secret insight-db-creds insight-authenticator-signing-keys   # expect 4 keys / 1-2 keys (current.pem [+ previous.pem])
```

Mirror Airbyte's auth Secret into `insight` — Analytics needs it to call the Airbyte API:

```sh
NS_INFRA=<your-infra-namespace>
kubectl -n $NS_INFRA get secret airbyte-auth-secrets -o json \
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
- The install also runs the `insight-clickhouse-migrate` hook Job, which applies the ClickHouse gold-view migrations (`src/ingestion/scripts/migrations/*.sql`) with `ingestion.toolboxImage`. It fires on **every** upgrade, not just the first install (gated by `clickhouse.runMigrations`, default `true`), and a failing migration fails the whole upgrade. It drops and recreates every gold object each run, so a failure points at Bronze/Silver schema or data, not a stale-object conflict.

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
| `helm install`/`upgrade` fails with `tlsDiscovery.issuerRef.name is required` or the `insight-authenticator-authn-tls` Certificate never turns `Ready` | cert-manager isn't installed, or the `ClusterIssuer` named in `authenticator.tlsDiscovery.issuerRef.name` (default `local-ca`) doesn't exist. Confirm with `kubectl get clusterissuer local-ca` and `kubectl -n insight describe certificate insight-authenticator-authn-tls` |
| `helm install`/`upgrade` fails with `authenticator.oidc.issuerUrl is required` / `redirectUri is required` | Both fields are mandatory (`charts/insight/templates/secrets.yaml` wraps them in `required`). Set real values, or for local/dev only, set `fakeidp.deploy: true` and point `issuerUrl` at the in-cluster fakeidp — never disable auth |
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
| `<TOOLBOX_IMAGE>` | `ingestion.toolboxImage` | Optional — defaults to the chart appVersion. Drives the ingestion WorkflowTemplates and the ClickHouse gold-view migrate Job |
| `<AIRBYTE_API_URL>` | `airbyte.apiUrl` | e.g. `http://host:8001` |
| `<ARGO_INSTANCE_ID>` | `ingestion.reconcile.argoInstanceId` | Match the controller's configured `instanceID` (Step 0); empty if unpinned |
| `<IMAGE_TAG>` | `gateway.image.tag`, `authenticator.image.tag`, `analytics.image.tag`, `identity.image.tag`, `frontend.image.tag` | All five are optional — each falls back to that subchart's `Chart.yaml` appVersion (pinned by the release pipeline). Set them explicitly (recommended) so every service lands on the exact same product build; leaving them blank is safe only when all five subcharts' appVersion are in lockstep in the chart release you install |
| `<HOST>` | `gateway.ingress.host`, `frontend.ingress.host` | Public FQDN, shared by the Gateway and Frontend (`/*` → UI, `/api/*` → Gateway routes to Analytics/Identity) |
| `<TLS_SECRET>` | `gateway.ingress.tls.secretName` | Kubernetes TLS Secret name |
| `<OIDC_ISSUER>` | `authenticator.oidc.issuerUrl`, `frontend.oidc.issuer` | Your IdP's issuer URL |
| `<OIDC_CLIENT_ID>` / `<OIDC_CLIENT_SECRET>` | `authenticator.oidc.clientId`/`clientSecret`, `frontend.oidc.clientId` | Your OIDC client / application registration credentials |

Other notable (non-placeholder) settings in this file:

- `credentials.deploymentMode: helm` and `credentials.autoGenerate: true` — this enables the "bring your own" credentials path, where the chart keeps a labelless `insight-db-creds` Secret instead of generating random passwords.
- `identity.deploy: true` — required override; the chart's own default is `false`.
- `authenticator.tlsDiscovery.issuerRef.name: local-ca` — the cert-manager `ClusterIssuer` name the JWKS-discovery Certificate is issued from; override to match your cluster's issuer.
- There is no auth-off toggle anywhere in this chart. `authenticator.oidc.issuerUrl` and `authenticator.oidc.redirectUri` are hard `required` fields — the simplest no-real-IdP path is `fakeidp.deploy: true`, local/dev only; `keycloak.deploy: true` is a heavier bundled alternative (not documented here).

### secrets/insight-db-creds.yaml keys

| Key | Meaning | Consumed by |
|-----|---------|--------------|
| `clickhouse-password` | ClickHouse admin password | Analytics |
| `mariadb-password` | MariaDB app-user password | Analytics + Identity |
| `mariadb-root-password` | MariaDB root password, used by the identity-DB init hook | Identity |
| `redis-password` | Redis password | Analytics + Authenticator |

Recall: this Secret must never carry an `app.kubernetes.io/managed-by: Helm` label.

### secrets/insight-authenticator-signing-keys.yaml keys

| Key | Meaning |
|-----|---------|
| `current.pem` | The active ES256 (EC P-256) signing key, PKCS#8 PEM, unencrypted. Required. |
| `previous.pem` | The outgoing key during a rotation window. Optional; keep it alongside `current.pem` until the JWT TTL plus downstream JWKS-cache age has elapsed |

### values/umbrella.orbstack.yaml (local variant)

This file is a pre-filled variant of the values file for local development on OrbStack's bundled k3s cluster, with all infrastructure running in an `insight-infra` namespace. It sets a fixed tenant UUID, in-cluster DNS hosts, an empty (host-less) ingress that matches any `Host` header, disabled TLS, and points the authenticator at the in-cluster `fakeidp`/`keycloak` provider rather than a real IdP. Use it only as a reference for local testing — do not reuse its host-less ingress, disabled TLS, or fake-IdP settings on a shared or production cluster.
