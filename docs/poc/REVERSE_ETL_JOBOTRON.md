# REVERSE_ETL_JOBOTRON.md — the Jobotron producer (the team's delta-to-kafka component)

**Status: v4 — ADOPTED (2026-11-11). The producer is the Jobotron team's own component
(`kaizen-dagster-delta-to-kafka`, MR !166 on `feature/APR-293-jobotron`), vendored as a wheel and
driven entirely by `defs.yaml`. The local producer code, the 13-field envelope, the zero-DBU scan
engines, and the cost meter are all gone — the team's revised component publishes plain JSON
instead (the envelope descoped under "Productionise Delta-to-Kafka"). The pre-!166 design lives on
in [`JOBOTRON_CONTRACT_PARITY.md`](JOBOTRON_CONTRACT_PARITY.md) and [`JOBOTRON_PORT_PLAN.md`](JOBOTRON_PORT_PLAN.md)
(historical; the RFC context §1–§8 there is unchanged and still the background).**

---

## 1. What runs now

```
orchestration/defs/unverified_customers_{cy,gr,ro}/defs.yaml      ← DeltaToKafkaComponent instances
        │  (Dagster renders {{ env(...) }} in the Resolver fields)
        ▼
kaizen_dagster_delta_to_kafka (vendored wheel, publish-only)
        │  query → Statement Execution API (the SQL warehouse) → one JSON message per row
        ▼
Kafka topic jobotron_unverified_customers_<market>  ──►  the demo's consume asset mirrors to Postgres
```

- **The publish asset is the library's.** Each `defs.yaml` declares
  `type: kaizen_dagster_delta_to_kafka.components.delta_to_kafka.DeltaToKafkaComponent` — the
  query, the warehouse call, the auth, the encoding, the Kafka producer, and the per-run dry-run
  switch all live in the wheel. This repo carries **zero publish-side code**.
- **The local side is demo plumbing only** (`orchestration/assets/reverse_etl_jobotron.py`, ~340
  lines): setup (create the topic), consume (mirror to Postgres), reset (delete topic + drop
  mirror). The library is publish-only by design; these stand in for the worker and the
  scheduler's scaffolding.
- **Vendor mechanics**: `vendor/kaizen_dagster_delta_to_kafka-0.1.0-py3-none-any.whl`, wired via
  `[tool.uv.sources]` in `pyproject.toml`. Rebuild after the team updates the branch:

  ```bash
  # in ~/Projects/dagster-workspace (no branch switch):
  rm -rf /tmp/d2k-build && mkdir -p /tmp/d2k-build
  git archive feature/APR-293-jobotron libraries/kaizen-dagster-delta-to-kafka | tar -x -C /tmp/d2k-build
  cd /tmp/d2k-build/libraries/kaizen-dagster-delta-to-kafka && uv build
  cp dist/kaizen_dagster_delta_to_kafka-*.whl ~/Projects/risingwave-test/vendor/
  # same-version replacement needs:
  uv sync --reinstall-package kaizen-dagster-delta-to-kafka
  ```

  The wheel pins `requires-python >=3.14,<3.15` — a future Python 3.15 bump here is blocked by it.

## 2. The wire contract (the team's, not ours)

One JSON message per selected customer, keyed by `CustomerId` (`message.key_column`). The payload
is the query's result columns plus the component's metadata:

```json
{"CustomerId": 490301250, "CompanyId": 7, "processed_at": "2026-11-11T05:30:17Z", "run_id": "1fba69e5-…"}
```

No registry, no Avro, no envelope — no `JobId`/`ItemNumber`/`ItemPayloadHash`. The mirror dedups
on the **Kafka key** (`CustomerId`, last write wins), not on a content hash. The full-set-every-run
semantics are unchanged: the worker (here, the mirror) owns dedup.

## 3. The instance YAML

Three folders, one per market — see `orchestration/defs/unverified_customers_cy/defs.yaml` for the
fully commented example. The shape is the team's own (their inverness instance is the reference);
repo-specific differences and why:

| Field | Ours | Why |
|---|---|---|
| `workspace` | `"{{ env('DBT_DATABRICKS_HOST') }}"` | no `databricks-workspaces.yaml` here — the literal-host escape hatch |
| `warehouse` | `"{{ env('DATABRICKS_WAREHOUSE_ID') }}"` | env templating instead of a hardcoded id |
| `topic` | `jobotron_unverified_customers_<mkt>` | real names — the demo UI's topics/checks keep working |
| `metadata` | `[processed_at, run_id]` | the component's run metadata on every message |
| `dry_run` | `true` explicitly | documents the safe default; the YAML value governs blank launches |
| `preview_limit` | omitted (=0) | customer data — the run echoes no previews |

The query is the notebook's SQL with tables qualified to the market's physical shard
(`de.voperdb_<mkt>` holds only that market's company — a 29-way UNION scan becomes a
seconds-class statement; evidence in the parity doc).

**Auth**: the component resolves credentials via `read_credentials`: `MULTI_WORKSPACE_ARM_*` →
`ServicePrincipal` (azure-client-secret), else `AmbientCredentials`. **The SDK's ambient chain does
not read `DATABRICKS_AZURE_*`** — so `orchestration/definitions.py` derives the
`MULTI_WORKSPACE_ARM_{TENANT_ID,CLIENT_ID,CLIENT_SECRET}` aliases from the canonical
`DATABRICKS_AZURE_*` values at load time (same SP as the reverse-ETL bridge). The Kafka profile is
the library's baked-in `openshift_kafka_connect` (SASL/SCRAM on the staging cluster); its password
env is `OPENSHIFT_STG_KAFKA_PASSWORD`, derived the same way from `KAFKA_OUTPUT_SASL_PASSWORD` (the
profile pins the name). The aliases are **not stored in `.env`** — one copy of each secret, no
duplication. The recovery script's C0/C0b probes derive them inline (they load `.env` standalone).
Editing `.env` breaks the dagster containers' single-file bind mount — `docker restart
dagster-webserver dagster-daemon` rebinds.

## 4. Demo plumbing (local, topic-parameterized)

| Asset/job | What it does |
|---|---|
| `<instance>_setup` | create-if-not-exists topic (1 partition). **Dry-run default: no-op — not even a Kafka connection.** |
| `<instance>_consume` | drain the topic from the earliest offset (watermark-verified, no timing races), decode JSON, dedup on the Kafka key, mirror into Postgres table named after the topic (`customer_id TEXT PRIMARY KEY … payload JSONB`, `ON CONFLICT DO UPDATE`) |
| `<instance>_reset_job` | delete the topic + drop the mirror. Allowlisted: only `jobotron_*` names |
| publish | **the library asset** (`jobotron/unverified_customers_<mkt>`), launched through `__ASSET_JOB` |

Jobs keep the pre-adoption names (`jobotron_unverified_customers_<mkt>[_setup|_consume|_reset_job]`),
so the demo UI is unchanged. `Definitions.merge` reaches the demo defs via the module attribute
(`reverse_etl_jobotron.jobotron_demo_defs`) — Dagster 1.13 refuses two module-scope
`Definitions` in one file.

## 5. Running it (demo UI, Dagster, or CLI)

Demo UI: `http://localhost:4002/jobotron` — market tabs, per-market buttons
setup → publish (dry-run checkbox) → consume, plus topic/mirror checks and reset.
Publish takes ~11 s wall: ~3 s statement + ~2 s Kafka + ~6 s Dagster multiprocess overhead.

**Dry-run semantics**: the checkbox (or run config) overrides the YAML default per run. With it on,
the full path runs — real query, real message build — and nothing is sent: the run log says
`Dry run: built N message(s) … and sent NOTHING`. The YAML default (`dry_run: true`) governs
blank/scheduled launches, and a request that omits the field entirely is treated as a dry run —
omission is never a real publish.

**Run-config gotcha (found live 2026-11-11, guarded by `tests/test_demo_server.py` + a mutation):**
the per-run config's ops key must be the **full op name** — the asset prefix folds in with a
double underscore:

```json
{"ops": {"jobotron__unverified_customers_cy": {"config": {"dry_run": false}}}}
```

Keyed by the bare asset name, Dagster accepts the run and **silently ignores the config** — the
"real" publish runs as a dry run. The demo server handles this; it matters only if you launch
manually.

### Single terminal, no UI (`dagster asset materialize`)

The cy cycle end-to-end from one terminal, without the Dagster UI or daemon — each command runs
the asset **in-process** and exits. From inside `devbox shell` (which puts the venv's `dbt` on
PATH — required by `_ensure_valid_manifest` — and loads `.env`; only `POSTGRES_HOST` needs
exporting, since `.env` deliberately omits it for the containers):

```bash
devbox shell
export POSTGRES_HOST=localhost      # the one var .env omits on purpose
which dbt                           # sanity: should print .venv/bin/dbt

# 1. setup — create the topic (create-if-not-exists; no-op if it's there)
dagster asset materialize --select jobotron_unverified_customers_cy_setup \
  -m orchestration.definitions \
  --config-json '{"ops":{"jobotron_unverified_customers_cy_setup":{"config":{"dry_run":false}}}}'

# 2. publish — DRY RUN first (real query, real message build, nothing sent)
dagster asset materialize --select 'jobotron/unverified_customers_cy' \
  -m orchestration.definitions \
  --config-json '{"ops":{"jobotron__unverified_customers_cy":{"config":{"dry_run":true}}}}'

# 3. publish — real
dagster asset materialize --select 'jobotron/unverified_customers_cy' \
  -m orchestration.definitions \
  --config-json '{"ops":{"jobotron__unverified_customers_cy":{"config":{"dry_run":false}}}}'

# 4. consume — mirror into Postgres
dagster asset materialize --select jobotron_unverified_customers_cy_consume \
  -m orchestration.definitions
```

Omitting `--config-json` on a publish means the YAML default — `dry_run: true` — so omission is
still a dry run. The other markets work the same way with `_gr`/`_ro` in the names.

Check the mirror directly (`PGUSER`/`PGDATABASE`/`PGHOST` defaults are set by `devbox.json`, so a
bare `psql` works inside the shell):

```bash
psql -c "SELECT count(*) FROM jobotron_unverified_customers_cy"
psql -c "SELECT customer_id, left(payload::text, 120) FROM jobotron_unverified_customers_cy LIMIT 3"
```

## 6. Ops notes (the things that bit us)

- **SP grants regress** (three times as of 2026-11-10): the demo SP periodically loses SELECT on
  the market schemas. Fix by application id, via the `personal` CLI profile:
  `GRANT SELECT ON SCHEMA de.voperdb_<mkt> TO '<sp application id>'`. Failure signature: the
  publish fails fast with `[INSUFFICIENT_PERMISSIONS] … does not have SELECT on Table
  'de.voperdb_<mkt>.cCustomer'`. The durable fix (Entra group mapping) is tracked in the port-plan
  doc.
- **Publish before setup fails fast** — the demo server preflights the topic before a real publish
  and rejects with "run setup first" (~1 s) instead of the library's 120 s `UNKNOWN_TOPIC_OR_PART`
  retry loop. Setup creates the topic; the library deliberately does not. A publish request that
  **omits** the dry-run field is a dry run, not a real publish (guarded by mutations: "an omitted
  mode publishes for real", "publish skips the topic preflight").
- **Recovery**: `bin/2_recover_demo_state.sh` rebuilds the demo end-to-end; its C0 gate waits for
  Databricks storage readiness and its **C0b preflight probes SELECT on every market schema**
  (`de.voperdb_{cy,gr,ro}.cCustomer` + `de.metadatadb.mVerificationStatus`) before the rebuild —
  a missing grant aborts up front with the GRANT command to run, instead of a mid-demo
  `[INSUFFICIENT_PERMISSIONS]` failure.
- **Verified live 2026-11-11** (post-adoption smoke, all three markets):
  cy 51, gr 669, ro 1452 — topic count == mirror count on every round trip; dry-run verified
  side-effect-free.
- Tests: `tests/test_jobotron.py` (plumbing), `tests/test_demo_server.py` (launch shapes),
  mutation groups under `jobotron` in `tests/mutation_check.py`. The wheel's own 148 tests cover
  the component internals.

## 7. Non-goals (unchanged from the RFC era)

Everything the RFC assigns to Jobotron itself: the worker, the WebAPI, the state database, dedup
beyond the demo mirror, eligibility re-validation, Iris/CustomerService/VOperDB actions. The
producer's guarantee is at-least-once, current-as-of-run; the worker is the safety net.
