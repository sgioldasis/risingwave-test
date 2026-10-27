# Jobotron port plan: risingwave-test → dagster-workspace

Plan for porting the **Jobotron component** (core code + tests only) from this
repo onto `feature/NOJIRA-jobotron` in `~/Projects/dagster-workspace`, into a new
capability library, with `defs.yaml` instance(s) in an existing code location.

Decided so far:
- Scope: the component + its tests. Docs, decks, demo server, mutation check,
  live test are out of scope.
- Home: new capability library `libraries/kaizen-dagster-jobotron/` (not an
  extension of `kaizen-dagster-databricks` — Jobotron is Databricks SQL +
  Kafka + Postgres, and its deps must not leak into a Databricks-scoped library).
- One `defs.yaml` instance in an **existing** project so the component is
  exercisable in `dg dev` — host project: **inverness** (data-principals), the
  CY instance first; GR/RO once `dg check defs` passes. Other teams (e.g.
  newport) adopt later by following the same five-file Quickstart.
- Ownership split: the **library is owned and maintained by data-platform**
  (the bristol CODEOWNERS group) — it is platform capability, consumable by any
  location; inverness only owns its instance of it.
- **Publish-only trim (decided 2026-10-27 after live verification):** the target
  component generates exactly one asset per instance — the publish asset — and
  no jobs. `_setup` (topic provisioning), `_consume`/mirror, and `_reset_job`
  are demo scaffolding and stay in this repo's demo harness, where they remain
  runnable. Rationale: the real Jobotron worker consumes the topic; a Dagster
  location mirroring its own topic is a demo stand-in (`mirror.py` docstring),
  and the publish hard-depends on the topic existing — in the target, topic
  provisioning belongs to deployment infrastructure.
- `DATABRICKS_WAREHOUSE_ID` becomes a per-instance `warehouse` attribute
  (§5.3) — the target workspace has no SQL-warehouse precedent either way
  (verified: zero statement-execution usage in `libraries/`, `projects/`,
  `devops/`), so the tiebreaker is the sibling component's pattern of taking
  deployment coordinates as instance attributes.

---

## 1. What is being ported (source inventory)

The self-contained core, from `risingwave-test`:

| File | Lines | Role |
|---|---|---|
| `orchestration/assets/reverse_etl_jobotron.py` | 795 | The engine: `JobotronConfig` (L95), envelope builder `build_messages` (L204), engine seam `_scan` (L392), Statement Execution client `_submit_statement`/`_run_statement` (L282–358), Kafka produce `_produce_one` (L259), Postgres mirror in the consume asset (L614), asset factories `build_setup_asset`/`build_publish_asset`/`build_consume_asset` (L475/L530/L614), reset job (L730), `build_jobotron_defs` (L782). |
| `orchestration/components/reverse_etl_jobotron.py` | 76 | The dg component `JobotronProducer` (L10) — YAML attrs, `to_config()` (L43), `build_defs()` (L75). |
| `tests/test_jobotron.py` | 929 | Contract test suite, fully fake-based (no live services). |
| `tests/fakes.py` | 351 | `FakeDatabricks`/`FakeKafka`/`FakeAdmin`/`bind_connections` the tests bind. |

Transitive first-party dependencies the core imports (port only the used parts):

- `orchestration/assets/reverse_etl_connections.py` — the env contract:
  `connections.databricks()`, `.kafka()`, `.state_postgres()`.
- `orchestration/assets/reverse_etl_cdf_setup.py` — 6 helpers (`_azure_storage_options`,
  `_cost_metadata`, `_poll`, `_rows_as_dicts`, `_with_all_chunks`, `_kafka_producer`/`_config`).
- `orchestration/assets/databricks_optimize.py` — `DATABRICKS_HOST`, `_get_token`, `_poll`.
- `orchestration/assets/kafka_topics_setup.py` — `_DEFAULT_REPLICATION`, `_admin_client`.
- `orchestration/assets/databricks_cost_meter.py` — `record`/`reset`/`snapshot`.

Runtime surface: Databricks Statement Execution API (+ Unity Catalog table
location), SQL warehouse `DATABRICKS_WAREHOUSE_ID`; Kafka output cluster
(admin + producer + consumer, JSON, key `CustomerId`); Postgres state/mirror
(`INSERT … ON CONFLICT (item_id) DO NOTHING`); optional delta-rs/DuckDB engines.

Env vars the code reads: `DBT_DATABRICKS_HOST`, `DATABRICKS_AZURE_TENANT_ID`,
`DATABRICKS_AZURE_CLIENT_ID`, `DATABRICKS_AZURE_CLIENT_SECRET`,
`DATABRICKS_WAREHOUSE_ID` (hard-required), `POSTGRES_HOST` (hard-required),
`POSTGRES_PORT/DB/USER/PASSWORD`, `KAFKA_OUTPUT_BOOTSTRAP` (hard-required),
`KAFKA_OUTPUT_SASL_*`.

## 2. Target conventions (verified in the target repo)

Components live in **capability libraries** under `libraries/`; code locations
hold no Python. The in-tree exemplar on `feature/NOJIRA-jobotron` is
`DatabricksJobOrchestrationComponent`
(`libraries/kaizen-dagster-databricks/src/…/components/databricks_job_orchestration/component.py:191`).
Rules the port must follow:

1. **Class shape**: `class JobotronComponent(Component, Model, Resolvable)` —
   pydantic attributes as `Annotated[T, Resolver.default(description="…")]`;
   descriptions feed the generated schema and the editor. `resolve_from_model`
   for shared pre-resolution; `@model_validator(mode="after")` for config
   validation. The source component already uses `dg.Component/dg.Model/
   dg.Resolvable`, so this is a re-skin, not a rewrite.
2. **Module split by concern** inside `components/jobotron/` (see §3).
3. **Generated JSON schema** `.schemas/components/jobotron.json`, regenerated by
   the pre-commit hook `devops/scripts/generate_component_schema.py` whenever
   the library's `src/` changes — never hand-edited. Instance `defs.yaml`s
   point at it with a `# $schema:` comment whose `../` count depends on depth.
4. **Instance rules**: `asset_key_prefix: [<location>]` (min length 1), literal
   `severity: sev-1|2|3` tag (missing = rejected — Dagster+ alert policies
   select on it), `workspace:` keyed against root `databricks-workspaces.yaml`,
   and `dg check defs` as the deploy-time validation gate.
5. **Library dev loop** (`kaizen-dagster-databricks/README.md` §"Working on the
   library"): `uv run --project libraries/<name> ruff format . / ruff check . /
   basedpyright / pytest`. Locations depend on the library editable via
   `[tool.uv.sources]`, so edits are live on next `dg dev` reload.
6. **CI**: per-project child pipeline (`format/lint/type/test` +
   `validate:defs:<env>:<name>` running `dg check defs` with Vault creds), a
   compatibility job resolving the library at its lowest dependency bounds,
   `devops/scripts/check-workspace.sh` as the workspace gate.
7. **Git**: Conventional Commits scoped by library/project
   (`feat(kaizen-dagster-jobotron): …`); NOJIRA branches append nothing to the
   MR description.
8. **Adopting a library in an existing location touches five files** (README
   Quickstart): `databricks-workspaces.yaml` entry (if needed), project
   `pyproject.toml` (`dependencies` + `[tool.uv.sources]` + `registry_modules`),
   `Dockerfile` COPY lines, path-gate/library_paths in `devops/.gitlab-ci.yml`,
   basedpyright `files:` widening — then the `defs.yaml` instance.

## 3. Target architecture

```
libraries/kaizen-dagster-jobotron/            # NEW capability library (uv project, src layout)
  pyproject.toml  uv.lock  README.md          # README documents the component (config reference, rules)
  src/kaizen_dagster_jobotron/
    __init__.py
    components/
      __init__.py                             # empty, like the sibling library
      jobotron/
        component.py        # JobotronComponent(Component, Model, Resolvable) ← components/reverse_etl_jobotron.py
        config.py           # JobotronConfig / JobotronRunMode / _run_mode_for
        envelope.py         # build_messages, _canonical_json, _sort_key, ENVELOPE_FIELDS
        statements.py       # Statement Execution client (_submit/_cancel/_run_statement)
        engines.py          # _scan, _coerced, _statement_table, delta-rs/DuckDB engines
        kafka_io.py         # _produce_one, _admin_client, _kafka_producer_config, topic create/delete
        mirror.py           # Postgres mirror create/insert/drop
        tokens.py           # _get_token, _require_databricks_env, _azure_storage_options
        cost_meter.py       # ported databricks_cost_meter (thread-locked accumulator)
        naming.py           # instance_name/derived_topic/setup_asset/… derivation
        assets.py           # build_setup_asset / build_publish_asset / build_consume_asset / reset job
  tests/
    conftest.py  jobotron_fakes.py          # ← tests/fakes.py, renamed per databricks_fakes.py convention
    test_envelope.py test_publish.py test_run_mode.py
    test_component_resolution.py            # defs.yaml loading tests (workspace pattern)

projects/inverness/                           # EXISTING location — five-file adoption, not a new location
  pyproject.toml                              # + dependency, [tool.uv.sources], registry_modules
  Dockerfile                                  # + COPY of the library
  src/inverness/defs/jobotron/unverified_customers_cy/defs.yaml   # first instance
```

Workspace plumbing for a new library: `devops/pipelines/libraries/kaizen-dagster-jobotron.yml`,
path gates + `library_paths` in `devops/.gitlab-ci.yml`, CODEOWNERS entry
(**data-platform** — same group as bristol — owns the library; inverness keeps
owning only its location), `dagster-python-projects.code-workspace`,
basedpyright hook in `.pre-commit-config.yaml` (the hook also regenerates the
component schema), `.schemas/components/jobotron.json` generated,
`check-workspace.sh` green.

## 4. Source → target mapping

| Source (risingwave-test) | Target (dagster-workspace) | Note |
|---|---|---|
| `components/reverse_etl_jobotron.py` (`JobotronProducer`) | `libraries/…/components/jobotron/component.py` (`JobotronComponent`) | Same Component/Model/Resolvable shape; re-declare attrs as `Annotated[…, Resolver.default(description=…)]`. |
| `assets/reverse_etl_jobotron.py` engine/clients | Split modules per concern (§3) | The sibling component's `client.py`/`naming.py`/`errors.py`/`metadata.py` split is the model. |
| `reverse_etl_connections.py` (`connections.*`) | Client modules (`statements.py`, `kafka_io.py`, `tokens.py`; `mirror.py` trimmed with the demo scaffolding) | Thin client modules, not a module-level env contract. |
| `defs/unverified_customers_*/defs.yaml` | `projects/inverness/src/inverness/defs/jobotron/<instance>/defs.yaml` | Same YAML keys + `$schema:` comment + `asset_key_prefix: [inverness]` + `severity` tags (none required: nothing is automated). |
| Launchpad dry-run prefill (`_run_mode_for`) | Kept as-is | A `Config` default is exactly the workspace's launchpad philosophy (§6.3). |
| `.env` colon-syntax loader in `definitions.py` | Not ported — env from k8s Secrets (Vault → `container_context.<env>.yaml`) | |
| `tests/test_jobotron.py` + `tests/fakes.py` | `libraries/…/tests/` | Re-skinned imports; fakes renamed `jobotron_fakes.py`. |
| `tests/mutation_check.py` (jobotron group) | Not ported | No mutation job in the target CI; the unit suite covers all 10 mutation targets. |
| `tests/test_live.py` | Not ported | Needs this repo's docker stack; the target's integration gate is `dg check defs`. |
| Demo server, docs, decks | Not ported (agreed) | |

## 5. Credential and config adaptation

The source env contract is re-expressed in the workspace's mechanisms:

1. **Databricks host** — no longer `DBT_DATABRICKS_HOST`; read from the
   repo-root `databricks-workspaces.yaml` (the instance's `workspace:` key
   resolves it, exactly like the sibling component's `resolve_workspace`).
   Inverness has no workspace of its own (`databricks-workspaces.yaml` has no
   `inverness` entry; its observation instance borrows `bristol` by default) —
   the Jobotron instance will name the registered workspace hosting the shards
   and warehouse, most likely `bristol`, following the same borrowing pattern.
2. **Service-principal secret** (`DATABRICKS_AZURE_*`) — same env var names,
   sourced from Vault via the host location's `container_context.<env>.yaml`.
3. **`DATABRICKS_WAREHOUSE_ID`** — becomes a required per-instance `warehouse`
   attribute on the component (same pattern as the sibling component's
   `workspace` attribute: `Annotated[str, Resolver.default(description=…)]`,
   validated by `@model_validator`). There is no SQL-warehouse precedent in the
   target workspace to follow — no statement-execution usage exists anywhere in
   it — so this matches the closest in-tree convention instead.
4. **Postgres + Kafka settings** — stay env vars (k8s Secrets); `POSTGRES_HOST`
   and `KAFKA_OUTPUT_BOOTSTRAP` hard-requirements stay.
5. **No `.env` parsing in the library.**

## 6. Behavior decisions to honor during the port

1. **Engines.** Three engines (`ENGINES`, L92): `databricks_sql` (Statement
   Execution, seconds-class) plus zero-DBU `delta_rs`/DuckDB over ADLS (the
   zero-DBU seam is blocked by UC storage RBAC — see
   `JOBOTRON_CONTRACT_PARITY.md`). Port all three (pure code, fully tested);
   instances default to `databricks_sql`. `deltalake`, `duckdb`, `pyarrow`,
   `duckdb-azure` go in the library's optional extras so the default install
   stays lean.
2. **Statement Execution, not `runs/submit`.** The sibling component launches
   Databricks jobs via `runs/submit`; Jobotron deliberately executes SQL
   statements (token-verbatim SQL against the shards is the contract). Reuse
   nothing from `submit.py`/`client.py` beyond the general style.
3. **Dry-run / launchpad.** Keep `_run_mode_for` per-instance scaffold prefill —
   consistent with the workspace's run-config philosophy ("a run that says
   nothing -- every automated one -- gets the defaults"), extended only by
   making the default visible in the scaffold. Do not adopt
   `DatabricksJobRunConfig.job_parameters` — Jobotron's mode is a typed bool,
   not a string-keyed job parameter.
4. **Cost meter.** Port `databricks_cost_meter` as a small self-contained module
   (feeds warehouse-seconds metadata). Trivially detachable if the workspace
   objects.
5. **Reset safety.** Superseded by the publish-only trim: the reset job (with
   its `jobotron_` name-prefix guard, L721) and the consume/mirror are demo
   scaffolding that stays in this repo — they are not part of the ported
   component.
6. **Envelope.** `build_messages` + `_canonical_json` + `_sort_key` travel
   token-verbatim; the 13-field PascalCase contract and SHA-256 wire hash are
   the point of the port.

## 7. Open questions

None — all three resolved:

1. **Host project: inverness** (data-principals) — first instance of a
   component owned and maintained by data-platform (bristol's CODEOWNERS
   group). Other locations adopt later via the same Quickstart.
2. **Instance set: one first.** Land the CY instance (`unverified_customers_cy`)
   with the component adoption; add GR/RO in a follow-up once `dg check defs`
   passes against the real workspace.
3. **Warehouse id: per-instance attribute** (§5.3).

## 8. Step-by-step plan

Each step = one conventional commit on `feature/NOJIRA-jobotron`.

1. **Scaffold the library** `libraries/kaizen-dagster-jobotron/` (copy the
   sibling library's project plumbing: pyproject, ruff/coverage config, pipeline
   yml, pre-commit hook entries, code-workspace entry, CODEOWNERS). Empty test
   suite passes (`pytest-allow-empty.sh`). —
   `feat(kaizen-dagster-jobotron): scaffold the jobotron capability library`
2. **Port the pure core**: `envelope.py`, `naming.py`, `config.py` +
   `test_envelope.py`. Zero I/O; this is the contract. —
   `feat(kaizen-dagster-jobotron): port the envelope and naming contract`
3. **Port the clients**: `statements.py`, `tokens.py`, `kafka_io.py`,
   `mirror.py`, `engines.py`, `cost_meter.py`, porting the fakes layer with
   them (`test_publish.py`, `test_consume.py`, `test_setup_reset.py`). —
   `feat(kaizen-dagster-jobotron): port the databricks, kafka and mirror clients`
4. **Port the component + asset factories** (`component.py`, `assets.py`,
   run-mode prefill) + `test_run_mode.py`, `test_component_resolution.py`;
   regenerate `.schemas/components/jobotron.json`. —
   `feat(kaizen-dagster-jobotron): port the jobotron component`
5. **Adopt in inverness**: the five-file Quickstart wiring + the CY instance
   `defs.yaml` with `$schema`/`asset_key_prefix`/`severity`. —
   `feat(inverness): instantiate the jobotron component for the CY demo instance`
6. **Wire CI/CD**: library path gates, compatibility job, `dg check defs`
   against Vault. `devops/scripts/check-workspace.sh` green. —
   `chore(devops): wire the jobotron library pipelines`
7. **Library README** documenting the component (YAML schema, dry-run
   semantics, reset guard, engines). —
   `docs(kaizen-dagster-jobotron): document the jobotron component`

Verification gate per step: `uv run --project libraries/kaizen-dagster-jobotron
pytest` (+ ruff/basedpyright), `uv run dg check defs` in the host location,
`devops/scripts/check-workspace.sh` at the end.

## 9. Explicitly out of scope (agreed)

- Docs/decks/data-flow pages (`docs/poc/JOBOTRON_*`, `REVERSE_ETL_*`) — the
  library README (step 7) is the only documentation the port produces.
- Demo server (`scripts/reverse_etl_demo_server.py`) and its bin script.
- `tests/mutation_check.py`, `tests/test_live.py`.
- RisingWave/Redpanda/anything from this repo's stack — the target workspace
  has no Kafka consumer infrastructure; the consume/mirror asset only needs a
  reachable output cluster.
