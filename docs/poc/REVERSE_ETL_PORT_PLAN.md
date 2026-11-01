# Reverse-ETL port plan: risingwave-test → dagster-workspace

Plan for porting the **released reverse-ETL component family** (sync + targets,
core code + tests only) from this repo into a new capability library in
`~/Projects/dagster-workspace`, with `defs.yaml` instance(s) in an existing code
location. Ticket: **APR-302** (child of the reverse-ETL epic APR-233). The
Jobotron port (APR-293, `docs/poc/JOBOTRON_PORT_PLAN.md`) is the template —
same target conventions, same verification gates; this plan differs where the
reverse-ETL family is a *family* (one sync component + slim per-target
components) rather than a single component, and where its whole point is the
**zero-DBU read path**.

Decided so far:

- Scope: the **released** component family only — what the release design
  (`docs/poc/REVERSE_ETL_RELEASE_DESIGN.md`) already split out: `ReverseEtlCdfSync`
  + `ReverseEtlRisingwaveTarget` / `ReverseEtlPostgresTarget` /
  `ReverseEtlDuckdbTarget`. The demo variant (`ReverseEtlCdfDemo`), demo hooks,
  change/reset scaffolding, DRT/notebook assets and all demo plumbing stay in
  this repo (same rationale as Jobotron's publish-only trim).
- Home: new capability library `libraries/kaizen-dagster-reverse-etl/` — the
  README skeleton (`docs/poc/REVERSE_ETL_LIBRARY_README.md`) already carries the
  name, the env contract and the boundary rule; extraction is a move, not a
  rewrite.
- Ownership: **data-platform** (bristol CODEOWNERS group) owns the library;
  the host location owns only its instance — same split as Jobotron.
- Zero-DBU is the acceptance bar, not a nice-to-have: the sync reads Delta
  versions with delta-rs over ADLS — no Spark, no SQL warehouse, no statement
  execution, no CDF enablement. One SRE prerequisite (§5.4) gates the first
  live instance.
- The RisingWave target is ported as code but has **no instance** in the target
  workspace (it has no RisingWave); Kafka is the first live destination per
  APR-233. The Postgres target is the production path; DuckDB is dev-volume only.

---

## 1. What is being ported (source inventory)

The released family, from `risingwave-test`:

| File | Lines | Role |
|---|---|---|
| `orchestration/components/reverse_etl_cdf_sync.py` | 121 | The dg component `ReverseEtlCdfSync` — YAML attrs, naming policy, `build_defs()`. |
| `orchestration/components/reverse_etl_targets.py` | 163 | The three slim target components (RisingWave / Postgres / DuckDB). |
| `orchestration/assets/reverse_etl_config.py` | 585 | `CdfSyncConfig` — typed/derived name resolution, the seam. |
| `orchestration/assets/reverse_etl_delta_reader.py` | 688 | The engine: delta-rs version diffing, chunked backlog reads, message building. |
| `orchestration/assets/reverse_etl_state.py` | 281 | Watermark state backends: Postgres (default) + DuckDB, append-only rows, the `source_fqn`/`label` guard. |
| `orchestration/assets/reverse_etl_connections.py` | 216 | The env contract — env-fed connection functions, read at call time. |
| `orchestration/assets/reverse_etl_topic.py` | 57 | The optional compacted-topic asset + bootstrap job. |
| `orchestration/assets/reverse_etl_debezium_sink.py` | 151 | The Postgres target's Debezium JDBC sink assembly. |
| `orchestration/assets/reverse_etl_health.py` | 127 | Sink health check (job + schedule on the Postgres target). |
| `orchestration/assets/reverse_etl_duckdb_target.py` | 131 | The stateless DuckDB mirror target. |
| `orchestration/assets/reverse_etl_risingwave_setup.py` | 351 | The RisingWave target (table create + schema read). Ported, no instance. |

Tests (fake-based, no live services):

| Path | Lines | Covers |
|---|---|---|
| `tests/sync_asset/` (9 files) | ~1,050 | Messages, schema changes, state backends, preconditions, Avro, history/chunks, config surface. |
| `tests/components/cdf_sync/` | ~250 | Sync component attribute/defs resolution. |
| `tests/components/targets/` | ~170 | Target component resolution. |
| `tests/components/hygiene/test_import_hygiene.py` | 104 | The boundary rule — released imports must not reach demo modules. |
| `tests/components/compat/test_defs_folder_load.py` | 85 | `load_from_defs_folder` compatibility (the workspace's loading path). |
| `tests/fakes.py` (the reverse-ETL fakes) | part | `FakeKafka`/`FakeState`/`bind_connections` bound by the suites above. |

Not ported (demo scaffolding, stays runnable in this repo): `reverse_etl_cdf_demo.py`,
`reverse_etl_demo_hooks.py`, `reverse_etl_demo_assets.py`, `reverse_etl_change.py`,
`reverse_etl_reset.py`, `reverse_etl_postgres_setup.py` (demo-schema helpers),
`reverse_etl_drt.py`, `reverse_etl_notebook_job.py`, `reverse_etl_defs.py`,
`reverse_etl_jobotron.py` (already ported as the Jobotron library),
`tests/mutation_check.py`, `tests/test_live.py`, docs/decks/demo server.

## 2. Target conventions (verified during the Jobotron port)

Unchanged from `docs/poc/JOBOTRON_PORT_PLAN.md` §2 — the same rules apply
verbatim: `Component, Model, Resolvable` class shape with
`Annotated[T, Resolver.default(description=…)]`; module split by concern;
generated JSON schema `.schemas/components/reverse_etl.json` via pre-commit
(never hand-edited); instance rules (`asset_key_prefix: [<location>]`, `workspace:`
keyed against root `databricks-workspaces.yaml`, `dg check defs` as the deploy
gate); library dev loop via `uv run --project libraries/kaizen-dagster-reverse-etl …`;
Conventional Commits scoped `feat(kaizen-dagster-reverse-etl): …`; the five-file
adoption Quickstart for the host location.

One additional convention this family hits that Jobotron did not:
**multiple components in one library**. The sibling libraries expose one
component each; this one exposes four (sync + three targets). Precedent for
multi-component structure is the workspace's `components/` package layout
itself — each component gets its own subpackage with its schema file
(`.schemas/components/reverse_etl_cdf_sync.json`, `…_postgres_target.json`, …),
so dg autocomplete and validation stay per-type.

### 2b. Style conventions observed in the shipped Jobotron library (the port follows these)

Read off the actual code on `feature/APR-293-jobotron` (2026-11-01), not from
any style guide:

1. **Narrative module docstrings that argue why.** Every module opens with a
   prose docstring carrying the decisions, the contract cross-references and
   the source line numbers — not a summary of what the code does. `tokens.py`
   explains why there are no Dagster resources and why a SAS token is never an
   option; the port's modules inherit the same voice from
   `REVERSE_ETL_COMPONENT_GUIDE.md` and the release design.
2. **Config flows one direction through one frozen dataclass.** `defs.yaml` →
   component → `to_config()` → one frozen dataclass that everything downstream
   takes and nothing else. Derivations live as `@property` on the frozen
   dataclass, which inherits the naming dataclass (Jobotron:
   `JobotronConfig(JobotronNaming)`; here: `CdfSyncConfig` over the same
   pattern — the release design already shaped the source this way).
3. **Component shape**: `dg.Component, dg.Model, dg.Resolvable`; every attr is
   `Annotated[T, Resolver.default(description=…)]` whose description carries
   the policy ("typed, never derived: it is the wire identity"); a
   `@model_validator(mode="after") validate_configuration` refuses at load time
   what would fail at run time; `@override` on `build_defs`.
4. **Env at call time, never import time**; a `require_*_env()` helper fails
   naming the missing variables. The workspace host is the one exception —
   resolved from `databricks-workspaces.yaml` via `resolve_workspace`.
5. **Sentence-length test names** stating the contract
   (`test_the_query_is_submitted_verbatim_in_the_yaml_context_on_the_instance_warehouse`),
   grouped in `Test*` classes by path; shared fake plumbing in
   `reverse_etl_fakes.py` with an `Env` helper object binding the fakes and
   seeded constants, and an **autouse `clear_ambient_environment` conftest
   fixture** deleting every ambient env var so each test states the environment
   it wants.
6. **Decision-laden pins**: a dependency pin gets a comment saying why it
   exists (Jobotron's `dagster>=1.13.17` carries a three-line rationale in
   pyproject); pytest runs importlib mode + strict + ruff `PT`;
   basedpyright `recommended`.
7. **Lazy imports for heavy deps behind an extra — with one deliberate
   exception here.** Jobotron ships `deltalake`/`duckdb` in an optional
   `engines` extra because `databricks_sql` is its default engine. The
   reverse-ETL port does the opposite: **the delta-rs engine is the point**
   (zero-DBU is the acceptance bar), so `deltalake` + `duckdb` are **base
   dependencies** of `kaizen-dagster-reverse-etl`, imported eagerly, and no
   extra exists.

## 3. Target architecture

```
libraries/kaizen-dagster-reverse-etl/         # NEW capability library (uv project, src layout)
  pyproject.toml  uv.lock  README.md          # README: the skeleton moved + real paths
  src/kaizen_dagster_reverse_etl/
    __init__.py
    components/
      __init__.py
      cdf_sync/
        component.py       # ReverseEtlCdfSync ← components/reverse_etl_cdf_sync.py
        config.py          # CdfSyncConfig, typed/derived naming ← reverse_etl_config.py
        naming.py          # reverse_etl_<name>_<role> derivation + overrides
      targets/
        __init__.py
        risingwave.py      # ReverseEtlRisingwaveTarget (ported, no instance)
        postgres.py        # ReverseEtlPostgresTarget
        duckdb.py          # ReverseEtlDuckdbTarget
    core/                                  # shared, import-clean (hygiene test pins this)
      delta_reader.py    # the engine ← reverse_etl_delta_reader.py
      state.py           # watermark backends + guard ← reverse_etl_state.py
      connections.py     # env contract ← reverse_etl_connections.py
      wire.py            # Debezium envelope, Kafka Connect JSON / Avro subjects
      kafka_io.py        # producer, admin, topic create ← reverse_etl_topic.py helpers
      debezium_sink.py   # JDBC sink assembly ← reverse_etl_debezium_sink.py
      health.py          # sink health check ← reverse_etl_health.py
      duckdb_mirror.py   # ← reverse_etl_duckdb_target.py
  tests/
    conftest.py  reverse_etl_fakes.py
    test_sync_messages.py test_sync_schema_changes.py test_state_backends.py
    test_preconditions.py test_sync_avro.py test_sync_history_and_chunks.py
    test_config_surface.py test_target_components.py
    test_import_hygiene.py test_defs_folder_load.py

projects/<host>/                              # EXISTING location — five-file adoption
  src/<host>/defs/reverse_etl/<instance>/defs.yaml   # first sync instance (+ target YAMLs)
```

Workspace plumbing: `devops/pipelines/libraries/kaizen-dagster-reverse-etl.yml`,
path gates + `library_paths` in `devops/.gitlab-ci.yml`, CODEOWNERS
(**data-platform**), `dagster-python-projects.code-workspace`, basedpyright +
schema-regeneration hooks in `.pre-commit-config.yaml`,
`.schemas/components/*.json` generated, `check-workspace.sh` green.

## 4. Source → target mapping

| Source (risingwave-test) | Target (dagster-workspace) | Note |
|---|---|---|
| `components/reverse_etl_cdf_sync.py` | `components/cdf_sync/component.py` | Re-skin to `Annotated[…, Resolver.default(…)]` attrs; naming policy verbatim. |
| `assets/reverse_etl_config.py` (`CdfSyncConfig`) | `components/cdf_sync/config.py` + `naming.py` | Typed vs derived split already done by the release design; carry the @model_validator checks over. |
| `assets/reverse_etl_delta_reader.py` | `core/delta_reader.py` | The zero-DBU engine — port token-verbatim where possible; this file is the reason the port exists. |
| `assets/reverse_etl_state.py` | `core/state.py` | Both backends + the `source_fqn`/`label` rename guard. |
| `assets/reverse_etl_connections.py` | `core/connections.py` | Env-fed functions, call-time reads; `POSTGRES_HOST` / `KAFKA_OUTPUT_BOOTSTRAP` stay hard-required. |
| `assets/reverse_etl_topic.py` | `core/kafka_io.py` | Topic asset + bootstrap job behind `create_topic`. |
| `assets/reverse_etl_debezium_sink.py` + `_health.py` | `core/debezium_sink.py` + `core/health.py` | Travel together (Postgres target ships both). |
| `assets/reverse_etl_duckdb_target.py` | `core/duckdb_mirror.py` | Stateless mirror, one-writer caveat documented. |
| `assets/reverse_etl_risingwave_setup.py` | `components/targets/risingwave.py` | Ported for completeness; no instance in the target workspace. |
| `tests/sync_asset/*` + component suites | `tests/` (renamed imports, fakes → `reverse_etl_fakes.py`) | Same coverage; hygiene + defs-folder compat suites keep their names. Fakes follow the Jobotron pattern: an `Env` helper binding the fakes, seeded constants, sentence-length test names, and the autouse `clear_ambient_environment` conftest fixture (§2b.5). |
| `.env` colon-syntax loader in `definitions.py` | Not ported — env from k8s Secrets (Vault → `container_context.<env>.yaml`) | Same as Jobotron §4. |
| Demo variant, hooks, change/reset, DRT, notebook job | Not ported (agreed) | Stay in this repo, still runnable. |

## 5. Credential and config adaptation

1. **Databricks host** — no `DBT_DATABRICKS_HOST`; the sync's `workspace:`
   attribute resolves against root `databricks-workspaces.yaml` (sibling
   pattern). Note the delta-rs read path needs the **workspace URL only as the
   control plane** (table-location resolution via the SDK); the data plane is
   direct ADLS reads.
2. **Service principal** (`DATABRICKS_AZURE_*`) — same env names, from Vault
   via the host location's `container_context.<env>.yaml`. Service principal,
   never a SAS token (memory: an expiring SAS silently broke every MV refresh
   once). PATs are not allowed (org policy).
3. **State Postgres** — `POSTGRES_HOST` hard-required, no default
   (`host.docker.internal` does not resolve on the macOS host; one value cannot
   serve containers and host). k8s Secrets, same as Jobotron §5.4.
4. **SRE prerequisite (the one gate on the zero-DBU seam):** delta-rs reads the
   table's storage directly, so the sync's service principal needs UC
   **storage-credential access on the source's ADLS containers** — UC storage
   RBAC blocked exactly this path in the Jobotron investigation
   (`JOBOTRON_CONTRACT_PARITY.md`). File the grant request before the code
   port lands so it is not the long pole.
5. **Schema registry** — stays a config field with an org-specific default
   (no credential).
6. **No `.env` parsing in the library.**

## 6. Behavior decisions to honor during the port

1. **Replay safety is the contract.** Append-only watermark rows, failed runs
   move nothing, targets upsert by the key column. `test_state_backends.py`
   pins it in the source; it must pin it in the target.
2. **The rename guard** (`source_fqn`/`label` anchor, release design step 3) —
   a renamed label must fail loudly, never silently restart. Port with its tests.
3. **Typed vs derived naming** — minted names are derived
   (`reverse_etl_<name>[_<role>]`), owned names are typed (`name`, `catalog`,
   `schema_name`, `source_table`, `key_column`, `state_backend`). The Avro
   subject byte-match is arithmetic, not discipline.
4. **The sync/target seam** — targets see the topic, encoding, key column and
   envelope name; never the source table (exception: RisingWave's `source_fqn`,
   its one explicit composition argument).
5. **The boundary rule** — the library imports nothing demo-shaped; the hygiene
   test travels and keeps passing inside the workspace.
6. **No CDF requirement** — version diffing works on plain Delta tables; do not
   "simplify" to a `readChangeFeed` call (that would re-introduce compute cost
   and a source-table prerequisite).
7. **DuckDB ceilings documented, not fixed** — one writer per file, demo
   volumes only; the Postgres target is the production path.

## 7. Open questions

1. **Host location.** inverness (data-principals) hosted the Jobotron pilot and
   is the path of least resistance — but the reverse-ETL epic's first real
   consumer is E&A. Proposal: inverness for the bring-up instance (parity with
   the Jobotron pattern), E&A adopts via the Quickstart once `dg check defs`
   passes. Decide at step 5.
2. **Kafka output cluster coordinates.** `KAFKA_OUTPUT_BOOTSTRAP` + SASL env
   vars exist in the source demo; which real cluster the first instance
   produces to is a deployment decision for the host location's Vault.
3. **Avro vs JSON for the first instance** — JSON needs no registry; Avro needs
   a schema-registry URL default ratified for the org. Default proposal: JSON
   first, Avro behind the existing config field.

## 8. Step-by-step plan

Each step = one conventional commit on `feature/APR-302-reverse-etl`
(dagster-workspace).

1. **Scaffold the library** `libraries/kaizen-dagster-reverse-etl/` (sibling
   plumbing: pyproject, ruff/coverage, pipeline yml, pre-commit hooks,
   code-workspace, CODEOWNERS). Empty suite passes. —
   `feat(kaizen-dagster-reverse-etl): scaffold the reverse-etl capability library`
2. **Port the pure core**: `naming.py`, `config.py`, `wire.py` +
   `test_config_surface.py`. Zero I/O; the naming policy and the seam are the
   contract. — `feat(kaizen-dagster-reverse-etl): port the naming and seam contract`
3. **Port the engine and state**: `delta_reader.py`, `state.py`,
   `connections.py` + `test_sync_messages.py`, `test_state_backends.py`,
   `test_preconditions.py`, `test_sync_schema_changes.py`,
   `test_sync_history_and_chunks.py`, `test_sync_avro.py` (fakes first). —
   `feat(kaizen-dagster-reverse-etl): port the delta-rs engine and watermark state`
4. **Port the IO clients**: `kafka_io.py`, `debezium_sink.py`, `health.py`,
   `duckdb_mirror.py`. — `feat(kaizen-dagster-reverse-etl): port the kafka, sink and mirror clients`
5. **Port the components** (sync + three targets) + `test_target_components.py`,
   `test_import_hygiene.py`, `test_defs_folder_load.py`; regenerate the
   per-component schemas. — `feat(kaizen-dagster-reverse-etl): port the sync and target components`
6. **Adopt in the host location**: five-file Quickstart + the first instance
   (`defs.yaml`: sync + postgres target, `create_topic: false` if the platform
   owns the topic). — `feat(<host>): instantiate the reverse-etl component`
7. **Wire CI/CD**: library pipelines, path gates, compatibility job, `dg check
   defs` against Vault; `check-workspace.sh` green. —
   `chore(devops): wire the reverse-etl library pipelines`
8. **Library README**: the skeleton from `REVERSE_ETL_LIBRARY_README.md` with
   real paths, the env-contract table, the boundary rule, targets and ceilings. —
   `docs(kaizen-dagster-reverse-etl): document the reverse-etl components`

Verification gate per step: `uv run --project libraries/kaizen-dagster-reverse-etl
pytest` (+ ruff/basedpyright), `uv run dg check defs` in the host location,
`devops/scripts/check-workspace.sh` at the end. The zero-DBU claim itself is
verified once on the first live run: no statement-execution / warehouse calls
in the run's telemetry (cost meter stays at zero).

## 9. Explicitly out of scope (agreed)

- The demo variant, demo hooks, change/reset scaffolding, demo assets — this
  repo keeps them runnable.
- DRT/notebook assets and all comparison/evaluation docs.
- `tests/mutation_check.py`, `tests/test_live.py`.
- RisingWave/Redpanda/Superset from this repo's stack — the target workspace
  has none of it; the RisingWave target ships as code with no instance.
- Docs/decks — the library README (step 8) is the only documentation the port
  produces.
