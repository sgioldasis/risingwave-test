"""Check that the tests notice when the production code is broken.

Each mutation below makes one small change to one production file (a guard removed, a check inverted) and runs the
tests that should catch it. A mutation the tests do NOT catch "survives": that is a gap in the tests, or, if the
mutation turns out to change nothing, a mutation to rewrite. A mutation whose text no longer occurs exactly once is
reported as stale (the code moved on): update it.

The mutants run in a temporary copy of orchestration/, notebooks/, scripts/ and tests/, so the working tree is never edited.

Run from the repo root:  .venv/bin/python tests/mutation_check.py            (all)
                         .venv/bin/python tests/mutation_check.py drt change  (those groups)
Exit status 1 when a mutation survives or is stale. Not collected by pytest (the name has no test_ prefix).
"""

import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ASSETS = "orchestration/assets"


@dataclass(frozen=True)
class Mutation:
    group: str
    name: str
    file: str
    old: str
    new: str
    tests: str  # the pytest target that must fail


MUTATIONS = [
    # --- the drt syncs ---
    Mutation("drt", "failed rows no longer fail the asset", f"{ASSETS}/reverse_etl_drt.py", "            if failed:", "            if False:", "tests/test_drt.py"),
    Mutation(
        "drt", "reset ignores the name allowlist", f"{ASSETS}/reverse_etl_drt.py",
        "if not names.target_table.startswith(cfg.reset_name_prefixes):", "if False:", "tests/test_drt.py",
    ),
    Mutation(
        "drt", "reset deletes every sync's key rows", f"{ASSETS}/reverse_etl_drt.py",
        "WHERE sync_name = %s'", "WHERE 1 = 1 OR sync_name = %s'", "tests/test_drt.py",
    ),
    Mutation(
        "drt", "target has no primary key", f"{ASSETS}/reverse_etl_drt.py",
        'suffix = " PRIMARY KEY" if name == cfg.key_column else ""', 'suffix = ""', "tests/test_drt.py",
    ),
    Mutation("drt", "PGTZ not set", f"{ASSETS}/reverse_etl_drt.py", 'os.environ["PGTZ"] = "UTC"', "pass", "tests/test_drt.py"),
    Mutation(
        "drt", "upsert key not checked", f"{ASSETS}/reverse_etl_drt.py",
        'if destination.get("upsert_key") != [cfg.key_column]:', "if False:", "tests/test_drt.py",
    ),
    Mutation(
        "drt", "setup leaves its connection open", f"{ASSETS}/reverse_etl_drt.py",
        "        finally:\n            conn.close()\n        context.log.info(f\"Ensured", "        finally:\n            pass\n        context.log.info(f\"Ensured",
        "tests/test_drt.py",
    ),
    # --- the notebook trigger ---
    Mutation(
        "notebook", "two Databricks jobs accepted", f"{ASSETS}/reverse_etl_notebook_job.py", "if len(jobs) > 1:", "if False:",
        "tests/test_notebook_job.py",
    ),
    Mutation(
        "notebook", "a failed run passes", f"{ASSETS}/reverse_etl_notebook_job.py", 'if result_state != "SUCCESS":', "if False:",
        "tests/test_notebook_job.py",
    ),
    Mutation(
        "notebook", "any label accepted", f"{ASSETS}/reverse_etl_notebook_job.py", "if not _LABEL_PATTERN.fullmatch(label):", "if False:",
        "tests/test_notebook_job.py",
    ),
    Mutation(
        "notebook", "SKIPPED does not end the wait", f"{ASSETS}/reverse_etl_notebook_job.py",
        '("TERMINATED", "INTERNAL_ERROR", "SKIPPED")', '("TERMINATED", "INTERNAL_ERROR")', "tests/test_notebook_job.py",
    ),
    Mutation(
        "notebook", "RisingWave columns never added", f"{ASSETS}/reverse_etl_notebook_job.py", "    if config.add_risingwave_columns:", "    if False:",
        "tests/test_notebook_job.py",
    ),
    Mutation(
        "notebook", "a rejected run-now is ignored", f"{ASSETS}/reverse_etl_notebook_job.py",
        "    if not resp.ok:\n        raise RuntimeError(f\"Triggering", "    if False:\n        raise RuntimeError(f\"Triggering",
        "tests/test_notebook_job.py",
    ),
    # --- the change asset ---
    Mutation("change", "rename picks the lowest column", f"{ASSETS}/reverse_etl_change.py", "n = max(new_numbers)", "n = min(new_numbers)", "tests/test_change.py"),
    Mutation(
        "change", "drop ignores renamed columns", f"{ASSETS}/reverse_etl_change.py",
        '("new_col_", "renamed_col_")', '("new_col_",)', "tests/test_change.py",
    ),
    Mutation(
        "change", "bulk commits one row too big", f"{ASSETS}/reverse_etl_change.py",
        "chunk = min(BULK_COMMIT_ROWS, rows - emitted)", "chunk = min(BULK_COMMIT_ROWS, rows - emitted) + 1", "tests/test_change.py",
    ),
    Mutation(
        "change", "a skip becomes a failure", f"{ASSETS}/reverse_etl_change.py", "except DemoChangeSkip as skip:", "except KeyError as skip:",
        "tests/test_change.py",
    ),
    Mutation(
        "change", "columns described for every change", f"{ASSETS}/reverse_etl_change.py",
        'if config.change in ("add_column", "rename_column", "drop_column"):', "if True:", "tests/test_change.py",
    ),
    Mutation(
        "change", "stale-config error not reworded", f"{ASSETS}/reverse_etl_change.py", 'if "UNRESOLVED_COLUMN" in str(error):', "if False:",
        "tests/test_change.py",
    ),
    # --- the Jobotron demo plumbing (the publish asset itself is the vendored
    # kaizen-dagster-jobotron wheel, mutation-checked in the library's own harness) ---
    Mutation(
        "jobotron", "dry-run setup creates a topic anyway", f"{ASSETS}/reverse_etl_jobotron.py",
        '        if config.dry_run:\n            # Zero side effects in dry-run',
        '        if False:\n            # Zero side effects in dry-run',
        "tests/test_jobotron.py",
    ),
    Mutation(
        "jobotron", "a dry-run record is mirrored", f"{ASSETS}/reverse_etl_jobotron.py",
        '            if value.get("IsDryRun"):\n                dry_run_count += 1',
        '            if False:\n                dry_run_count += 1',
        "tests/test_jobotron.py",
    ),
    Mutation(
        "jobotron", "mirror dedup loosened", f"{ASSETS}/reverse_etl_jobotron.py",
        '            by_customer[str(customer_id)] = value', '            by_customer[str(len(by_customer))] = value',
        "tests/test_jobotron.py",
    ),
    Mutation(
        "jobotron", "customerless message mirrored", f"{ASSETS}/reverse_etl_jobotron.py",
        '            if customer_id is None:\n                context.log.warning',
        '            if False:\n                context.log.warning',
        "tests/test_jobotron.py",
    ),
    Mutation(
        "jobotron", "reset allowlist off", f"{ASSETS}/reverse_etl_jobotron.py",
        '    if not inst.topic.startswith("jobotron_"):\n        raise RuntimeError(',
        '    if False:\n        raise RuntimeError(',
        "tests/test_jobotron.py",
    ),
    Mutation(
        "jobotron", "publish config keyed by the bare asset name", "scripts/reverse_etl_demo_server.py",
        '        op_name = "__".join(path)', '        op_name = path[-1]',
        "tests/test_demo_server.py",
    ),
    Mutation(
        "jobotron", "an omitted mode publishes for real", "scripts/reverse_etl_demo_server.py",
        '    dry_run = True if dry_run is None else bool(dry_run)',
        '    dry_run = bool(dry_run)',
        "tests/test_demo_server.py",
    ),
    Mutation(
        "jobotron", "publish skips the topic preflight", "scripts/reverse_etl_demo_server.py",
        '    if action == "publish" and not dry_run:',
        '    if False:',
        "tests/test_demo_server.py",
    ),
    # --- the Delta reader ---
    # --- the wire formats ---
    # --- properties ---
    # --- the notebook must stay in step with the asset ---
    Mutation("notebook-parity", "notebook Avro schema text not compact", "notebooks/reverse_etl_cdf_to_kafka.py", 'return json.dumps(schema, separators=(",", ":"))', "return json.dumps(schema)", "tests/test_notebook_parity.py"),
    Mutation("notebook-parity", "notebook maps TINYINT differently", "notebooks/reverse_etl_cdf_to_kafka.py", '"TINYINT": "int8"', '"TINYINT": "int16"', "tests/test_notebook_parity.py"),
    Mutation("notebook-parity", "notebook date loses its logical type", "notebooks/reverse_etl_cdf_to_kafka.py", 'DATE: {"type": "int", "logicalType": "date"},', 'DATE: "int",', "tests/test_notebook_parity.py"),
    Mutation("notebook-parity", "notebook key not sorted", "notebooks/reverse_etl_cdf_to_kafka.py", "json.dumps(key, default=str, sort_keys=True)", "json.dumps(key, default=str)", "tests/test_notebook_parity.py"),
    Mutation("notebook-parity", "notebook reads booleans case-sensitively", "notebooks/reverse_etl_cdf_to_kafka.py", 'str(value).lower() == "true"', 'str(value) == "true"', "tests/test_notebook_parity.py"),
    Mutation(
        "notebook-parity", "notebook pairs updates by truncating", "notebooks/reverse_etl_cdf_to_kafka.py", 'for pre, post in zip_longest(group.get("update_preimage", []), group.get("update_postimage", [])):',
        'for pre, post in zip(group.get("update_preimage", []), group.get("update_postimage", [])):', "tests/test_notebook_parity.py",
    ),
    Mutation(
        "notebook-parity", "notebook date field loses its name", "notebooks/reverse_etl_cdf_to_kafka.py", '{"field": name, "type": "int32", "name": DATE_SCHEMA_NAME, "optional": optional}',
        '{"field": name, "type": "int32", "optional": optional}', "tests/test_notebook_parity.py",
    ),
    Mutation(
        "notebook-parity", "notebook row fields nullable without a default", "notebooks/reverse_etl_cdf_to_kafka.py", '{"name": n, "type": ["null", AVRO_TYPE[t]], "default": None} for n, t, _ in ROW_FIELDS',
        '{"name": n, "type": ["null", AVRO_TYPE[t]]} for n, t, _ in ROW_FIELDS', "tests/test_notebook_parity.py",
    ),
    # --- the RisingWave target ---
    Mutation("risingwave", "drop may remove the key column", f"{ASSETS}/reverse_etl_risingwave_setup.py", "if column in existing and column != cfg.key_column:", "if column in existing:", "tests/test_risingwave_setup.py"),
    Mutation("risingwave", "rename may remove the key column", f"{ASSETS}/reverse_etl_risingwave_setup.py", "if gone in existing and gone != cfg.key_column:", "if gone in existing:", "tests/test_risingwave_setup.py"),
    Mutation("risingwave", "avro schema not registered before ADD", f"{ASSETS}/reverse_etl_risingwave_setup.py", 'if missing and cfg.encoding == "avro":', "if False:", "tests/test_risingwave_setup.py"),
    Mutation("risingwave", "rename drops the column before its companion", f"{ASSETS}/reverse_etl_risingwave_setup.py", 'for gone in (f"{old}_ts", old):', 'for gone in (old, f"{old}_ts"):', "tests/test_risingwave_setup.py"),
    Mutation("risingwave", "rename not idempotent", f"{ASSETS}/reverse_etl_risingwave_setup.py", "if old not in existing and new in existing:", "if False:", "tests/test_risingwave_setup.py"),
    Mutation(
        "risingwave", "rename does not copy the data", f"{ASSETS}/reverse_etl_risingwave_setup.py", 'f"UPDATE {cfg.risingwave_table} SET {_quote(new)} = COALESCE({_quote(new)}, {_quote(old)})"', "pass", "tests/test_risingwave_setup.py",
    ),
    Mutation("risingwave", "companions registered as columns", f"{ASSETS}/reverse_etl_risingwave_setup.py", 'if name in known or name.endswith("_ts"):', "if name in known:", "tests/test_risingwave_setup.py"),
    Mutation("risingwave", "connect timeout changed", f"{ASSETS}/reverse_etl_connections.py", "            # zero-DBU sync ran from outside the container network (2026-10-14).\n            connect_timeout=10,", "            # zero-DBU sync ran from outside the container network (2026-10-14).\n            connect_timeout=60,", "tests/test_risingwave_setup.py"),
    Mutation("risingwave", "topic read from the end", f"{ASSETS}/reverse_etl_risingwave_setup.py", "\"scan.startup.mode = 'earliest'\",", "\"scan.startup.mode = 'latest'\",", "tests/test_risingwave_setup.py"),
    Mutation("risingwave", "missing bootstrap accepted", f"{ASSETS}/reverse_etl_risingwave_setup.py", "if not bootstrap:", "if False:", "tests/test_risingwave_setup.py"),
    # --- the Postgres setup ---
    Mutation("postgres", "rename with both columns skips the copy", f"{ASSETS}/reverse_etl_postgres_setup.py", 'UPDATE {cfg.postgres_table} SET "{new}" = COALESCE("{new}", "{old}")', "pass", "tests/test_postgres_setup.py"),
    Mutation("postgres", "sink not restarted after a rename", f"{ASSETS}/reverse_etl_postgres_setup.py", "    if ensured:\n        restart_sink_task(http, cfg)\n", "    pass\n", "tests/test_postgres_setup.py"),
    Mutation("postgres", "drop may remove the key column", f"{ASSETS}/reverse_etl_postgres_setup.py", "- {cfg.key_column})", ")", "tests/test_postgres_setup.py"),
    Mutation("postgres", "failed restart accepted", f"{ASSETS}/reverse_etl_postgres_setup.py", "(200, 201, 202, 204)", "(200, 201, 202, 204, 500)", "tests/test_postgres_setup.py"),
    # --- the demo component (the reusable core's mutations are under the core signature) ---
    Mutation("component", "a sync override is ignored", "orchestration/components/reverse_etl_cdf_demo.py", '"uniform", "propagate_drops", "encoding", "schema_registry_url", "state_backend", "state_path"', '"uniform", "encoding", "schema_registry_url", "state_backend", "state_path"', "tests/components"),
    Mutation("component", "a targets override is ignored", "orchestration/components/reverse_etl_cdf_demo.py", 'for field in ("sink_tasks_max", "duckdb_target_path", "starrocks_table")', 'for field in ("sink_tasks_max",)', "tests/components"),
    Mutation("component", "a false override is ignored", "orchestration/components/reverse_etl_cdf_demo.py", "if getattr(self, field) is not None", "if getattr(self, field)", "tests/components"),
    Mutation("component", "reset prefixes left as a list", "orchestration/components/reverse_etl_cdf_demo.py", 'overrides["reset_name_prefixes"] = tuple(self.reset_name_prefixes)', 'overrides["reset_name_prefixes"] = self.reset_name_prefixes', "tests/components"),
    Mutation("component", "key column not passed", "orchestration/components/reverse_etl_cdf_demo.py", "key_column=self.sync.key_column,\n            seed_statements", "seed_statements", "tests/components"),
    Mutation("component", "seed statements not passed", "orchestration/components/reverse_etl_cdf_demo.py", "seed_statements=tuple(self.sync.seed_statements or ()),", "seed_statements=(),", "tests/components"),
    Mutation("component", "schema taken from the catalog", "orchestration/components/reverse_etl_cdf_demo.py", "schema=self.sync.schema_name,", "schema=self.sync.catalog,", "tests/components"),
    Mutation("component", "the targets block is not read", "orchestration/components/reverse_etl_cdf_demo.py", "connector_name=self.targets.connector_name,", "connector_name=self.sync.sync_name,", "tests/components"),
    Mutation("component", "build_defs ignores the overrides", "orchestration/components/reverse_etl_cdf_demo.py", "return build_demo_defs(self.to_config())", "return build_demo_defs(ReverseEtlSyncConfig.for_name(self.name, catalog=self.sync.catalog, schema=self.sync.schema_name, source_columns=tuple(self.sync.source_columns or ())))", "tests/components"),
    Mutation("component", "the core's deps are dropped", "orchestration/components/reverse_etl_cdf_sync.py", "build_sync_asset(self.to_config(), deps=team_deps)", "build_sync_asset(self.to_config())", "tests/components/cdf_sync"),
    Mutation("component", "the component skips the topic", "orchestration/components/reverse_etl_cdf_sync.py", "if not self.create_topic:", "if True:", "tests/components/cdf_sync"),
    Mutation("component", "create_topic is ignored", "orchestration/components/reverse_etl_cdf_sync.py", "if not self.create_topic:", "if False:", "tests/components/cdf_sync"),
    Mutation("component", "the topic config is dropped", f"{ASSETS}/reverse_etl_topic.py", 'config = {"cleanup.policy": "compact", **(topic_config or {})}', 'config = {**(topic_config or {})}', "tests/components/cdf_sync"),
    Mutation("component", "sync_name is ignored", "orchestration/components/reverse_etl_cdf_sync.py", 'sync_name=self.sync_name or f"reverse_etl_{self.name}",', 'sync_name=self.sync_name or f"reverse_etl_moved_{self.name}",', "tests/components"),
    Mutation("component", "state_backend requiredness sneaks back", "orchestration/components/reverse_etl_cdf_sync.py", "    state_backend: str\n", '    state_backend: str = "postgres"\n', "tests/components"),
    # Two sites read cfg.state_backend (the resource-key guard and the state build); this
    # anchors on the state build. Either one alone is caught statically (test_config_surface).
    Mutation("component", "topic asset always built", f"{ASSETS}/reverse_etl_defs.py", "    if cfg.create_topic_asset:\n", "    if True:\n", "tests/components"),
    Mutation("component", "seed not part of the setup job", f"{ASSETS}/reverse_etl_defs.py", "    if cfg.seed_statements:\n        job_assets.append(cfg.seed_asset)\n", "", "tests/components"),
    Mutation("component", "make_change built without source columns", f"{ASSETS}/reverse_etl_defs.py", "build_change_asset(cfg) if cfg.source_columns else None", "build_change_asset(cfg)", "tests/components"),
    # --- the core signature: no resources (the env contract), the demo wires its deps in ---
    # --- the target selection (plan section 12, option A): each gate is pinned by a selection's exact asset set ---
    Mutation("targets", "the RisingWave target is built anyway", f"{ASSETS}/reverse_etl_defs.py", '*([build_risingwave_table_asset(risingwave_target_config(cfg, source_fqn))] if "risingwave" in cfg.targets else []),', "*([build_risingwave_table_asset(risingwave_target_config(cfg, source_fqn))]),", "tests/components"),
    Mutation("targets", "the JDBC sink is built anyway", f"{ASSETS}/reverse_etl_defs.py", '*([build_debezium_sink_asset(postgres_cfg)] if has_postgres_target else []),', "*([build_debezium_sink_asset(postgres_cfg)]),", "tests/components"),
    Mutation("targets", "the DuckDB mirror is built anyway", f"{ASSETS}/reverse_etl_defs.py", '*([build_duckdb_target_asset(duckdb_target_config(cfg))] if "duckdb" in cfg.targets else []),', "*([build_duckdb_target_asset(duckdb_target_config(cfg))]),", "tests/components"),
    Mutation("targets", "the RisingWave target joins the setup job anyway", f"{ASSETS}/reverse_etl_defs.py", '*([cfg.risingwave_asset] if "risingwave" in cfg.targets else []),', "*([cfg.risingwave_asset]),", "tests/components"),
    Mutation("targets", "the DuckDB mirror joins the setup job anyway", f"{ASSETS}/reverse_etl_defs.py", '*([f"{cfg.sync_name}_duckdb_target"] if "duckdb" in cfg.targets else []),', "*([f\"{cfg.sync_name}_duckdb_target\"]),", "tests/components"),
    Mutation("targets", "the health check follows no target", f"{ASSETS}/reverse_etl_defs.py", 'has_postgres_target = "postgres" in cfg.targets', "has_postgres_target = True", "tests/components"),
    Mutation("targets", "before_produce feeds an unselected RisingWave", f"{ASSETS}/reverse_etl_demo_hooks.py", 'if "risingwave" in cfg.targets:', "if False:", "tests/sync_asset"),
    Mutation("targets", "after_delivery touches targets it was not given", f"{ASSETS}/reverse_etl_demo_hooks.py", 'if "risingwave" not in cfg.targets and "postgres" not in cfg.targets and "starrocks" not in cfg.targets:', "if False:", "tests/sync_asset"),
    Mutation("targets", "propagate_drops accepts a half selection", f"{ASSETS}/reverse_etl_demo_hooks.py", 'if cfg.propagate_drops and not {"risingwave", "postgres"} <= set(cfg.targets):', "if False:", "tests/sync_asset"),
    Mutation("targets", "tombstones keep the key", f"{ASSETS}/reverse_etl_duckdb_target.py", 'if envelope["op"] == "d":', "if False:", "tests/test_duckdb_target.py"),
    Mutation("targets", "unknown targets pass the config", f"{ASSETS}/reverse_etl_config.py", 'unknown = set(targets) - {"risingwave", "postgres", "duckdb", "starrocks"}', "unknown = set()", "tests/components"),
    Mutation("targets", "the split keeps the sync's selection", f"{ASSETS}/reverse_etl_config.py", "            sink_tasks_max=self.sink_tasks_max,\n            targets=self.targets,\n", "            sink_tasks_max=self.sink_tasks_max,\n            targets=(\"risingwave\", \"postgres\"),\n", "tests/test_targets_defs.py"),
    # --- the slim per-target configs (release design step 4) ---
    Mutation("targets", "a derived name is ignored", f"{ASSETS}/reverse_etl_config.py", 'object.__setattr__(self, "risingwave_table", f"reverse_etl_{self.label}_target")', 'object.__setattr__(self, "risingwave_table", "reverse_etl_moved_target")', "tests/test_targets_defs.py"),
    Mutation("targets", "the envelope drops the schema component", f"{ASSETS}/reverse_etl_config.py", '        return f"{self.sync_name}.{schema}.{table}"', '        return f"{self.sync_name}.{table}"', "tests/test_targets_defs.py"),
    Mutation("targets", "the bridge strips the wrong prefix", f"{ASSETS}/reverse_etl_config.py", '    return RisingwaveTargetConfig(\n        label=cfg.sync_name.removeprefix("reverse_etl_"),', '    return RisingwaveTargetConfig(\n        label=cfg.sync_name.removeprefix("reverse_etl"),', "tests/test_targets_defs.py"),
    # --- the released target components (release design step 5) ---
    Mutation("target-components", "the RisingWave's derived sync_name is ignored", "orchestration/components/reverse_etl_targets.py", '            source_fqn=self.source_fqn,\n            sync_name=self.sync_name or f"reverse_etl_{self.name}",', '            source_fqn=self.source_fqn,\n            sync_name=self.sync_name or f"reverse_etl_moved_{self.name}",', "tests/components/targets"),
    Mutation("target-components", "the sink's derived topic is ignored", "orchestration/components/reverse_etl_targets.py", '        config = PostgresTargetConfig(\n            label=self.name,\n            kafka_topic=self.kafka_topic or f"reverse_etl_{self.name}_topic",', '        config = PostgresTargetConfig(\n            label=self.name,\n            kafka_topic=self.kafka_topic or f"reverse_etl_moved_{self.name}_topic",', "tests/components/targets"),
    Mutation("target-components", "the mirror's derived topic is ignored", "orchestration/components/reverse_etl_targets.py", '        return DuckdbTargetConfig(\n            label=self.name,\n            kafka_topic=self.kafka_topic or f"reverse_etl_{self.name}_topic",', '        return DuckdbTargetConfig(\n            label=self.name,\n            kafka_topic=self.kafka_topic or f"reverse_etl_moved_{self.name}_topic",', "tests/components/targets"),
    Mutation("target-components", "the RisingWave's deps are dropped", "orchestration/components/reverse_etl_targets.py", '            deps=tuple(self.deps or ()),\n            **overrides,', "            deps=(),\n            **overrides,", "tests/components/targets"),
    Mutation("target-components", "the health check is dropped", "orchestration/components/reverse_etl_targets.py", "            asset_checks=[check],", "            asset_checks=[],", "tests/components/targets"),
    Mutation("target-components", "the health job and schedule are dropped", "orchestration/components/reverse_etl_targets.py", "            jobs=[job],\n            schedules=[schedule],", "            jobs=[],\n            schedules=[],", "tests/components/targets"),
    # --- the connector's coordinates (release design step 6): env only, password untouched ---
    Mutation("connector-coords", "the schema qualifier is dropped", f"{ASSETS}/reverse_etl_debezium_sink.py", '        "collection.name.format": cfg.postgres_table,', '        "collection.name.format": cfg.postgres_table.rsplit(".", 1)[-1],', "tests/test_types.py"),
    Mutation("connector-coords", "the password is interpolated", f"{ASSETS}/reverse_etl_debezium_sink.py", '        "connection.password": "${env:POSTGRES_PASSWORD}",', '        "connection.password": os.environ.get("POSTGRES_PASSWORD", "${env:POSTGRES_PASSWORD}"),', "tests/test_types.py"),
    # --- the env contract (release design step 7): connections from the environment, meter optional ---
    Mutation("env-connections", "the state Postgres host is defaulted", f"{ASSETS}/reverse_etl_connections.py", '    host = os.environ.get("POSTGRES_HOST", "")\n    if not host:', '    host = os.environ.get("POSTGRES_HOST", "host.docker.internal")\n    if not host:', "tests/test_postgres_setup.py"),
    # --- hygiene and compatibility (release design step 8): the scan must flag what it forbids ---
    Mutation("hygiene", "the scan misses a demo import", "tests/components/hygiene/test_import_hygiene.py", 'FORBIDDEN = re.compile(r"reverse_etl_(demo_\\w+|change|reset|resources)$")', 'FORBIDDEN = re.compile(r"reverse_etl_(demoX_\\w+|change|reset|resources)$")', "tests/components/hygiene"),
    # --- the core's preconditions: the topic and the state table are the team's (D1, D5) ---
    Mutation("preconditions", "the duckdb backend is ignored", f"{ASSETS}/reverse_etl_state.py", '    if cfg.state_backend == "duckdb":\n        return DuckDBSyncState(cfg.state_path)', "    if False:", "tests/sync_asset/test_state_backends.py"),
    # --- the watermark guard (release design step 3): renames must fail loudly ---
    Mutation("preconditions", "require skips the guard columns", f"{ASSETS}/reverse_etl_state.py", "missing = sorted(set(GUARD_COLUMNS) - columns)", "missing = []", "tests/sync_asset"),
    Mutation("preconditions", "create skips the migration", f"{ASSETS}/reverse_etl_state.py", "                cur.execute(state_table_migration_ddl(self.table))", "                pass", "tests/sync_asset/test_state_backends.py"),
    # --- the sync's hooks (the demo's extension points) ---
    # --- the reset job ---
    Mutation("reset", "a missing group is retried", f"{ASSETS}/reverse_etl_reset.py", "GROUP_ID_NOT_FOUND", "NEVER_MATCHES", "tests/test_reset.py"),
    # --- the sync asset ---
    # --- the Jobotron partition-sync host piece (the wheel's registration contract) ---
    Mutation(
        "jobotron_sync", "the scan serves static instances too", f"{ASSETS}/jobotron_partition_sync.py",
        "        if not source:\n            continue",
        "        if False:\n            continue",
        "tests/test_partition_sync.py",
    ),
    Mutation(
        "jobotron_sync", "the partitions-def name drops the component's derivation", f"{ASSETS}/jobotron_partition_sync.py",
        '                "partitions_name": dagster_identifier(\n                    "_".join([*prefix, name, "companies"])\n                ),',
        '                "partitions_name": dagster_identifier(name),',
        "tests/test_partition_sync.py",
    ),
    Mutation(
        "jobotron_sync", "an arrival is not registered", f"{ASSETS}/jobotron_partition_sync.py",
        "    additions = [key for key in codes if key not in current]",
        "    additions = []",
        "tests/test_partition_sync.py",
    ),
    Mutation(
        "jobotron_sync", "an in-flight departure is removed anyway", f"{ASSETS}/jobotron_partition_sync.py",
        "    removals = [key for key in departed if key not in in_flight]",
        "    removals = [key for key in departed]",
        "tests/test_partition_sync.py",
    ),
]


def run_tests(workdir: Path, target: str) -> bool:
    """True when the tests pass (so a mutation survived)."""
    result = subprocess.run(
        [str(ROOT / ".venv/bin/python"), "-m", "pytest", target, "-q", "-x", "-p", "no:randomly", "-p", "no:cacheprovider"],
        cwd=workdir,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def main(groups: list[str]) -> int:
    chosen = [m for m in MUTATIONS if not groups or m.group in groups]
    if not chosen:
        print(f"no mutations in {groups}; groups are {sorted({m.group for m in MUTATIONS})}")
        return 1
    survived, stale = [], []
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        for folder in ("orchestration", "tests", "notebooks", "scripts"):
            shutil.copytree(ROOT / folder, work / folder, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        shutil.copy(ROOT / "pyproject.toml", work / "pyproject.toml")
        # Without this a copy that cannot run its tests would make every mutation look caught.
        for target in sorted({m.tests for m in chosen}):
            if not run_tests(work, target):
                print(f"BASELINE FAILS: {target} does not pass in the unmodified copy; fix that first")
                return 1
        for mutation in chosen:
            path = work / mutation.file
            original = path.read_text()
            if original.count(mutation.old) != 1:
                print(f"STALE     [{mutation.group}] {mutation.name} (text found {original.count(mutation.old)} times)")
                stale.append(mutation)
                continue
            path.write_text(original.replace(mutation.old, mutation.new))
            try:
                caught = not run_tests(work, mutation.tests)
            finally:
                path.write_text(original)
            print(f"{'caught  ' if caught else 'SURVIVED'}  [{mutation.group}] {mutation.name}")
            if not caught:
                survived.append(mutation)
    print(f"\n{len(chosen) - len(survived) - len(stale)} caught, {len(survived)} survived, {len(stale)} stale")
    return 1 if survived or stale else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
