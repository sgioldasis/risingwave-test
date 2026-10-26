"""Check that the tests notice when the production code is broken.

Each mutation below makes one small change to one production file (a guard removed, a check inverted) and runs the
tests that should catch it. A mutation the tests do NOT catch "survives": that is a gap in the tests, or, if the
mutation turns out to change nothing, a mutation to rewrite. A mutation whose text no longer occurs exactly once is
reported as stale (the code moved on): update it.

The mutants run in a temporary copy of orchestration/, notebooks/ and tests/, so the working tree is never edited.

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
    # --- the Jobotron demo ---
    Mutation("jobotron", "unknown operators published anyway", f"{ASSETS}/reverse_etl_jobotron.py", "if topic is None:", "if False:", "tests/test_jobotron.py"),
    Mutation("jobotron", "rejected deliveries ignored", f"{ASSETS}/reverse_etl_jobotron.py", "if undelivered or delivery_errors:", "if False:", "tests/test_jobotron.py"),
    Mutation(
        "jobotron", "payload fields not nullable", f"{ASSETS}/reverse_etl_jobotron.py", '{"name": field.name, "type": ["null", _avro_type(field.type)], "default": None}',
        '{"name": field.name, "type": _avro_type(field.type)}', "tests/test_jobotron.py",
    ),
    Mutation(
        "jobotron", "latest run taken from one topic", f"{ASSETS}/reverse_etl_jobotron.py", "latest_run = max(stamps)[1] if stamps else None",
        "latest_run = stamps[0][1] if stamps else None", "tests/test_jobotron.py",
    ),
    Mutation("jobotron", "consume does not truncate", f"{ASSETS}/reverse_etl_jobotron.py", '                    cur.execute(f"TRUNCATE {topic}")\n', "", "tests/test_jobotron.py"),
    Mutation(
        "jobotron", "reset allowlist off", f"{ASSETS}/reverse_etl_jobotron.py", "if bad or cfg.schema != \"sr_poc_external\":", "if False:", "tests/test_jobotron.py",
    ),
    Mutation(
        "jobotron", "registry subjects not deleted permanently", f"{ASSETS}/reverse_etl_jobotron.py", 'for params in ({}, {"permanent": "true"}):', "for params in ({},):",
        "tests/test_jobotron.py",
    ),
    Mutation("jobotron", "a skipped change still runs", f"{ASSETS}/reverse_etl_jobotron.py", "        if skip:\n            context.log.warning", "        if False:\n            context.log.warning", "tests/test_jobotron.py"),
    Mutation(
        "jobotron", "location cache kept after the tables are dropped", f"{ASSETS}/reverse_etl_jobotron.py", "        databricks.clear_location_cache()\n", "", "tests/test_jobotron.py",
    ),
    # --- the Delta reader ---
    Mutation("reader", "any load failure treated as missing", f"{ASSETS}/reverse_etl_delta_reader.py", "if not _VERSION_LOAD_FAILURE.search(str(e)):", "if False:", "tests/test_delta_reader.py"),
    Mutation(
        "reader", "the stalest state row wins", f"{ASSETS}/reverse_etl_delta_reader.py", 'freshest = max(rows, key=lambda r: int(r["last_commit_version"] or 0))',
        'freshest = min(rows, key=lambda r: int(r["last_commit_version"] or 0))', "tests/test_delta_reader.py",
    ),
    Mutation("reader", "deletion-vector mask length unchecked", f"{ASSETS}/reverse_etl_delta_reader.py", "if len(mask) != file_table.num_rows:", "if False:", "tests/test_delta_reader.py"),
    Mutation("reader", "deletion-vector mask not applied", f"{ASSETS}/reverse_etl_delta_reader.py", "file_table = file_table.filter(pa.array(mask))", "pass", "tests/test_delta_reader.py"),
    Mutation(
        "reader", "dates left as dates", f"{ASSETS}/reverse_etl_delta_reader.py", "if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):", "if False:",
        "tests/test_delta_reader.py",
    ),
    Mutation("reader", "aware timestamps not converted to UTC", f"{ASSETS}/reverse_etl_delta_reader.py", "if value.tzinfo is not None:", "if False:", "tests/test_delta_reader.py"),
    Mutation(
        "reader", "all-null snapshots not re-read", f"{ASSETS}/reverse_etl_delta_reader.py", "if rows and all(value is None for row in rows for value in row.values()):", "if False:",
        "tests/test_delta_reader.py",
    ),
    Mutation(
        "reader", "snapshot not keyed", f"{ASSETS}/reverse_etl_delta_reader.py", "return {tuple(row[name] for name in key_columns): row for row in rows}", "return {}",
        "tests/test_delta_reader.py",
    ),
    Mutation(
        "reader", "deletion vectors read without an Azure filesystem", f"{ASSETS}/reverse_etl_delta_reader.py", "if dv_enabled and azure is None:", "if False:",
        "tests/test_delta_reader.py",
    ),
    Mutation(
        "reader", "a column present on one side counts as drift", f"{ASSETS}/reverse_etl_delta_reader.py", "for name in (set(old_row) | set(new_row)) - skip", "for name in set(old_row) - skip",
        "tests/test_delta_reader.py",
    ),
    Mutation("reader-dv", "DV tables take the snapshot path", f"{ASSETS}/reverse_etl_delta_reader.py", "use_fast = fast_path or (dv_enabled and azure is not None)", "use_fast = fast_path", "tests/test_delta_reader_dv.py"),
    Mutation(
        "reader-dv", "pre-images read through no mask", f"{ASSETS}/reverse_etl_delta_reader.py", "pre_masks = dv_masks_at(version - 1) if dv_enabled and version > 0 else None",
        "pre_masks = None", "tests/test_delta_reader_dv.py",
    ),
    Mutation(
        "reader-dv", "post-images read through no mask", f"{ASSETS}/reverse_etl_delta_reader.py", "post_masks = dv_masks_at(version) if dv_enabled and added_with_dv else None",
        "post_masks = None", "tests/test_delta_reader_dv.py",
    ),
    Mutation(
        "reader-dv", "pre-image masks taken from the same version", f"{ASSETS}/reverse_etl_delta_reader.py", "dv_masks_at(version - 1) if dv_enabled and version > 0",
        "dv_masks_at(version) if dv_enabled and version > 0", "tests/test_delta_reader_dv.py",
    ),
    Mutation(
        "reader-dv", "a DV-only commit read from the file set", f"{ASSETS}/reverse_etl_delta_reader.py", "adds, removes_actions = _commit_actions(azure, log_dir, version)",
        "adds, removes_actions = [], []", "tests/test_delta_reader_dv.py",
    ),
    Mutation(
        "reader-dv", "partition values not injected", f"{ASSETS}/reverse_etl_delta_reader.py", "added_pv = partition_values_at(version, set(added)) if partition_columns and dv_enabled else None",
        "added_pv = None", "tests/test_delta_reader_dv.py",
    ),
    Mutation(
        "reader-dv", "removed files' partition values not injected", f"{ASSETS}/reverse_etl_delta_reader.py", "if partition_columns and dv_enabled and removed and version > 0",
        "if False", "tests/test_delta_reader_dv.py",
    ),
    # --- the wire formats ---
    Mutation("wire", "Avro schema text no longer compact", f"{ASSETS}/reverse_etl_cdf_setup.py", 'return json.dumps(schema, separators=(",", ":"))', "return json.dumps(schema)", "tests/test_wire_format.py"),
    Mutation(
        "wire", "date loses its logical type", f"{ASSETS}/reverse_etl_cdf_setup.py", 'return {"type": "int", "logicalType": "date"}', 'return "int"', "tests/test_wire_format.py",
    ),
    Mutation(
        "wire", "payload fields not nullable in Avro", f"{ASSETS}/reverse_etl_cdf_setup.py", '{"name": n, "type": ["null", _avro_value_type(t)], "default": None} for n, t, _ in row_fields',
        '{"name": n, "type": _avro_value_type(t)} for n, t, _ in row_fields', "tests/test_wire_format.py",
    ),
    Mutation(
        "wire", "Connect key not sorted", f"{ASSETS}/reverse_etl_cdf_setup.py", "json.dumps(key, default=str, sort_keys=True).encode", "json.dumps(key, default=str).encode", "tests/test_wire_format.py",
    ),
    Mutation(
        "wire", "Connect date loses its name", f"{ASSETS}/reverse_etl_cdf_setup.py", 'return {"field": name, "type": "int32", "name": _DATE_SCHEMA_NAME, "optional": optional}',
        'return {"field": name, "type": "int32", "optional": optional}', "tests/test_wire_format.py",
    ),
    Mutation("wire", "dates not turned into epoch days", f"{ASSETS}/reverse_etl_cdf_setup.py", "return (datetime.date.fromisoformat(str(value)[:10]) - _EPOCH).days", "return str(value)", "tests/test_wire_format.py"),
    Mutation(
        "wire", "envelope fields reordered", f"{ASSETS}/reverse_etl_cdf_setup.py", '{"field": "before", **_connect_row_schema(cfg, optional=True, row_fields=row_fields)},\n            {"field": "after", **_connect_row_schema(cfg, optional=True, row_fields=row_fields)},',
        '{"field": "after", **_connect_row_schema(cfg, optional=True, row_fields=row_fields)},\n            {"field": "before", **_connect_row_schema(cfg, optional=True, row_fields=row_fields)},', "tests/test_wire_format.py",
    ),
    # --- properties ---
    Mutation(
        "props", "chunks one version too long", f"{ASSETS}/reverse_etl_cdf_setup.py", "min(version + size - 1, end)", "min(version + size, end)", "tests/test_properties.py",
    ),
    Mutation("props", "split halves overlap", f"{ASSETS}/reverse_etl_cdf_setup.py", "visit(middle + 1, last)", "visit(middle, last)", "tests/test_properties.py"),
    Mutation("props", "chunks are always split", f"{ASSETS}/reverse_etl_cdf_setup.py", "if first == last or schema_at(first) == schema_at(last):", "if first == last:", "tests/test_properties.py"),
    Mutation("props", "watermark is the last version seen", f"{ASSETS}/reverse_etl_cdf_setup.py", "    return max(versions)\n", "    return versions[-1]\n", "tests/test_properties.py"),
    Mutation("props", "updates not paired positionally", f"{ASSETS}/reverse_etl_cdf_setup.py", "for pre, post in zip_longest(preimages, postimages):", "for pre, post in zip(preimages, postimages):", "tests/test_properties.py"),
    Mutation("props", "dates cut short", f"{ASSETS}/reverse_etl_cdf_setup.py", "str(value)[:10]", "str(value)[:9]", "tests/test_properties.py"),
    Mutation("props", "booleans read case-sensitively", f"{ASSETS}/reverse_etl_cdf_setup.py", 'str(value).lower() == "true"', 'str(value) == "true"', "tests/test_properties.py"),
    Mutation("props", "timestamps lose their UTC conversion", f"{ASSETS}/reverse_etl_delta_reader.py", "value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)", "value = value.replace(tzinfo=None)", "tests/test_properties.py"),
    Mutation("props", "row comparison one-sided", f"{ASSETS}/reverse_etl_delta_reader.py", "for name in (set(old_row) | set(new_row)) - skip", "for name in set(new_row) - skip", "tests/test_properties.py"),
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
        "risingwave", "rename does not copy the data", f"{ASSETS}/reverse_etl_risingwave_setup.py", 'cur.execute(f"UPDATE {cfg.risingwave_table} SET {_quote(new)} = {_quote(old)}")', "pass", "tests/test_risingwave_setup.py",
    ),
    Mutation("risingwave", "companions registered as columns", f"{ASSETS}/reverse_etl_risingwave_setup.py", 'if name in known or name.endswith("_ts"):', "if name in known:", "tests/test_risingwave_setup.py"),
    Mutation("risingwave", "connect timeout changed", f"{ASSETS}/reverse_etl_risingwave_setup.py", "connect_timeout=10,", "connect_timeout=60,", "tests/test_risingwave_setup.py"),
    Mutation("risingwave", "topic read from the end", f"{ASSETS}/reverse_etl_risingwave_setup.py", "\"scan.startup.mode = 'earliest'\",", "\"scan.startup.mode = 'latest'\",", "tests/test_risingwave_setup.py"),
    Mutation("risingwave", "missing bootstrap accepted", f"{ASSETS}/reverse_etl_risingwave_setup.py", "if not bootstrap:", "if False:", "tests/test_risingwave_setup.py"),
    # --- the Postgres setup ---
    Mutation("postgres", "rename with both columns skips the copy", f"{ASSETS}/reverse_etl_postgres_setup.py", 'cur.execute(f\'UPDATE {cfg.postgres_table} SET "{new}" = "{old}"\')', "pass", "tests/test_postgres_setup.py"),
    Mutation("postgres", "sink not restarted after a rename", f"{ASSETS}/reverse_etl_postgres_setup.py", "    if ensured:\n        restart_sink_task(http, cfg)\n", "    pass\n", "tests/test_postgres_setup.py"),
    Mutation("postgres", "drop may remove the key column", f"{ASSETS}/reverse_etl_postgres_setup.py", "- {cfg.key_column})", ")", "tests/test_postgres_setup.py"),
    Mutation("postgres", "failed restart accepted", f"{ASSETS}/reverse_etl_postgres_setup.py", "(200, 201, 202, 204)", "(200, 201, 202, 204, 500)", "tests/test_postgres_setup.py"),
    # --- the demo component (the reusable core's mutations are under the core signature) ---
    Mutation("component", "a sync override is ignored", "orchestration/components/reverse_etl_cdf_demo.py", '"uniform", "propagate_drops", "encoding", "schema_registry_url", "state_backend", "state_path"', '"uniform", "encoding", "schema_registry_url", "state_backend", "state_path"', "tests/components"),
    Mutation("component", "a targets override is ignored", "orchestration/components/reverse_etl_cdf_demo.py", 'for field in ("sink_tasks_max", "duckdb_target_path")', 'for field in ("sink_tasks_max",)', "tests/components"),
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
    Mutation("component", "the asset reads a demo-only field", f"{ASSETS}/reverse_etl_cdf_setup.py", 'context.resources.postgres if cfg.state_backend == "postgres" else None', 'context.resources.postgres if cfg.propagate_drops == "postgres" else None', "tests/sync_asset"),
    Mutation("component", "topic asset always built", f"{ASSETS}/reverse_etl_defs.py", "    if cfg.create_topic_asset:\n", "    if True:\n", "tests/components"),
    Mutation("component", "seed not part of the setup job", f"{ASSETS}/reverse_etl_defs.py", "    if cfg.seed_statements:\n        job_assets.append(cfg.seed_asset)\n", "", "tests/components"),
    Mutation("component", "make_change built without source columns", f"{ASSETS}/reverse_etl_defs.py", "build_change_asset(cfg) if cfg.source_columns else None", "build_change_asset(cfg)", "tests/components"),
    # --- the core signature: resources are decorator-declared, the demo wires the extras in ---
    Mutation("component", "the core requires the demo resources", f"{ASSETS}/reverse_etl_cdf_setup.py", 'required_resource_keys={"databricks", "kafka", "registry"}', 'required_resource_keys={"databricks", "kafka", "registry", "risingwave", "http"}', "tests/sync_asset"),
    Mutation("component", "the demo's extra resources are dropped", f"{ASSETS}/reverse_etl_cdf_setup.py", '| set(extra_resource_keys),', '| set(),', "tests/components"),
    # The postgres resource belongs to the postgres state backend (reverse_etl_state.py): a
    # duckdb-state sync needs only its file, so the core must not require postgres unconditionally.
    Mutation("component", "the postgres resource is required even for duckdb", f"{ASSETS}/reverse_etl_cdf_setup.py", '| ({"postgres"} if cfg.state_backend == "postgres" else set())', '| {"postgres"}', "tests/components"),
    Mutation("component", "the demo's upstream deps are dropped", f"{ASSETS}/reverse_etl_cdf_setup.py", "deps=list(deps) or None,", "deps=None,", "tests/components"),
    # --- the target selection (plan section 12, option A): each gate is pinned by a selection's exact asset set ---
    Mutation("targets", "the RisingWave target is built anyway", f"{ASSETS}/reverse_etl_defs.py", '*([build_risingwave_table_asset(cfg, source_fqn)] if "risingwave" in cfg.targets else []),', "*([build_risingwave_table_asset(cfg, source_fqn)]),", "tests/components"),
    Mutation("targets", "the JDBC sink is built anyway", f"{ASSETS}/reverse_etl_defs.py", '*([build_debezium_sink_asset(cfg)] if "postgres" in cfg.targets else []),', "*([build_debezium_sink_asset(cfg)]),", "tests/components"),
    Mutation("targets", "the DuckDB mirror is built anyway", f"{ASSETS}/reverse_etl_defs.py", '*([build_duckdb_target_asset(cfg)] if "duckdb" in cfg.targets else []),', "*([build_duckdb_target_asset(cfg)]),", "tests/components"),
    Mutation("targets", "the RisingWave target joins the setup job anyway", f"{ASSETS}/reverse_etl_defs.py", '*([cfg.risingwave_asset] if "risingwave" in cfg.targets else []),', "*([cfg.risingwave_asset]),", "tests/components"),
    Mutation("targets", "the DuckDB mirror joins the setup job anyway", f"{ASSETS}/reverse_etl_defs.py", '*([f"{cfg.sync_name}_duckdb_target"] if "duckdb" in cfg.targets else []),', "*([f\"{cfg.sync_name}_duckdb_target\"]),", "tests/components"),
    Mutation("targets", "the health check follows no target", f"{ASSETS}/reverse_etl_defs.py", 'has_postgres_target = "postgres" in cfg.targets', "has_postgres_target = True", "tests/components"),
    Mutation("targets", "before_produce feeds an unselected RisingWave", f"{ASSETS}/reverse_etl_demo_hooks.py", 'if "risingwave" not in cfg.targets:', "if False:", "tests/sync_asset"),
    Mutation("targets", "after_delivery touches targets it was not given", f"{ASSETS}/reverse_etl_demo_hooks.py", 'if "risingwave" not in cfg.targets and "postgres" not in cfg.targets:', "if False:", "tests/sync_asset"),
    Mutation("targets", "propagate_drops accepts a half selection", f"{ASSETS}/reverse_etl_demo_hooks.py", 'if cfg.propagate_drops and not {"risingwave", "postgres"} <= set(cfg.targets):', "if False:", "tests/sync_asset"),
    Mutation("targets", "tombstones keep the key", f"{ASSETS}/reverse_etl_duckdb_target.py", 'if envelope["op"] == "d":', "if False:", "tests/test_duckdb_target.py"),
    Mutation("targets", "unknown targets pass the config", f"{ASSETS}/reverse_etl_config.py", 'unknown = set(targets) - {"risingwave", "postgres", "duckdb"}', "unknown = set()", "tests/components"),
    Mutation("targets", "the split keeps the sync's selection", f"{ASSETS}/reverse_etl_config.py", "            sink_tasks_max=self.sink_tasks_max,\n            targets=self.targets,\n", "            sink_tasks_max=self.sink_tasks_max,\n            targets=(\"risingwave\", \"postgres\"),\n", "tests/test_targets_defs.py"),
    # --- the core's preconditions: the topic and the state table are the team's (D1, D5) ---
    Mutation("preconditions", "the topic check is dropped", f"{ASSETS}/reverse_etl_cdf_setup.py", "if topic not in kafka.admin_client().list_topics(timeout=15).topics:", "if False:", "tests/sync_asset/test_preconditions.py"),
    Mutation("preconditions", "the state check is dropped", f"{ASSETS}/reverse_etl_cdf_setup.py", "        state.require()", "        pass", "tests/sync_asset/test_preconditions.py"),
    Mutation("preconditions", "the DDL is dropped from the error", f"{ASSETS}/reverse_etl_state.py", 'f"{self.ddl()}"', '""', "tests/sync_asset/test_preconditions.py"),
    Mutation("preconditions", "the duckdb backend is ignored", f"{ASSETS}/reverse_etl_state.py", '    if cfg.state_backend == "duckdb":\n        return DuckDBSyncState(cfg.state_path)', "    if False:", "tests/sync_asset/test_state_backends.py"),
    # --- the sync's hooks (the demo's extension points) ---
    Mutation("hooks", "before_produce never called", f"{ASSETS}/reverse_etl_cdf_setup.py", "_merge_hook_metadata(hook_metadata, hooks.before_produce(context, cfg, row_fields))", "pass", "tests/sync_asset/test_sync_hooks.py"),
    Mutation(
        "hooks", "after_delivery never called", f"{ASSETS}/reverse_etl_cdf_setup.py",
        """            if hooks is not None:
                # Here: AFTER the flush above succeeded, BEFORE the watermark
                # write below -- a failing hook fails the run before the
                # watermark moves, and the next run replays the same versions
                # and re-runs it (every hook operation is idempotent).
                _merge_hook_metadata(
                    hook_metadata,
                    hooks.after_delivery(
                        context,
                        cfg,
                        {
                            "row_fields": row_fields,
                            "column_ids": column_ids,
                            "last_schema": last_schema,
                            "event_row_fields": event_row_fields,
                            "event_fields_version": event_fields_version,
                            "source_location": source_location,
                        },
                    ),
                )
""",
        "            pass\n",
        "tests/sync_asset/test_sync_hooks.py",
    ),
    Mutation(
        "hooks", "watermark written before the hook", f"{ASSETS}/reverse_etl_cdf_setup.py",
        """            if hooks is not None:
                # Here: AFTER the flush above succeeded""",
        """            _write_state(postgres, cfg, new_version, _last_schema_json(row_fields, column_ids))
            if hooks is not None:
                # Here: AFTER the flush above succeeded""",
        "tests/sync_asset/test_sync_hooks.py",
    ),
    Mutation("hooks", "idle metadata dropped", f"{ASSETS}/reverse_etl_cdf_setup.py", "**(hooks.idle_metadata() if hooks else {}),", "**{},", "tests/sync_asset/test_sync_hooks.py"),
    Mutation("hooks", "hook metadata dropped from the output", f"{ASSETS}/reverse_etl_cdf_setup.py", "            **hook_metadata,\n", "", "tests/sync_asset/test_sync_hooks.py"),
    # --- the reset job ---
    Mutation("reset", "a missing group is retried", f"{ASSETS}/reverse_etl_reset.py", "GROUP_ID_NOT_FOUND", "NEVER_MATCHES", "tests/test_reset.py"),
    # --- the sync asset ---
    Mutation(
        "sync", "chunks not split at schema changes", f"{ASSETS}/reverse_etl_cdf_setup.py",
        "            chunks = _split_at_schema_changes(chunks, lambda version: get_row_fields(source_location, version=version))\n", "",
        "tests/sync_asset",
    ),
    Mutation(
        "sync", "watermark moves only for chunks with rows", f"{ASSETS}/reverse_etl_cdf_setup.py",
        "                new_version = chunk_watermark if new_version is None else max(new_version, chunk_watermark)\n"
        "                if not rows:\n                    continue\n",
        "                if not rows:\n                    continue\n"
        "                new_version = chunk_watermark if new_version is None else max(new_version, chunk_watermark)\n",
        "tests/sync_asset",
    ),
    Mutation(
        "sync", "producer never flushed", f"{ASSETS}/reverse_etl_cdf_setup.py", "                _flush_producer(cfg, producer, delivery_errors)\n",
        "                pass\n", "tests/sync_asset",
    ),
    Mutation(
        "sync", "aged-out history not recognised", f"{ASSETS}/reverse_etl_delta_reader.py", "|expired|No files in log segment", "|expired",
        "tests/sync_asset",
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
        for folder in ("orchestration", "tests", "notebooks"):
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
