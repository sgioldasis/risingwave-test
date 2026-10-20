# Reverse ETL syncs vs the Jobotron producer

The orchestration contains two kinds of Kafka producers over Databricks Delta
tables. They sit on the same infrastructure — delta-rs reads, Avro, the staging
schema registry, Kafka — but they solve different problems: the four reverse
ETL syncs are a **CDC pipeline** (state replication), while the Jobotron
producer is the RFC's **upstream publisher** (a job that fans out rows matching
a business rule). This note records the exact overlap and the exact differences.

## Shared infrastructure

Both are defs.yaml-driven Dagster components that:

- **Read Delta directly through delta-rs** — filesystem scans through the
  service principal, no warehouse, no Spark, no DBUs.
- **Produce Avro to Kafka** via confluent_kafka's `AvroSerializer`, with one
  `<topic>-key` and one `<topic>-value` subject per topic in the shared
  staging Apicurio registry.
- **Wrap payloads in an envelope** and carry the Dagster `run_id` with every
  message.

The registry interaction is common enough that both hit the same quirks (see
findings §2.8): registering content that matches an older version returns that
version's id, and reading back the "latest" version lags the write by seconds —
which is why registration decides from the returned schema id, never from a
read-after-write.

## Where they differ

| | Reverse ETL syncs (`avro`, `cdf`, `orders`, `delta`) | Jobotron producer |
|---|---|---|
| **What it reads** | Changes since a watermark — the change feed is reconstructed by diffing Delta versions (append fast path, rewritten-file-pair diff, full-snapshot fallback; see the reader's docstring and findings §6) | Current state — full delta-rs scans registered in DuckDB, then the YAML's `query` selects candidates (day one: no predicate pushdown, hence `scan_columns`) |
| **Incremental?** | Yes — the watermark lives in the sync's state table; each sync emits only what changed since it last committed | No — every run re-selects whoever currently matches the query; nothing tracks what was sent before |
| **What it emits** | Debezium-style change events: `{"before", "after", "op": "c"/"u"/"d", "source": {"commit_version", "commit_timestamp"}}`, keyed on the identity key `rid`. An update's preimage/postimage pair collapses into one event's `before`/`after` | The selected rows as-is: `{"run_id", "job_type", ...row}`, keyed on `key_columns` (the `customer_id`). No ops, no before-images |
| **Routing** | One topic per sync | One topic per operator (`topic_template` filled from `operator_column`) |
| **Downstream** | Kafka → Debezium JDBC sink → Postgres, and RisingWave consuming the topic directly (registry-validated when Avro) | The demo's consume asset archives each operator's topic into its operator table |
| **Encoding** | JSON by default (schema embedded in every message); `encoding: avro` opts into the registry (the `avro` and `delta` syncs) | Avro only; key/value schemas are built from the DuckDB result schema at run time |

## Two different questions

The syncs answer **"what changed in the source?"** — their whole design exists
to keep the targets (Postgres, RisingWave) converged with the source table:
a watermark decides where to read from, per-version diffing decides which
events to emit, and the Debezium envelope shape is what lets RisingWave's
primary-key tables apply `c`/`u`/`d` in order.

The Jobotron producer answers **"who matches the rule right now?"** — the RFC's
example job ("send a PM to customers that remain unverified") is a selection
over current state, not a change stream. Its value is the fan-out semantics
(one message per selected customer, to its own operator's topic), not delta
detection.

The practical consequence: a row updated twice between two syncs produces two
update events on the sync topics (per-version diffing, never netted out), while
a Jobotron re-run produces the customer's current row again regardless of how
many times it changed.

## Where the code lives

- Syncs: `orchestration/components/reverse_etl_cdf_sync.py`,
  defs at `orchestration/defs/reverse_etl_{avro,cdf,orders,delta}/`,
  change-feed reader `orchestration/assets/reverse_etl_delta_reader.py`,
  sync asset `orchestration/assets/reverse_etl_cdf_setup.py`
  (envelope construction, registry registration).
- Jobotron: `orchestration/components/reverse_etl_jobotron.py`, defs at
  `orchestration/defs/reverse_etl_jobotron/`, producer/consume assets in
  `orchestration/assets/reverse_etl_jobotron.py`.

## See also

- `REVERSE_ETL_CODE_WALKTHROUGH.md` — the sync's read paths and schema flow.
- `REVERSE_ETL_JOBOTRON.md` — the producer demo, its RFC framing and registry
  behavior.
- `REVERSE_ETL_SPARK_FREE_FINDINGS.md` — §2.8 (registry id-based dedup),
  §6 (reader scaling, the file-pair diff).
