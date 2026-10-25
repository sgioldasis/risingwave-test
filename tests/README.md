# Tests

The reverse-ETL code (`orchestration/assets/reverse_etl_*.py`) is tested without any services: every resource
(Postgres, RisingWave, Databricks, Kafka, HTTP, schema registry) is replaced by a fake from `tests/fakes.py`, bound
under the keys the project registers. Each fake records what it was asked to do in one shared `calls` list of
`(kind, detail)` tuples, so a test asserts what happened and in which order.

```bash
.venv/bin/python -m pytest                              # the default run: everything except `live`
.venv/bin/python -m pytest --cov --cov-report=term-missing   # with the coverage floor (see pyproject.toml)
.venv/bin/ruff check tests orchestration/assets/reverse_etl_*.py orchestration/definitions.py
```

Warnings are errors (`filterwarnings = error` in `pyproject.toml`, with a short list of known exceptions), tests time
out after 60 s, and the order is randomised (`pytest-randomly`), so a test that depends on another one fails.

## Layers

| Layer | Where | What it protects |
|---|---|---|
| Unit | `test_change.py`, `test_reset.py`, `test_sink.py`, `test_table_setup.py`, `test_risingwave_setup.py`, `test_postgres_setup.py`, `test_types.py`, `test_notebook_job.py`, `test_drt.py`, `test_jobotron.py`, `test_control_plane_cache.py`, `test_delta_reader*.py` | One module's behaviour, including its failure and retry paths |
| Sync asset | `sync_asset/` (shared setup in `support.py`, `conftest.py`) | The whole sync end to end against a real local Delta table: messages, Avro, schema changes, history and chunks |
| Golden | `test_wire_format.py`, `golden/` | The exact bytes produced for Connect JSON and Avro. Change them only on purpose |
| Property | `test_properties.py` | Invariants checked on generated inputs with hypothesis (chunking, watermark, coercion, timestamps) |
| Parity | `test_notebook_parity.py` | The Databricks notebook builds what the asset builds. Its pure functions are lifted out with `ast` and compared; nothing runs Spark |
| Fake contract | `test_fakes_contract.py` | Every public method of a real resource exists on its fake with the same parameters |
| Components | `components/<component>/` (so far `cdf_sync/`: attributes, built definitions, and the real `defs.yaml` files) | Each Dagster component on its own: required and unknown attributes, defaults, overrides, the assets, jobs, check and schedule it builds, and that every `defs.yaml` using it validates, loads and collides with no other. Runs in CI, no dbt manifests needed |
| Project definitions | `test_definitions.py` | The whole project's definitions register every resource the code asks for. Skipped when the dbt manifests are missing, which includes CI: `dbt parse` here runs SQL against a live RisingWave |
| Live | `test_live.py` (marker `live`) | Jobotron and a rename in a backlog against the running local stack |
| Mutation | `mutation_check.py` | Whether the tests notice a broken line (below) |

`fixtures/delta_dv/` holds synthetic Delta tables written by Databricks with deletion vectors; delta-rs cannot write
those, so they are checked in. Ids in them are scrubbed.

## Regenerating the golden files

```bash
UPDATE_GOLDEN=1 .venv/bin/python -m pytest tests/test_wire_format.py
git diff tests/golden        # read the diff: every changed byte is a change to the wire format
```

## Live tests

They need the local stack (`./bin/1_up.sh`) and run through `docker exec dagster-webserver`:

```bash
.venv/bin/python -m pytest -m live
```

`orchestration/` is bind-mounted into the Dagster containers, so restart them after a code change before running these.

## Mutation check

`mutation_check.py` applies each listed mutation (a one-line break of the production code) in a temporary copy of
`orchestration/`, `tests/` and `notebooks/`, runs the tests that should notice, and reports `caught` or `SURVIVED`. It
first runs the unmutated copy, so a red baseline stops it. The real files are never edited.

```bash
.venv/bin/python tests/mutation_check.py              # all groups
.venv/bin/python tests/mutation_check.py reset postgres   # only these groups
```

A survivor means a test is missing or too weak: add the test, do not delete the mutation. A "stale" mutation means
its search text no longer occurs in the code; update the entry. When you add a test for a failure path, add a
mutation for it. The same run happens weekly in `.github/workflows/mutation.yml` (and on manual dispatch).

## CI

`.github/workflows/tests.yml` runs ruff and `pytest --cov` on every push and pull request. The workflows pin actions
to commit SHAs; Dependabot (`.github/dependabot.yml`) proposes the bumps.
