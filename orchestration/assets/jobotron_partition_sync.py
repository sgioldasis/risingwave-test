"""The host side of the source-based Jobotron instances (the "host registers the
keys" contract in the wheel's README): a sensor that keeps each instance's dynamic
partition set equal to its enumeration, so companies joining or leaving
de.metadatadb.mcompany need no human and no code deploy.

Per source-based instance (discovered from orchestration/defs/*/defs.yaml):

1. enumerate -- the wheel's own `companies_statement` (the instance's source table
   minus its exclude list, ids or exact comCodes as written) run through the wheel's
   statement execution, with the same env-resolved workspace and warehouse the
   instances publish through;
2. diff -- the enumeration's comCodes (the partition keys, codes everywhere) against
   the instance's current dynamic partitions;
3. apply -- add the arrivals; remove the departures only when no run for that
   partition is still in flight (else keep the key and say so -- removing a key
   whose run is executing is ruder than one stale tick);
4. report -- one log line per instance, e.g. `synced: 28 companies, +1/-0`.

The sensor never publishes and touches no customer data: read-only against
Databricks, partition metadata only against the Dagster instance.
"""

import os
from pathlib import Path

import dagster as dg
import yaml
from dagster import DagsterRunStatus, RunsFilter

from kaizen_dagster_jobotron.components.jobotron.core.companies import (
    parse_companies,
    companies_statement,
)
from kaizen_dagster_jobotron.components.jobotron.runtime.assets import (
    dagster_identifier,
)
from kaizen_dagster_jobotron.components.jobotron.source.authentication import (
    open_workspace_client,
    read_credentials,
)
from kaizen_dagster_jobotron.components.jobotron.source.rows import fetch_rows

DEF_ROOT = Path(__file__).resolve().parent.parent / "defs"

_NON_TERMINAL_STATUSES = frozenset(
    {
        DagsterRunStatus.QUEUED,
        DagsterRunStatus.NOT_STARTED,
        DagsterRunStatus.STARTING,
        DagsterRunStatus.STARTED,
        DagsterRunStatus.CANCELING,
    }
)


def scan_source_based_instances(def_root: Path = DEF_ROOT) -> list[dict[str, object]]:
    """The Jobotron yaml instances whose companies come from a source table -- the
    ones the host serves. Static include lists need no host cooperation (their keys
    are in the code), so they are not here. Each entry: the instance's name, its
    dynamic partitions def's name (the same derivation the component does), the
    enumeration source, and the exclude list (ids or exact comCodes, as written)."""
    instances: list[dict[str, object]] = []
    for path in sorted(def_root.glob("*/defs.yaml")):
        doc = yaml.safe_load(path.read_text())
        if "jobotron" not in str(doc.get("type", "")):
            continue  # the root also holds ReverseEtlCdfSync yamls
        attributes = doc.get("attributes") or {}
        companies = attributes.get("companies") or {}
        source = companies.get("source")
        if not source:
            continue
        name = str(attributes["name"])
        prefix = list(attributes.get("asset_key_prefix") or [])
        instances.append(
            {
                "name": name,
                "partitions_name": dagster_identifier(
                    "_".join([*prefix, name, "companies"])
                ),
                "source": str(source),
                "exclude": list(companies.get("exclude") or []),
            }
        )
    return instances


def partition_delta(
    codes: list[str], current: set[str], in_flight: set[str]
) -> tuple[list[str], list[str], list[str]]:
    """The sync's diff: (keys to add, keys safe to remove, departed keys kept because
    a run for them is still in flight)."""
    additions = [key for key in codes if key not in current]
    departed = sorted(current - set(codes))
    removals = [key for key in departed if key not in in_flight]
    kept = [key for key in departed if key in in_flight]
    return additions, removals, kept


def _in_flight_keys(instance: dg.DagsterInstance, keys: set[str]) -> set[str]:
    """The keys among `keys` that have a non-terminal run tagged to them (the
    single-partition launch convention: the `dagster/partition` run tag)."""
    in_flight: set[str] = set()
    for key in keys:
        if instance.get_runs(
            filters=RunsFilter(tags={"dagster/partition": key}, statuses=_NON_TERMINAL_STATUSES),
            limit=1,
        ):
            in_flight.add(key)
    return in_flight


@dg.op
def jobotron_sync_dynamic_partitions(context: dg.OpExecutionContext) -> None:
    instances = scan_source_based_instances()
    if not instances:
        context.log.info("No source-based Jobotron instances -- nothing to sync")
        return
    host = os.environ["DBT_DATABRICKS_HOST"]
    warehouse = os.environ["DATABRICKS_WAREHOUSE_ID"]
    client = open_workspace_client(host, read_credentials(os.environ))
    for inst in instances:
        statement = companies_statement(
            source_table=str(inst["source"]), exclude=inst["exclude"]  # pyright: ignore[reportArgumentType]
        )
        rows = fetch_rows(client, warehouse, statement, context.log)
        codes = [company.com_code for company in parse_companies(rows)]
        partitions_name = str(inst["partitions_name"])
        current = set(context.instance.get_dynamic_partitions(partitions_name))
        in_flight = _in_flight_keys(context.instance, current - set(codes))
        additions, removals, kept = partition_delta(codes, current, in_flight)
        if additions:
            context.instance.add_dynamic_partitions(partitions_name, additions)
        for key in removals:
            context.instance.delete_dynamic_partition(partitions_name, key)
        message = (
            f"{inst['name']}: {len(codes)} companies in the enumeration, "
            + f"+{len(additions)} -{len(removals)} ({len(current)} keys before)"
        )
        if kept:
            message += f"; kept in-flight: {', '.join(kept)}"
        context.log.info(message)


@dg.job(name="jobotron_dynamic_partition_sync_job")
def _jobotron_partition_sync_job():
    jobotron_sync_dynamic_partitions()


@dg.sensor(
    job=_jobotron_partition_sync_job,
    minimum_interval_seconds=60,
    name="jobotron_dynamic_partition_sync",
    description="Keeps the source-based Jobotron instances' dynamic partitions equal "
    + "to their mcompany enumeration (adds arrivals, removes departed companies "
    + "whose partitions are idle).",
)
def jobotron_dynamic_partition_sync(context: dg.SensorEvaluationContext):
    instances = scan_source_based_instances()
    if not instances:
        return dg.SkipReason("No source-based Jobotron instances -- nothing to sync")
    return dg.RunRequest(run_key=None)
