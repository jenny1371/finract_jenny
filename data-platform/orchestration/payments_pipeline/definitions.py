"""Dagster definitions: the scheduled batch pipeline.

    raw_cdc_lake  ->  dbt staging -> intermediate -> marts (reconciliation + daily metrics), with every dbt test as a check

- raw_cdc_lake drains the Debezium topics from Kafka into Parquet (lake_writer) and carries a BLOCKING asset check:
  every Debezium connector must be RUNNING. If capture is down the lake silently stops growing and the metrics would
  look fine and be wrong, so the run stops here instead of publishing them.
- the dbt assets run `dbt build`: models and tests together; a failing test fails the run, so a bad table is never
  published as "yesterday's numbers".
- reconcile_daily runs at 02:00 UTC for the previous day's data and is idempotent: running it twice changes nothing.

    cd data-platform/orchestration
    dagster dev -m payments_pipeline.definitions            # UI on http://localhost:3000
    dagster job execute -m payments_pipeline.definitions -j reconcile_daily
"""
import json
import os
import sys
import urllib.request
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Config,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

ROOT = Path(__file__).resolve().parents[2]            # data-platform/
sys.path.insert(0, str(ROOT))                         # lake_writer lives next to the orchestration project
LAKE = Path(os.environ.get("LAKE_DIR", ROOT / "lake"))
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8093")

dbt_project = DbtProject(project_dir=ROOT / "dbt", profiles_dir=ROOT / "dbt")
dbt_project.prepare_if_dev()

# One asset per source table, all produced by the same capture step (one Kafka read feeds every table).
# dbt source name -> "<database>.<table>" as lake_writer reports it
LAKE_SOURCE = {
    "orders": "orders.orders",
    "payments": "payments.payments",
    "ledger_entries": "ledger.ledger_entries",
    "fineract_savings_account": "fineract_default.m_savings_account",
    "fineract_savings_transaction": "fineract_default.m_savings_account_transaction",
    "fineract_note": "fineract_default.m_note",
}
LAKE_TABLES = list(LAKE_SOURCE)
RAW_KEYS = [AssetKey(["lake", t]) for t in LAKE_TABLES]


class CaptureConfig(Config):
    idle_seconds: float = 8.0          # stop reading when no new change event arrives for this long


def connector_states() -> dict[str, str]:
    """name -> RUNNING / FAILED / ... for every Debezium connector, plus its tasks."""
    def get(path: str):
        with urllib.request.urlopen(CONNECT_URL + path, timeout=15) as response:
            return json.loads(response.read())
    states = {}
    for name in get("/connectors"):
        status = get(f"/connectors/{name}/status")
        states[name] = "RUNNING" if status["connector"]["state"] == "RUNNING" and status["tasks"] and all(
            t["state"] == "RUNNING" for t in status["tasks"]) else "NOT_RUNNING"
    return states


@multi_asset(
    specs=[AssetSpec(key, group_name="ingest", description="Change events as Parquet, partitioned by day. Append only.")
           for key in RAW_KEYS],
    # blocking: when this fails nothing downstream runs, so stale tables are never published as current numbers
    check_specs=[AssetCheckSpec("all_connectors_running", asset=RAW_KEYS[0], blocking=True)],
    name="raw_cdc_lake",
)
def raw_cdc_lake(context: AssetExecutionContext, config: CaptureConfig):
    from lake_writer.writer import drain

    written = drain(BOOTSTRAP, LAKE, idle_seconds=config.idle_seconds)
    context.log.info("rows written per table: %s", written)
    for table, key in zip(LAKE_TABLES, RAW_KEYS):
        yield MaterializeResult(asset_key=key, metadata={"rows_written": written.get(LAKE_SOURCE[table], 0)})

    try:
        states = connector_states()
        failed = sorted(n for n, s in states.items() if s != "RUNNING")
        ok = bool(states) and not failed
        description = "all connectors RUNNING" if ok else f"not running: {failed or 'no connectors registered'}"
    except Exception as error:            # Connect unreachable counts as "capture is down"
        ok, states, description = False, {}, f"Kafka Connect unreachable: {error}"
    yield AssetCheckResult(check_name="all_connectors_running", asset_key=RAW_KEYS[0], passed=ok, severity=AssetCheckSeverity.ERROR,
                           description=description, metadata={"connectors": states})


class Translator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey(["lake", dbt_resource_props["name"]])        # the raw tables, so dbt runs after the capture
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=Translator(), project=dbt_project)
def payments_dbt(context: AssetExecutionContext, dbt: DbtCliResource):
    # `build` = run + test in dependency order. A failing test raises, which fails the run.
    yield from dbt.cli(["build"], context=context).stream()


reconcile_daily = define_asset_job(
    "reconcile_daily",
    selection=AssetSelection.all(),
    description="Capture -> staging -> batch reconciliation -> daily metrics, with data-quality tests.",
)

defs = Definitions(
    assets=[raw_cdc_lake, payments_dbt],
    jobs=[reconcile_daily],
    schedules=[ScheduleDefinition(job=reconcile_daily, cron_schedule="0 2 * * *", execution_timezone="UTC")],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
