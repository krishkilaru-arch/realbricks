from __future__ import annotations

from dataclasses import asdict
import os
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from realbricks.agents.document_offer import DocumentOfferAgent
from realbricks.connectors.freddie_mac import FreddieMacClient
from realbricks.connectors.repliers import RepliersClient
from realbricks.orchestration.supervisor import SupervisorAgent
from realbricks.pipelines.ingest import load_source_configs, run_ingestion
from realbricks.pipelines.models import (
    build_gold_market_kpis,
    build_gold_property_rankings,
    build_silver_listing_core,
)
from realbricks.settings import Settings


def _run_ingest_models(
    spark: SparkSession,
    settings: Settings,
    config_path: str,
    run_id: str = "ad-hoc",
) -> dict[str, str]:
    sources = load_source_configs(config_path)
    run_ingestion(spark, settings, sources, run_id=run_id)
    bronze_tables = [src.target_table for src in sources if src.enabled]
    silver = build_silver_listing_core(spark, settings, bronze_tables)
    gold_rank = build_gold_property_rankings(spark, settings)
    gold_kpi = build_gold_market_kpis(spark, settings)
    return {
        "silver_listing_core": silver,
        "gold_property_rankings": gold_rank,
        "gold_market_kpis": gold_kpi,
    }


def _last_deal_snapshot(spark: SparkSession, settings: Settings, deal_id: str) -> dict[str, Any]:
    if not spark.catalog.tableExists(settings.deal_state_table):
        return {}

    row = (
        spark.table(settings.deal_state_table)
        .where(F.col("deal_id") == deal_id)
        .orderBy(F.col("event_time").desc())
        .select("payload_json")
        .limit(1)
        .collect()
    )
    if not row:
        return {}
    return {"latest_payload_json": row[0]["payload_json"]}


def _integration_healthcheck() -> dict[str, Any]:
    checks: dict[str, Any] = {
        "repliers": {"configured": False, "status": "not_configured"},
        "freddie_mac": {"configured": False, "status": "not_configured"},
    }

    repliers_key = os.getenv("REPLIERS_API_KEY", "")
    if repliers_key:
        checks["repliers"]["configured"] = True
        try:
            client = RepliersClient(api_key=repliers_key)
            sample = client.search_listings(limit=1)
            checks["repliers"]["status"] = "ok"
            checks["repliers"]["sample_keys"] = list(sample.keys())[:10]
        except Exception as exc:
            checks["repliers"]["status"] = "error"
            checks["repliers"]["error"] = str(exc)

    freddie_token = os.getenv("FREDDIE_MAC_BEARER_TOKEN", "")
    freddie_path = os.getenv("FREDDIE_MAC_HEALTHCHECK_PATH", "")
    if freddie_token and freddie_path:
        checks["freddie_mac"]["configured"] = True
        try:
            client = FreddieMacClient(bearer_token=freddie_token)
            sample = client.get_rates_snapshot(endpoint_path=freddie_path)
            checks["freddie_mac"]["status"] = "ok"
            checks["freddie_mac"]["sample_keys"] = list(sample.keys())[:10]
        except Exception as exc:
            checks["freddie_mac"]["status"] = "error"
            checks["freddie_mac"]["error"] = str(exc)

    return checks


def run_phase_1(
    spark: SparkSession,
    settings: Settings,
    config_path: str,
    deal_id: str,
    lead_payload: dict[str, Any],
    run_id: str = "ad-hoc",
) -> dict[str, Any]:
    built = _run_ingest_models(spark, settings, config_path, run_id=run_id)
    supervisor = SupervisorAgent(spark, settings)
    result = supervisor.run(deal_id=deal_id, lead_payload=lead_payload)
    return {"phase": 1, "tables": built, "deal_result": result}


def run_phase_2(
    spark: SparkSession,
    settings: Settings,
    config_path: str,
    deal_id: str,
    lead_payload: dict[str, Any],
    offer_text: str = "",
    run_id: str = "ad-hoc",
) -> dict[str, Any]:
    phase_1 = run_phase_1(spark, settings, config_path, deal_id, lead_payload, run_id=run_id)

    doc_agent = DocumentOfferAgent()
    extraction = doc_agent.extract(offer_text)

    supervisor = SupervisorAgent(spark, settings)
    supervisor.record_event(deal_id, "document_offer", asdict(extraction))

    affordability_grid = []
    for rate in (0.055, 0.0625, 0.07, 0.0775):
        scenario = dict(lead_payload)
        scenario["interest_rate"] = rate
        selected = phase_1["deal_result"].get("top_matches", [{}])[0] if phase_1["deal_result"].get("top_matches") else None
        financing = supervisor.mortgage_agent.assess(scenario, selected)
        affordability_grid.append({"interest_rate": rate, **financing})

    supervisor.record_event(deal_id, "what_if_affordability", {"scenarios": affordability_grid})
    return {
        "phase": 2,
        "phase_1": phase_1,
        "document_offer": asdict(extraction),
        "what_if_affordability": affordability_grid,
    }


def run_phase_3(
    spark: SparkSession,
    settings: Settings,
    config_path: str,
    deal_id: str,
    lead_payload: dict[str, Any],
    run_id: str = "ad-hoc",
) -> dict[str, Any]:
    phase_1 = run_phase_1(spark, settings, config_path, deal_id, lead_payload, run_id=run_id)
    snapshot = _last_deal_snapshot(spark, settings, deal_id)
    return {
        "phase": 3,
        "phase_1": phase_1,
        "production_integration_ready": {
            "crm_adapter": "table_ingest_enabled",
            "lender_adapter": "connector_healthcheck_enabled",
            "document_ingest_adapter": "text_extract_enabled",
            "external_connectors": _integration_healthcheck(),
            "state_snapshot": snapshot,
        },
    }


def run_phase_4(
    spark: SparkSession,
    settings: Settings,
    config_path: str,
    deals: list[dict[str, Any]],
    run_id: str = "ad-hoc",
) -> dict[str, Any]:
    _run_ingest_models(spark, settings, config_path, run_id=run_id)
    supervisor = SupervisorAgent(spark, settings)
    outcomes = []
    for item in deals:
        outcomes.append(
            supervisor.run(
                deal_id=item["deal_id"],
                lead_payload=item["lead_payload"],
            )
        )

    ready = sum(1 for x in outcomes if x.get("status") == "ready_for_offer")
    total = len(outcomes)
    summary = {
        "total_deals": total,
        "ready_for_offer": ready,
        "ready_rate": round((ready / total), 4) if total else 0.0,
    }
    return {"phase": 4, "summary": summary, "sample_outcomes": outcomes[:10]}

