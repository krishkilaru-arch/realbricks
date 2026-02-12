from __future__ import annotations

import argparse
import json
import sys

from realbricks.observability import RunTracker
from realbricks.orchestration.phases import run_phase_1, run_phase_2, run_phase_3, run_phase_4
from realbricks.orchestration.supervisor import SupervisorAgent
from realbricks.pipelines.ingest import load_source_configs, run_ingestion
from realbricks.pipelines.models import build_gold_market_kpis, build_gold_property_rankings, build_silver_listing_core
from realbricks.settings import get_settings
from realbricks.spark_utils import get_spark


def run_ingest_cmd(config_path: str) -> None:
    spark = get_spark("realbricks-ingest")
    settings = get_settings()
    tracker = RunTracker(spark, settings)
    with tracker.track("ingest") as run:
        sources = load_source_configs(config_path)
        loaded = run_ingestion(spark, settings, sources, run_id=run.run_id)
        print(json.dumps({"run_id": run.run_id, "loaded_tables": loaded}, indent=2))


def run_models_cmd(config_path: str) -> None:
    spark = get_spark("realbricks-models")
    settings = get_settings()
    tracker = RunTracker(spark, settings)
    with tracker.track("models") as run:
        sources = load_source_configs(config_path)
        bronze_tables = [src.target_table for src in sources if src.enabled]

        silver = build_silver_listing_core(spark, settings, bronze_tables)
        gold = build_gold_property_rankings(spark, settings)
        kpis = build_gold_market_kpis(spark, settings)
        print(json.dumps({"run_id": run.run_id, "silver_table": silver, "gold_table": gold, "kpi_table": kpis}, indent=2))


def run_deal_cmd(deal_id: str, lead_json: str) -> None:
    spark = get_spark("realbricks-supervisor")
    settings = get_settings()
    tracker = RunTracker(spark, settings)
    with tracker.track("deal") as run:
        supervisor = SupervisorAgent(spark, settings)
        lead = json.loads(lead_json)
        result = supervisor.run(deal_id=deal_id, lead_payload=lead)
        print(json.dumps({"run_id": run.run_id, **result}, indent=2, default=str))


def _default_lead() -> dict:
    return {
        "lead_id": "L-100",
        "budget": 650000,
        "target_zip": "75205",
        "household_income": 180000,
        "preapproved": True,
        "intent_days": 21,
        "min_beds": 3,
        "down_payment": 120000,
        "interest_rate": 0.0675,
    }


def run_phase_cmd(phase: int, config_path: str, deal_id: str, lead_json: str | None, offer_text: str) -> None:
    spark = get_spark(f"realbricks-phase-{phase}")
    settings = get_settings()
    tracker = RunTracker(spark, settings)
    with tracker.track(f"phase_{phase}") as run:
        lead = json.loads(lead_json) if lead_json else _default_lead()

        if phase == 1:
            result = run_phase_1(spark, settings, config_path, deal_id, lead, run_id=run.run_id)
        elif phase == 2:
            result = run_phase_2(spark, settings, config_path, deal_id, lead, offer_text=offer_text, run_id=run.run_id)
        elif phase == 3:
            result = run_phase_3(spark, settings, config_path, deal_id, lead, run_id=run.run_id)
        elif phase == 4:
            deals = [
                {"deal_id": f"{deal_id}-A", "lead_payload": lead},
                {"deal_id": f"{deal_id}-B", "lead_payload": {**lead, "lead_id": "L-101", "budget": 500000, "preapproved": False}},
                {"deal_id": f"{deal_id}-C", "lead_payload": {**lead, "lead_id": "L-102", "target_zip": "75214", "interest_rate": 0.0725}},
            ]
            result = run_phase_4(spark, settings, config_path, deals=deals, run_id=run.run_id)
        else:
            raise ValueError(f"Unsupported phase: {phase}")

        print(json.dumps({"run_id": run.run_id, **result}, indent=2, default=str))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RealBricks multi-phase pipeline runner")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_ingest = sub.add_parser("ingest", help="Load bronze tables from configured marketplace sources")
    p_ingest.add_argument("--config", default="conf/pipeline_config.json")

    p_models = sub.add_parser("models", help="Build silver/gold canonical models")
    p_models.add_argument("--config", default="conf/pipeline_config.json")

    p_deal = sub.add_parser("deal", help="Run one lead-to-offer supervisor workflow")
    p_deal.add_argument("--deal-id", required=True)
    p_deal.add_argument("--lead-json", required=True)

    p_phase = sub.add_parser("phase", help="Run end-to-end phase workflow (1-4)")
    p_phase.add_argument("--phase", type=int, choices=[1, 2, 3, 4], required=True)
    p_phase.add_argument("--config", default="conf/pipeline_config.json")
    p_phase.add_argument("--deal-id", default="demo-phase")
    p_phase.add_argument("--lead-json")
    p_phase.add_argument("--offer-text", default="")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.cmd == "ingest":
            run_ingest_cmd(args.config)
        elif args.cmd == "models":
            run_models_cmd(args.config)
        elif args.cmd == "deal":
            run_deal_cmd(args.deal_id, args.lead_json)
        elif args.cmd == "phase":
            run_phase_cmd(args.phase, args.config, args.deal_id, args.lead_json, args.offer_text)
        else:
            raise ValueError(f"Unsupported command: {args.cmd}")
    except Exception as exc:
        print(json.dumps({"status": "error", "message": str(exc)}, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()

