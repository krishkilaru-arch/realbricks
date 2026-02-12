from __future__ import annotations

import json
from typing import Any

from pyspark.sql import SparkSession

from realbricks.agents.lead_qualification import LeadQualificationAgent
from realbricks.agents.mortgage_financing import MortgageFinancingAgent
from realbricks.agents.property_matching import PropertyMatchingAgent
from realbricks.settings import Settings
from realbricks.state_store import DeltaStateStore, LakebaseStateStore, StateStore


class SupervisorAgent:
    def __init__(self, spark: SparkSession, settings: Settings):
        self.spark = spark
        self.settings = settings
        self.lead_agent = LeadQualificationAgent()
        self.match_agent = PropertyMatchingAgent()
        self.mortgage_agent = MortgageFinancingAgent()
        self.state_store = self._build_state_store()

    def _build_state_store(self) -> StateStore:
        backend = self.settings.state_backend.lower()
        if backend == "lakebase":
            if not self.settings.lakebase_dsn:
                raise RuntimeError("RB_LAKEBASE_DSN is required when RB_STATE_BACKEND=lakebase")
            return LakebaseStateStore(
                dsn=self.settings.lakebase_dsn,
                table_name=self.settings.lakebase_table,
            )
        return DeltaStateStore(self.spark, self.settings.deal_state_table)

    def _checkpoint(self, deal_id: str, stage: str, payload: dict[str, Any]) -> None:
        self.state_store.write_event(
            deal_id=deal_id,
            stage=stage,
            payload=json.loads(json.dumps(payload, default=str)),
        )

    def record_event(self, deal_id: str, stage: str, payload: dict[str, Any]) -> None:
        self._checkpoint(deal_id, stage, payload)

    def run(self, deal_id: str, lead_payload: dict[str, Any]) -> dict[str, Any]:
        try:
            lead_result = self.lead_agent.evaluate(lead_payload)
            self._checkpoint(deal_id, "lead_qualification", lead_result)

            if not lead_result["is_ready_for_matching"]:
                return {"deal_id": deal_id, "status": "blocked", "lead": lead_result}

            matches = self.match_agent.match_top_properties(
                spark=self.spark,
                listing_table=self.settings.silver_listing_table,
                lead=lead_payload,
                top_n=3,
            )
            match_result = {"matches": matches}
            self._checkpoint(deal_id, "property_matching", match_result)

            selected = matches[0] if matches else None
            financing = self.mortgage_agent.assess(lead_payload, selected)
            self._checkpoint(deal_id, "mortgage_financing", financing)

            final = {
                "deal_id": deal_id,
                "status": "ready_for_offer" if financing.get("affordable") else "needs_review",
                "lead": lead_result,
                "top_matches": matches,
                "financing": financing,
            }
            self._checkpoint(deal_id, "supervisor_final", final)
            return final
        except Exception as exc:
            error_payload = {
                "deal_id": deal_id,
                "status": "error",
                "message": str(exc),
            }
            self._checkpoint(deal_id, "supervisor_error", error_payload)
            raise

