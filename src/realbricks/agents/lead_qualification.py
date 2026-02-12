from __future__ import annotations


REQUIRED_FIELDS = ("lead_id", "budget", "target_zip", "household_income")


class LeadQualificationAgent:
    def evaluate(self, lead: dict) -> dict:
        missing = [field for field in REQUIRED_FIELDS if lead.get(field) in (None, "")]

        score = 0.0
        if lead.get("preapproved"):
            score += 40
        if lead.get("household_income", 0) and lead.get("budget", 0):
            ratio = float(lead["budget"]) / max(float(lead["household_income"]), 1.0)
            if ratio <= 4:
                score += 25
            elif ratio <= 6:
                score += 15

        if lead.get("intent_days") is not None:
            if lead["intent_days"] <= 30:
                score += 25
            elif lead["intent_days"] <= 90:
                score += 15

        score = max(0.0, min(100.0, score))
        band = "hot" if score >= 70 else "warm" if score >= 45 else "cold"

        return {
            "lead_id": lead.get("lead_id"),
            "score": round(score, 2),
            "band": band,
            "missing_fields": missing,
            "is_ready_for_matching": len(missing) == 0 and score >= 45,
        }

