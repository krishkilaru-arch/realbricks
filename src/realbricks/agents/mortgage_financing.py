from __future__ import annotations


class MortgageFinancingAgent:
    def assess(self, lead: dict, property_match: dict | None) -> dict:
        if not property_match:
            return {
                "affordable": False,
                "risk_level": "high",
                "notes": ["No property selected for financing analysis."],
            }

        price = float(property_match.get("list_price") or 0.0)
        down_payment = float(lead.get("down_payment", 0.0))
        income = float(lead.get("household_income", 0.0))
        annual_rate = float(lead.get("interest_rate", 0.07))

        principal = max(price - down_payment, 0.0)
        monthly_income = income / 12.0 if income > 0 else 0.0
        monthly_rate = annual_rate / 12.0
        term_months = int(lead.get("loan_term_months", 360))

        if principal == 0:
            est_payment = 0.0
        elif monthly_rate == 0:
            est_payment = principal / term_months
        else:
            est_payment = (principal * monthly_rate) / (1 - ((1 + monthly_rate) ** (-term_months)))

        dti_like = est_payment / monthly_income if monthly_income else 1.0
        affordable = dti_like <= 0.35
        risk_level = "low" if dti_like <= 0.28 else "medium" if dti_like <= 0.40 else "high"

        notes = []
        if not lead.get("preapproved"):
            notes.append("Lead is not pre-approved.")
        if dti_like > 0.4:
            notes.append("Estimated payment appears high relative to household income.")

        return {
            "property_id": property_match.get("listing_id"),
            "estimated_monthly_payment": round(est_payment, 2),
            "payment_to_income_ratio": round(dti_like, 3),
            "affordable": affordable,
            "risk_level": risk_level,
            "notes": notes,
        }

