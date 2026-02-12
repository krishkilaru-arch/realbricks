import unittest

from realbricks.agents.document_offer import DocumentOfferAgent
from realbricks.agents.lead_qualification import LeadQualificationAgent
from realbricks.agents.mortgage_financing import MortgageFinancingAgent


class TestLeadQualificationAgent(unittest.TestCase):
    def test_ready_lead_scores_hot(self) -> None:
        agent = LeadQualificationAgent()
        result = agent.evaluate(
            {
                "lead_id": "L1",
                "budget": 600000,
                "target_zip": "75205",
                "household_income": 200000,
                "preapproved": True,
                "intent_days": 14,
            }
        )
        self.assertEqual(result["band"], "hot")
        self.assertTrue(result["is_ready_for_matching"])


class TestMortgageFinancingAgent(unittest.TestCase):
    def test_affordability_output_shape(self) -> None:
        agent = MortgageFinancingAgent()
        result = agent.assess(
            {
                "household_income": 240000,
                "down_payment": 100000,
                "interest_rate": 0.065,
                "loan_term_months": 360,
                "preapproved": True,
            },
            {"listing_id": "P1", "list_price": 550000},
        )
        self.assertIn("estimated_monthly_payment", result)
        self.assertIn("affordable", result)
        self.assertIn(result["risk_level"], {"low", "medium", "high"})


class TestDocumentOfferAgent(unittest.TestCase):
    def test_extract_offer_features(self) -> None:
        agent = DocumentOfferAgent()
        out = agent.extract("Inspection contingency. Closing in 10 days. Down payment 8%. As-is sale.")
        self.assertIn("inspection", out.contingencies)
        self.assertEqual(out.closing_days, 10)
        self.assertEqual(out.down_payment_percent, 8.0)
        self.assertGreaterEqual(len(out.risk_flags), 1)


if __name__ == "__main__":
    unittest.main()

