from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractionResult:
    contingencies: list[str]
    closing_days: int | None
    down_payment_percent: float | None
    risk_flags: list[str]


class DocumentOfferAgent:
    """Simple rules-based extraction for PoC phase 2.

    This intentionally avoids external OCR dependencies and assumes extracted text
    input from upstream document processing.
    """

    _closing_re = re.compile(r"closing\s*(?:in|within)?\s*(\d{1,3})\s*days", re.IGNORECASE)
    _down_re = re.compile(r"down\s*payment\s*[:\-]?\s*(\d{1,3}(?:\.\d+)?)\s*%", re.IGNORECASE)

    def extract(self, offer_text: str) -> ExtractionResult:
        text = offer_text or ""
        lowered = text.lower()

        contingencies = []
        for keyword in ("inspection", "financing", "appraisal", "sale of existing home"):
            if keyword in lowered:
                contingencies.append(keyword)

        closing_days = None
        m = self._closing_re.search(text)
        if m:
            closing_days = int(m.group(1))

        down_payment_percent = None
        m2 = self._down_re.search(text)
        if m2:
            down_payment_percent = float(m2.group(1))

        risk_flags = []
        if closing_days is not None and closing_days < 14:
            risk_flags.append("Aggressive closing timeline.")
        if down_payment_percent is not None and down_payment_percent < 10:
            risk_flags.append("Low down payment may increase financing risk.")
        if "as-is" in lowered or "as is" in lowered:
            risk_flags.append("As-is clause present.")

        return ExtractionResult(
            contingencies=contingencies,
            closing_days=closing_days,
            down_payment_percent=down_payment_percent,
            risk_flags=risk_flags,
        )

