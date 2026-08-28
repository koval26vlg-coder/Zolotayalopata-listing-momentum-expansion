from __future__ import annotations

import sys
import json
import unittest
from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from listing_spot_asset_class import (  # noqa: E402
    ASSET_CLASS_CRYPTO_TOKEN,
    ASSET_CLASS_TOKENIZED_EQUITY,
    ASSET_CLASS_UNCLASSIFIED,
    DECLARED_CRYPTO_TOKEN_BASES,
    DECLARED_TOKENIZED_EQUITY_BASES,
    classify_spot_asset,
)


BITGET_REALITY_STOCK_BASES = frozenset(
    {
        "RAA",
        "RBURL",
        "RDJT",
        "RDKS",
        "REFX",
        "REROC",
        "RH",
        "RLNT",
        "RLSCC",
        "RNXT",
        "RPURR",
        "RSEZL",
        "RTTMI",
        "RULTA",
        "RUSFD",
    }
)

REVIEW_PATH = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "agent-log"
    / "bitget-reality-identity-review-20260828.json"
)


class BitgetRealityIdentityTests(unittest.TestCase):
    """Reviewed Reality stock identities must never enter crypto acceptance."""

    def test_reviewed_reality_bases_are_declared_tokenized_equities(self) -> None:
        declared = DECLARED_TOKENIZED_EQUITY_BASES.get("bitget", frozenset())
        self.assertTrue(BITGET_REALITY_STOCK_BASES <= declared)

        for base in BITGET_REALITY_STOCK_BASES:
            with self.subTest(base=base):
                result = classify_spot_asset("bitget", base)
                self.assertEqual(ASSET_CLASS_TOKENIZED_EQUITY, result.asset_class)
                self.assertFalse(result.acceptance_eligible)

    def test_declaration_matches_the_checked_in_official_metadata_review(self) -> None:
        review = json.loads(REVIEW_PATH.read_text(encoding="utf-8"))
        reviewed = {
            row["base"]
            for row in review["reviewed_rows"]
            if row["isReality"] == "yes" and row["symbolType"] == "stock"
        }
        self.assertEqual(BITGET_REALITY_STOCK_BASES, reviewed)
        self.assertEqual(
            BITGET_REALITY_STOCK_BASES,
            frozenset(review["review_decision"]["declare_tokenized_equity_bases"]),
        )

    def test_tokenized_and_crypto_declarations_are_disjoint(self) -> None:
        equities = DECLARED_TOKENIZED_EQUITY_BASES.get("bitget", frozenset())
        crypto = DECLARED_CRYPTO_TOKEN_BASES.get("bitget", frozenset())
        self.assertEqual(frozenset(), equities & crypto)

    def test_existing_reviewed_crypto_bases_remain_crypto_eligible(self) -> None:
        for base in ("ALIGN", "DGAI", "PWT", "SWARM"):
            with self.subTest(base=base):
                result = classify_spot_asset("BITGET", base.lower())
                self.assertEqual(ASSET_CLASS_CRYPTO_TOKEN, result.asset_class)
                self.assertTrue(result.acceptance_eligible)

    def test_unreviewed_r_prefixed_symbol_remains_fail_closed(self) -> None:
        result = classify_spot_asset("bitget", "RARE")
        self.assertEqual(ASSET_CLASS_UNCLASSIFIED, result.asset_class)
        self.assertFalse(result.acceptance_eligible)


if __name__ == "__main__":
    unittest.main()
