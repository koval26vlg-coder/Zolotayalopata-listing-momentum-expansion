"""Fail-closed asset classification for listing-momentum spot instruments.

Exchange instrument endpoints describe market mechanics, not whether an underlying is
a crypto token, a tokenised share, or something else. In particular, ``instType=SPOT``
on OKX is true for both ordinary crypto and tokenised-equity products. Unknown symbols
therefore never default to crypto.
"""

from __future__ import annotations

from dataclasses import dataclass


ASSET_CLASS_CRYPTO_TOKEN = "crypto_token"
ASSET_CLASS_TOKENIZED_EQUITY = "tokenized_equity"
ASSET_CLASS_UNCLASSIFIED = "unclassified"
DECLARATION_SOURCE = "declared_spot_asset_registry_v1"


# Exact OKX bases already observed in the expansion state. This is deliberately an
# explicit identity set, not an ``X``-prefix heuristic which could misclassify a token.
DECLARED_TOKENIZED_EQUITY_BASES: dict[str, frozenset[str]] = {
    "okx": frozenset(
        {
            "XAPLD", "XBOT", "XBX", "XCIEN", "XCRM", "XDKNG", "XGLW",
            "XHPE", "XISRG", "XJNJ", "XKLAC", "XKO", "XLRCX", "XNOW",
            "XOKTA", "XPOPMART", "XQCOM", "XRDDT", "XRIVN", "XROK",
            "XSMH", "XSNOW", "XSTRC", "XTTWO", "XUNH", "XWDC",
            "XXIAOMI", "XZM",
        }
    )
}

# Ticker spelling alone is not evidence that an instrument belongs to the crypto
# acceptance universe, so every entry here is a reviewed identity rather than a guess.
#
# The Bitget four were established on 2026-08-26 under probe plan
# listing_spot_crypto_identity_probe_20260826_v2: Bitget publishes both deposit and
# withdrawal for each on a public network with a contract address - ALIGN on ERC20,
# DGAI, PWT and SWARM on BEP20. An instrument that can be moved onto a public chain
# and back is not an internal exchange ledger entry, which is what a tokenised share
# on these venues is. The response bytes each verdict rests on are hashed in
# docs/agent-log/listing-spot-crypto-identity-probe-result-20260826.json.
#
# TMX is deliberately absent. Bitget publishes deposit for it and not withdrawal, and
# one-way movement does not establish the identity: a suspended withdrawal looks the
# same here as an asset that structurally cannot leave.
DECLARED_CRYPTO_TOKEN_BASES: dict[str, frozenset[str]] = {
    "bitget": frozenset({"ALIGN", "DGAI", "PWT", "SWARM"}),
}


@dataclass(frozen=True)
class SpotAssetClassification:
    asset_class: str
    source: str
    acceptance_eligible: bool


def classify_spot_asset(exchange: str, base: str) -> SpotAssetClassification:
    venue = str(exchange or "").strip().lower()
    underlying = str(base or "").strip().upper()
    if underlying and underlying in DECLARED_TOKENIZED_EQUITY_BASES.get(venue, frozenset()):
        return SpotAssetClassification(
            ASSET_CLASS_TOKENIZED_EQUITY, DECLARATION_SOURCE, False
        )
    if underlying and underlying in DECLARED_CRYPTO_TOKEN_BASES.get(venue, frozenset()):
        return SpotAssetClassification(
            ASSET_CLASS_CRYPTO_TOKEN, DECLARATION_SOURCE, True
        )
    return SpotAssetClassification(
        ASSET_CLASS_UNCLASSIFIED, "unclassified_no_positive_identity", False
    )
