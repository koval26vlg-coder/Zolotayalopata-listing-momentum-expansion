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
    # Bitget's official UTA instrument metadata reports these pairs with
    # ``isReality=yes`` and ``symbolType=stock``. Reality is Bitget's tokenized-US-
    # stock product; on-chain deposit/withdrawal is therefore not crypto-underlying
    # evidence. The reviewed response metadata and raw response hash are retained in
    # docs/agent-log/bitget-reality-identity-review-20260828.json.
    "bitget": frozenset(
        {
            "RAA", "RBURL", "RDJT", "RDKS", "REFX", "REROC", "RH",
            "RLNT", "RLSCC", "RNXT", "RPURR", "RSEZL", "RTTMI",
            "RULTA", "RUSFD",
        }
    ),
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
# listing_spot_crypto_identity_probe_20260826_v2. The venue's 2026-08-28 UTA
# instrument metadata independently reports ``isReality=no`` and
# ``symbolType=crypto`` for each. Deposit/withdrawal proves token mechanics, not the
# nature of the underlying: Reality stock tokens are movable too. The reviewed
# identity rows and source-response hash are retained in
# docs/agent-log/bitget-reality-identity-review-20260828.json.
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
