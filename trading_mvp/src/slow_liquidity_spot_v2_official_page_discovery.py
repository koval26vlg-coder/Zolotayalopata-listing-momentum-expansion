from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Mapping

from slow_liquidity_official_identity_proposal import (
    COLLISION_FAIL_CLOSED_BASES,
    EXPECTED_BASES,
    EXPECTED_VENUES,
    GATE_METADATA_ENDPOINT,
    MEXC_METADATA_ENDPOINT,
    PROPOSAL_ID,
    collected_spot_instrument,
)
from slow_liquidity_official_identity_verification import (
    FetchedResponse,
    IdentityVerificationError,
    _strict_json_loads,
    _validate_official_source_url,
    _validate_request_plan_item,
)
from slow_liquidity_spot_v2_request_plan import (
    BINDINGS_PATH,
    PLAN_ID as BINDINGS_PLAN_ID,
    SPOT_V2_PROPOSAL_FILE_SHA256,
    SPOT_V2_PROPOSAL_HASH,
    SPOT_V2_RUNTIME_FILE_SHA256,
    SPOT_V2_RUNTIME_HASH,
    SPOT_V2_RUNTIME_PATH,
    build_spot_v2_request_plan_bindings,
    validate_spot_v2_request_plan_bindings,
)


SCHEMA = "trading_mvp_slow_liquidity_spot_v2_official_page_discovery_planonly_v1"
PLAN_ID = "slow_liquidity_spot_v2_official_page_discovery_20260815"
HASH_METHOD = "sha256_canonical_json_excluding_plan_hash"
REPO_ROOT = Path(__file__).resolve().parents[2]
DISCOVERY_PLAN_PATH = (
    REPO_ROOT
    / "docs/plans/slow-liquidity-spot-v2-official-page-discovery-planonly-20260815.json"
)
BINDINGS_PLAN_HASH = (
    "6f3eb75bd23303ee8c5123dbcdb53c70000ae414629f8734d970fe5e111b0d07"
)
BINDINGS_FILE_SHA256 = (
    "bf68d7f87dad8495145a1e35e1ceaa319387db43f67fffaa648d59dead9b4061"
)
EVIDENCE_HOSTS = {
    "mexc": ("www.mexc.com", "/support/articles/"),
    "gateio": ("www.gate.com", "/announcements/article/"),
}
SEED_FIELDS = {
    "venue",
    "base_ticker",
    "instrument_id",
    "collision_fail_closed",
    "search_url",
    "navigation_query",
    "expected_official_host",
    "allowed_official_path_prefix",
}


class SpotV2OfficialPageDiscoveryError(ValueError):
    pass


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_hash(payload: Mapping[str, Any]) -> str:
    normalized = copy.deepcopy(dict(payload))
    normalized.pop("plan_hash", None)
    return hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()


def _require(value: bool, message: str) -> None:
    if not value:
        raise SpotV2OfficialPageDiscoveryError(message)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _official_host(venue: str) -> tuple[str, str]:
    return EVIDENCE_HOSTS[venue]


def _navigation_query(venue: str, base: str, instrument_id: str) -> str:
    host, prefix = _official_host(venue)
    return (
        f'site:{host}{prefix} "{base}" "{instrument_id}" '
        '"Contract Address"'
    )


def _navigation_url(query: str) -> str:
    return "https://www.bing.com/search?" + urllib.parse.urlencode(
        (("format", "rss"), ("q", query))
    )


def _seed_items() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for base in EXPECTED_BASES:
        for venue in EXPECTED_VENUES:
            instrument_id = collected_spot_instrument(venue, base)
            host, prefix = _official_host(venue)
            query = _navigation_query(venue, base, instrument_id)
            items.append(
                {
                    "venue": venue,
                    "base_ticker": base,
                    "instrument_id": instrument_id,
                    "collision_fail_closed": base in COLLISION_FAIL_CLOSED_BASES,
                    "search_url": _navigation_url(query),
                    "navigation_query": query,
                    "expected_official_host": host,
                    "allowed_official_path_prefix": prefix,
                }
            )
    return items


def _approval_phrase_template() -> str:
    return (
        "Разрешаю один видимый public read-only запуск "
        f"{PLAN_ID} через "
        "tools\\start_exact_approved_slow_liquidity_spot_v2_official_page_"
        "discovery_visible.ps1 по plan_hash=<PLAN_HASH> и "
        "plan_file_sha256=<PLAN_FILE_SHA256>: MEXC и Gate SPOT_USDT, "
        "18 пар из frozen instrument bindings, MEXC BASEUSDT / Gate BASE_USDT, "
        "EDGE и RAIN fail-closed. Bing navigation только для поиска страниц. "
        "Официальные страницы — не identity verdict. Не v7 и не MEXC perp "
        "underscore ticker. STOPPED_INCOMPLETE не повторять. Без evaluator, OOS, "
        "returns/PnL, grid/retune, execution probe, paper/live, private API, "
        "реальных денег, плеча или маржи."
    )


def build_spot_v2_official_page_discovery_plan(
    generated_at_utc: str,
) -> dict[str, Any]:
    if BINDINGS_PATH.is_file():
        bindings = json.loads(BINDINGS_PATH.read_text(encoding="utf-8"))
        validate_spot_v2_request_plan_bindings(bindings)
        _require(bindings.get("plan_hash") == BINDINGS_PLAN_HASH, "bindings hash mismatch")
        _require(
            _sha256_file(BINDINGS_PATH) == BINDINGS_FILE_SHA256,
            "bindings file hash mismatch",
        )
    else:
        bindings = build_spot_v2_request_plan_bindings(generated_at_utc)
    plan: dict[str, Any] = {
        "schema": SCHEMA,
        "plan_id": PLAN_ID,
        "mode": "PlanOnly",
        "status": "AWAIT_EXACT_HASH_BOUND_DISCOVERY_APPROVAL",
        "generated_at_utc": generated_at_utc,
        "research_only": True,
        "identity_evidence": False,
        "network_authorized": False,
        "execution_authorized": False,
        "consumer_runtime": PROPOSAL_ID,
        "market": "SPOT_USDT",
        "required_pair_count": 18,
        "collision_fail_closed_bases": list(COLLISION_FAIL_CLOSED_BASES),
        "collision_ambiguity_disposition": "REJECT_EXCLUDE_FAIL_CLOSED",
        "goal": (
            "Discover official public pages for the frozen 18 collected spot "
            "instruments. This is not an identity verdict."
        ),
        "source_bindings": {
            "instrument_bindings": {
                "path": str(BINDINGS_PATH),
                "plan_id": BINDINGS_PLAN_ID,
                "file_sha256": BINDINGS_FILE_SHA256,
                "plan_hash": BINDINGS_PLAN_HASH,
            },
            "spot_v2_proposal": {
                "proposal_hash": SPOT_V2_PROPOSAL_HASH,
                "file_sha256": SPOT_V2_PROPOSAL_FILE_SHA256,
            },
            "spot_v2_runtime": {
                "path": str(SPOT_V2_RUNTIME_PATH),
                "file_sha256": SPOT_V2_RUNTIME_FILE_SHA256,
                "manifest_hash": SPOT_V2_RUNTIME_HASH,
            },
        },
        "compatibility_contract": {
            "consumer_runtime": PROPOSAL_ID,
            "required_pair_count": 18,
            "instrument_rule": "collected_spot_instrument(venue, base)",
            "perp_template_forbidden": True,
            "v7_runtime_forbidden": True,
            "request_plan_items_must_pass_spot_v2_consumer": True,
        },
        "navigation_contract": {
            "provider": "BING_RSS",
            "scheme": "https",
            "host": "www.bing.com",
            "path": "/search",
            "role": "NAVIGATION_ONLY_NOT_IDENTITY_EVIDENCE",
            "search_result_title_is_identity_evidence": False,
            "search_result_snippet_is_identity_evidence": False,
            "search_result_persistence_allowed": False,
            "redirect_following_allowed": False,
        },
        "official_source_contract": {
            "metadata_endpoints": {
                "mexc": MEXC_METADATA_ENDPOINT,
                "gateio": GATE_METADATA_ENDPOINT,
            },
            "evidence_hosts": {
                venue: {
                    "host": host,
                    "allowed_path_prefix": prefix,
                }
                for venue, (host, prefix) in EVIDENCE_HOSTS.items()
            },
            "only_allowlisted_official_page_content_is_identity_evidence": True,
            "ticker_or_name_match_alone_is_identity_evidence": False,
            "exact_active_spot_metadata_required": True,
            "exact_unique_canonical_identifier_required": True,
        },
        "limits": {
            "maximum_total_http_requests": 40,
            "maximum_attempts_per_url": 2,
            "maximum_response_bytes_per_request": 1_000_000,
            "max_runtime_sec": 600,
            "hard_output_cap_bytes": 20_000_000,
        },
        "seed_items": _seed_items(),
        "approval_request": {
            "exact_user_text_template": _approval_phrase_template(),
            "text_normalization": (
                "normalize CRLF/CR to LF, then trim outer whitespace; "
                "all internal text must match exactly"
            ),
        },
        "authorized_scope_after_exact_approval": {
            "one_visible_public_read_only_official_page_discovery": True,
            "official_source_content_read": True,
            "request_plan_output": True,
            "identity_verdict": False,
            "evaluator_or_oos": False,
            "returns_or_pnl": False,
            "grid_or_retune": False,
            "execution_probe": False,
            "paper_or_live": False,
        },
        "authorization_now": {
            "plan_freeze_allowed": True,
            "actual_network_run_allowed": False,
            "official_source_content_read_allowed": False,
            "identity_verdict_allowed": False,
            "exact_user_approval_required": True,
        },
        "plan_hash_method": HASH_METHOD,
    }
    del bindings
    plan["plan_hash"] = canonical_hash(plan)
    validate_spot_v2_official_page_discovery_plan(plan)
    return plan


def validate_spot_v2_official_page_discovery_plan(plan: Mapping[str, Any]) -> None:
    _require(plan.get("schema") == SCHEMA, "discovery plan schema mismatch")
    _require(plan.get("plan_id") == PLAN_ID, "discovery plan id mismatch")
    _require(plan.get("mode") == "PlanOnly", "discovery plan mode mismatch")
    _require(
        plan.get("status") == "AWAIT_EXACT_HASH_BOUND_DISCOVERY_APPROVAL",
        "discovery plan status mismatch",
    )
    _require(plan.get("identity_evidence") is False, "discovery plan claimed identity evidence")
    _require(plan.get("network_authorized") is False, "discovery plan authorized network")
    _require(plan.get("execution_authorized") is False, "discovery plan authorized execution")
    _require(plan.get("market") == "SPOT_USDT", "discovery plan market mismatch")
    _require(plan.get("consumer_runtime") == PROPOSAL_ID, "consumer runtime mismatch")
    _require(plan.get("plan_hash") == canonical_hash(plan), "discovery plan hash mismatch")
    dumped = json.dumps(plan, ensure_ascii=False)
    _require("20260815-v7" not in dumped, "v7 runtime leaked into discovery plan")
    _require("{BASE}_USDT" not in dumped, "perp instrument template leaked")
    _require("contract.mexc.com" not in dumped, "perp metadata endpoint leaked")
    _require("/futures/usdt/contracts" not in dumped, "perp futures endpoint leaked")
    endpoints = (plan.get("official_source_contract") or {}).get("metadata_endpoints")
    _require(
        endpoints
        == {
            "mexc": MEXC_METADATA_ENDPOINT,
            "gateio": GATE_METADATA_ENDPOINT,
        },
        "spot metadata endpoints mismatch",
    )
    seeds = plan.get("seed_items")
    _require(isinstance(seeds, list) and len(seeds) == 18, "seed item count mismatch")
    seen: set[tuple[str, str]] = set()
    for item in seeds:
        _require(isinstance(item, dict), "seed item is invalid")
        _require(set(item) == SEED_FIELDS, "seed item field set changed")
        venue = str(item.get("venue"))
        base = str(item.get("base_ticker"))
        expected = collected_spot_instrument(venue, base)
        _require(item.get("instrument_id") == expected, "collected spot instrument mismatch")
        _require(expected in str(item.get("navigation_query") or ""), "query missing instrument")
        if venue == "mexc":
            _require(
                f"{base}_USDT" not in str(item.get("navigation_query") or ""),
                "mexc query used perp ticker",
            )
        _require(
            item.get("collision_fail_closed") is (base in COLLISION_FAIL_CLOSED_BASES),
            "collision fail-closed flag mismatch",
        )
        pair = (venue, base)
        _require(pair not in seen, "duplicate seed pair")
        seen.add(pair)
    _require(
        seen == {(venue, base) for venue in EXPECTED_VENUES for base in EXPECTED_BASES},
        "seed universe mismatch",
    )
    sources = plan.get("source_bindings") or {}
    bindings = sources.get("instrument_bindings") or {}
    _require(bindings.get("plan_hash") == BINDINGS_PLAN_HASH, "instrument bindings hash mismatch")
    _require(
        bindings.get("file_sha256") == BINDINGS_FILE_SHA256,
        "instrument bindings file hash mismatch",
    )
    runtime = sources.get("spot_v2_runtime") or {}
    _require(runtime.get("manifest_hash") == SPOT_V2_RUNTIME_HASH, "spot v2 runtime hash mismatch")
    authorization = plan.get("authorization_now") or {}
    _require(authorization.get("actual_network_run_allowed") is False, "network already allowed")
    _require(authorization.get("identity_verdict_allowed") is False, "identity verdict already allowed")


def write_spot_v2_official_page_discovery_plan(generated_at_utc: str) -> Path:
    plan = build_spot_v2_official_page_discovery_plan(generated_at_utc)
    payload = json.dumps(plan, indent=2, ensure_ascii=False) + "\n"
    if DISCOVERY_PLAN_PATH.exists():
        _require(
            DISCOVERY_PLAN_PATH.read_text(encoding="utf-8") == payload,
            f"immutable artifact mismatch: {DISCOVERY_PLAN_PATH}",
        )
        return DISCOVERY_PLAN_PATH
    DISCOVERY_PLAN_PATH.parent.mkdir(parents=True, exist_ok=True)
    DISCOVERY_PLAN_PATH.write_text(payload, encoding="utf-8")
    return DISCOVERY_PLAN_PATH


EXPECTED_APPROVAL_TEXT = (
    "Разрешаю один видимый public read-only запуск "
    "slow_liquidity_spot_v2_official_page_discovery_20260815 через "
    "tools\\start_exact_approved_slow_liquidity_spot_v2_official_page_"
    "discovery_visible.ps1 по "
    "plan_hash=becfd2d04871b435614f8a0785ac9e6f90c79cc3537b9868745508bc73e45d20 "
    "и plan_file_sha256=10d6cc6407915c49969711afc013e5865d3179b03a2166fb647c88abfd3b4360: "
    "MEXC и Gate SPOT_USDT, 18 пар из frozen instrument bindings, "
    "MEXC BASEUSDT / Gate BASE_USDT, EDGE и RAIN fail-closed. Bing navigation "
    "только для поиска страниц. Официальные страницы — не identity verdict. "
    "Не v7 и не MEXC perp underscore ticker. STOPPED_INCOMPLETE не повторять. "
    "Без evaluator, OOS, returns/PnL, grid/retune, execution probe, paper/live, "
    "private API, реальных денег, плеча или маржи."
)
OUTPUT_ROOT = Path(
    r"E:\ZolotyayLopata-data\exports\trading-mvp\slow-liquidity-spot-v2-official-page-discovery"
    r"\slow_liquidity_spot_v2_official_page_discovery_20260815"
)
APPROVAL_RECEIPT_PATH = (
    REPO_ROOT
    / "docs/agent-log/approvals/"
    "2026-08-15-slow-liquidity-spot-v2-official-page-discovery-approval.json"
)
CONTRACT_ADDRESS_PATTERN = re.compile(r"contract[\s_.:-]*address", re.IGNORECASE)
EVM_IDENTIFIER_PATTERN = re.compile(
    r"(?<![0-9A-Fa-f])0[xX][0-9a-fA-F]{40}(?![0-9A-Fa-f])"
)
HREF_PATTERN = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.IGNORECASE)
ALLOWED_FETCH_HOSTS = {
    "www.bing.com",
    "api.mexc.com",
    "api.gateio.ws",
    "www.mexc.com",
    "www.gate.com",
}


class _VisibleTextExtractor(HTMLParser):
    _HIDDEN_TAGS = {"script", "style", "template", "noscript"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._hidden_depth = 0
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag.lower() in self._HIDDEN_TAGS:
            self._hidden_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in self._HIDDEN_TAGS and self._hidden_depth > 0:
            self._hidden_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._hidden_depth == 0 and data:
            self._parts.append(data)

    def text(self) -> str:
        return " ".join(self._parts)


@dataclass(frozen=True)
class SpotV2DiscoveryResult:
    status: str
    request_plan: tuple[dict[str, Any], ...]
    unresolved_pairs: tuple[str, ...]
    request_count: int
    identity_verdict: bool
    network_accessed: bool


def _exact_token(value: str, text: str) -> bool:
    return (
        re.search(rf"(?<![A-Za-z0-9]){re.escape(value)}(?![A-Za-z0-9])", text)
        is not None
    )


def _rss_official_candidates(
    venue: str, base: str, instrument_id: str, body: bytes
) -> tuple[str, ...]:
    _require(b"<!DOCTYPE" not in body.upper(), "navigation XML DTD is forbidden")
    _require(b"<!ENTITY" not in body.upper(), "navigation XML entity is forbidden")
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise SpotV2OfficialPageDiscoveryError(
            "navigation response is not valid RSS XML"
        ) from exc
    candidates: list[str] = []
    for node in root.findall(".//item"):
        title = node.findtext("title") or ""
        link = (node.findtext("link") or "").strip()
        if not _exact_token(base, title) or not _exact_token(instrument_id, title):
            continue
        try:
            _validate_official_source_url(venue, link)
        except IdentityVerificationError:
            continue
        candidates.append(link)
    return tuple(sorted(set(candidates)))


def _unwrap_navigation_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    host = (parsed.hostname or "").lower()
    if host not in {"www.bing.com", "bing.com"}:
        return url
    raw = (urllib.parse.parse_qs(parsed.query).get("u") or [""])[0]
    if not raw:
        return url
    if raw.startswith("a1"):
        raw = raw[2:]
    try:
        padded = raw + "=" * ((4 - len(raw) % 4) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return url
    if decoded.startswith("https://"):
        return decoded
    return url


def _html_official_candidates(
    venue: str, base: str, instrument_id: str, body: bytes
) -> tuple[str, ...]:
    del instrument_id
    text = body.decode("utf-8", "replace")
    candidates: list[str] = []
    for match in HREF_PATTERN.finditer(text):
        raw = urllib.parse.unquote(match.group(1).strip())
        url = _unwrap_navigation_url(raw)
        try:
            _validate_official_source_url(venue, url)
        except IdentityVerificationError:
            continue
        window = text[max(0, match.start() - 240) : match.end() + 240]
        if not _exact_token(base, window) and not _exact_token(base, url):
            continue
        candidates.append(url)
    return tuple(sorted(set(candidates)))


def _navigation_official_candidates(
    venue: str, base: str, instrument_id: str, body: bytes
) -> tuple[str, ...]:
    prefix = body.lstrip()[:64].lower()
    looks_like_feed = prefix.startswith(b"<?xml") or b"<rss" in prefix or b"<feed" in prefix
    if looks_like_feed:
        try:
            return _rss_official_candidates(venue, base, instrument_id, body)
        except SpotV2OfficialPageDiscoveryError:
            pass
    return _html_official_candidates(venue, base, instrument_id, body)


def _active_expected_spot_instruments(venue: str, payload: Any) -> tuple[str, ...]:
    expected = {
        collected_spot_instrument(venue, base): base for base in EXPECTED_BASES
    }
    found: set[str] = set()
    if venue == "mexc":
        _require(isinstance(payload, dict), "MEXC metadata response must be an object")
        rows = payload.get("symbols")
        _require(isinstance(rows, list), "MEXC metadata symbols must be an array")
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").upper()
            if symbol not in expected:
                continue
            if str(row.get("quoteAsset") or "").upper() != "USDT":
                continue
            if row.get("isSpotTradingAllowed") is False:
                continue
            if str(row.get("status") or "").upper() not in {"1", "ENABLED", "TRADING"}:
                continue
            base = str(row.get("baseAsset") or "").upper()
            if base != expected[symbol]:
                continue
            found.add(symbol)
        return tuple(sorted(found))
    _require(isinstance(payload, list), "Gate metadata response must be an array")
    for row in payload:
        if not isinstance(row, dict):
            continue
        name = str(row.get("id") or row.get("name") or "").upper()
        if name not in expected:
            continue
        if str(row.get("quote") or "").upper() != "USDT":
            continue
        if str(row.get("trade_status") or "").lower() != "tradable":
            continue
        base = str(row.get("base") or "").upper()
        if base and base != expected[name]:
            continue
        found.add(name)
    return tuple(sorted(found))


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Any,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        raise SpotV2OfficialPageDiscoveryError("HTTP redirect is forbidden")


def fetch_public_discovery_response(
    url: str, timeout_sec: float = 20.0
) -> FetchedResponse:
    parsed = urllib.parse.urlsplit(url)
    _require(parsed.scheme == "https", "only HTTPS is allowed")
    _require(
        (parsed.hostname or "").lower() in ALLOWED_FETCH_HOSTS,
        "fetch host is not allowlisted",
    )
    request = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": (
                "application/rss+xml, application/xml, text/xml, "
                "application/json, text/html;q=0.8, */*;q=0.1"
            ),
            "Accept-Encoding": "identity",
            "User-Agent": "trading-mvp-spot-v2-official-page-discovery/1.0",
        },
    )
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _NoRedirectHandler(),
    )
    try:
        with opener.open(request, timeout=timeout_sec) as response:
            body = response.read(1_000_001)
            final_url = response.geturl()
            status = int(response.status)
    except SpotV2OfficialPageDiscoveryError:
        raise
    except urllib.error.HTTPError as exc:
        if 300 <= exc.code < 400:
            raise SpotV2OfficialPageDiscoveryError("HTTP redirect is forbidden") from exc
        raise SpotV2OfficialPageDiscoveryError(f"HTTP {exc.code} for {url}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise SpotV2OfficialPageDiscoveryError("public read-only request failed") from exc
    return FetchedResponse(url, final_url, status, body)


def _canonical_identifier_from_official_page(
    base: str, instrument_id: str, body: bytes
) -> str:
    try:
        source = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SpotV2OfficialPageDiscoveryError(
            "official response is not valid UTF-8"
        ) from exc
    extractor = _VisibleTextExtractor()
    extractor.feed(source)
    extractor.close()
    text = re.sub(r"\s+", " ", extractor.text())
    labels = list(CONTRACT_ADDRESS_PATTERN.finditer(text))
    _require(bool(labels), "official page has no canonical identifier label")
    identifiers: set[str] = set()
    for label in labels:
        start = max(0, label.start() - 256)
        end = min(len(text), label.end() + 256)
        context = text[start:end]
        if not _exact_token(base, context) or not _exact_token(instrument_id, context):
            continue
        identifiers.update(EVM_IDENTIFIER_PATTERN.findall(context))
    _require(
        len({value.lower() for value in identifiers}) == 1,
        "official page has no unique canonical identifier bound to the exact instrument",
    )
    return sorted(identifiers, key=str.lower)[0]


def _request_plan_item(
    *, venue: str, base: str, official_url: str, identifier: str
) -> dict[str, str]:
    instrument_id = collected_spot_instrument(venue, base)
    assertion = json.dumps(
        {
            "base_ticker": base,
            "canonical_asset_identifier_label": "contract_address",
            "canonical_asset_identifier_value": identifier,
            "instrument_id": instrument_id,
            "venue": venue,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    item = {
        "venue": venue,
        "official_source_url": official_url,
        "instrument_id": instrument_id,
        "base_ticker": base,
        "canonical_asset_identifier_namespace": "EVM_CONTRACT",
        "canonical_asset_identifier_value": identifier,
        "canonical_asset_identifier_label": "contract_address",
        "evidence_locator_type": "CANONICAL_REQUIRED_EXACT_UTF8_TOKENS_V1",
        "evidence_locator_value": assertion,
        "sanitized_evidence_fragment": assertion,
    }
    try:
        _validate_request_plan_item(item)
    except IdentityVerificationError as exc:
        raise SpotV2OfficialPageDiscoveryError(
            "discovered request plan is incompatible"
        ) from exc
    return item


def normalize_approval_text(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n").strip()


def discover_spot_v2_official_pages(
    plan: Mapping[str, Any],
    *,
    user_approval_text: str,
    fetch: Callable[[str], FetchedResponse] = fetch_public_discovery_response,
    monotonic: Callable[[], float] = time.monotonic,
) -> SpotV2DiscoveryResult:
    validate_spot_v2_official_page_discovery_plan(plan)
    _require(
        normalize_approval_text(user_approval_text) == EXPECTED_APPROVAL_TEXT,
        "approval text mismatch",
    )
    _require(
        _sha256_file(DISCOVERY_PLAN_PATH)
        == "10d6cc6407915c49969711afc013e5865d3179b03a2166fb647c88abfd3b4360",
        "frozen discovery plan file hash mismatch",
    )
    started = monotonic()
    request_count = 0
    requests_by_url: dict[str, int] = {}

    def fetch_counted(url: str) -> bytes:
        nonlocal request_count
        _require(monotonic() - started <= 600, "discovery runtime cap exceeded")
        _require(request_count < 40, "discovery HTTP request cap exceeded")
        requests_by_url[url] = requests_by_url.get(url, 0) + 1
        _require(requests_by_url[url] <= 2, "attempt cap per URL exceeded")
        request_count += 1
        try:
            response = fetch(url)
        except IdentityVerificationError as exc:
            raise SpotV2OfficialPageDiscoveryError(str(exc)) from exc
        _require(isinstance(response, FetchedResponse), "invalid fetch response")
        _require(response.requested_url == url, "fetcher request URL mismatch")
        _require(response.final_url == url, "HTTP redirect is forbidden")
        _require(response.status == 200, f"HTTP {response.status} for {url}")
        _require(len(response.body) <= 1_000_000, "response exceeds cap")
        return response.body

    endpoints = plan["official_source_contract"]["metadata_endpoints"]
    active: dict[str, tuple[str, ...]] = {}
    for venue, url in endpoints.items():
        try:
            payload = _strict_json_loads(fetch_counted(url).decode("utf-8"))
            active[venue] = _active_expected_spot_instruments(venue, payload)
        except (
            SpotV2OfficialPageDiscoveryError,
            IdentityVerificationError,
            UnicodeDecodeError,
            ValueError,
        ):
            active[venue] = ()
        print(
            f"SPOT_V2_DISCOVERY_METADATA venue={venue} "
            f"active={len(active[venue])} requests={request_count}",
            flush=True,
        )

    request_plan: list[dict[str, Any]] = []
    unresolved: list[str] = []
    for item in plan["seed_items"]:
        venue = str(item["venue"])
        base = str(item["base_ticker"])
        instrument = str(item["instrument_id"])
        pair = f"{venue}:{base}"
        collision = bool(item["collision_fail_closed"])
        if instrument not in active.get(venue, ()):
            unresolved.append(
                f"{pair}:AMBIGUOUS_KNOWN_TICKER_COLLISION"
                if collision
                else f"{pair}:ACTIVE_SPOT_METADATA_MISSING"
            )
            continue
        try:
            navigation = fetch_counted(str(item["search_url"]))
            candidates = _navigation_official_candidates(
                venue, base, instrument, navigation
            )
        except (SpotV2OfficialPageDiscoveryError, UnicodeDecodeError, ValueError):
            unresolved.append(
                f"{pair}:AMBIGUOUS_KNOWN_TICKER_COLLISION"
                if collision
                else f"{pair}:NAVIGATION_RESPONSE_INVALID"
            )
            print(
                f"SPOT_V2_DISCOVERY_PROGRESS pair={pair} "
                f"resolved={len(request_plan)} unresolved={len(unresolved)} "
                f"requests={request_count}",
                flush=True,
            )
            continue
        if len(candidates) != 1:
            unresolved.append(
                f"{pair}:AMBIGUOUS_KNOWN_TICKER_COLLISION"
                if collision
                else (
                    f"{pair}:EXACT_OFFICIAL_URL_NOT_FOUND"
                    if not candidates
                    else f"{pair}:AMBIGUOUS_OFFICIAL_URL"
                )
            )
            print(
                f"SPOT_V2_DISCOVERY_PROGRESS pair={pair} "
                f"resolved={len(request_plan)} unresolved={len(unresolved)} "
                f"requests={request_count}",
                flush=True,
            )
            continue
        try:
            official_body = fetch_counted(candidates[0])
            identifier = _canonical_identifier_from_official_page(
                base, instrument, official_body
            )
            request_plan.append(
                _request_plan_item(
                    venue=venue,
                    base=base,
                    official_url=candidates[0],
                    identifier=identifier,
                )
            )
        except (SpotV2OfficialPageDiscoveryError, IdentityVerificationError):
            unresolved.append(
                f"{pair}:AMBIGUOUS_KNOWN_TICKER_COLLISION"
                if collision
                else f"{pair}:CANONICAL_IDENTIFIER_NOT_UNIQUE"
            )
        print(
            f"SPOT_V2_DISCOVERY_PROGRESS pair={pair} "
            f"resolved={len(request_plan)} unresolved={len(unresolved)} "
            f"requests={request_count}",
            flush=True,
        )

    complete = len(request_plan) == 18 and not unresolved
    return SpotV2DiscoveryResult(
        status=(
            "SPOT_V2_OFFICIAL_PAGE_DISCOVERY_COMPLETE"
            if complete
            else "SPOT_V2_OFFICIAL_PAGE_DISCOVERY_INCOMPLETE"
        ),
        request_plan=tuple(request_plan),
        unresolved_pairs=tuple(unresolved),
        request_count=request_count,
        identity_verdict=False,
        network_accessed=True,
    )


def write_discovery_bundle(
    result: SpotV2DiscoveryResult,
    *,
    user_approval_text: str,
    generated_at_utc: str,
) -> dict[str, Any]:
    _require(
        normalize_approval_text(user_approval_text) == EXPECTED_APPROVAL_TEXT,
        "approval text mismatch",
    )
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    request_plan_path = OUTPUT_ROOT / "request-plan.json"
    manifest_path = OUTPUT_ROOT / "manifest.json"
    _require(not request_plan_path.exists(), "discovery output already exists")
    request_plan = list(result.request_plan)
    request_plan_hash = hashlib.sha256(canonical_json_bytes(request_plan)).hexdigest()
    manifest = {
        "schema": "trading_mvp_slow_liquidity_spot_v2_official_page_discovery_output_v1",
        "status": result.status,
        "generated_at_utc": generated_at_utc,
        "plan_id": PLAN_ID,
        "plan_hash": "becfd2d04871b435614f8a0785ac9e6f90c79cc3537b9868745508bc73e45d20",
        "identity_verdict": False,
        "identity_evidence": False,
        "network_accessed": result.network_accessed,
        "request_count": result.request_count,
        "request_plan_count": len(request_plan),
        "unresolved_pairs": list(result.unresolved_pairs),
        "request_plan_sha256": request_plan_hash,
        "retry_authorized": False,
        "v7_used": False,
        "perp_template_used": False,
    }
    request_plan_path.write_text(
        json.dumps(request_plan, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    if not APPROVAL_RECEIPT_PATH.exists():
        receipt = {
            "schema": "trading_mvp_slow_liquidity_spot_v2_discovery_approval_receipt_v1",
            "status": "APPROVED_SINGLE_USE_VISIBLE_DISCOVERY",
            "user_approval_text": EXPECTED_APPROVAL_TEXT,
            "plan_id": PLAN_ID,
            "plan_hash": "becfd2d04871b435614f8a0785ac9e6f90c79cc3537b9868745508bc73e45d20",
            "plan_file_sha256": "10d6cc6407915c49969711afc013e5865d3179b03a2166fb647c88abfd3b4360",
            "output_manifest_path": str(manifest_path),
            "request_plan_path": str(request_plan_path),
            "request_plan_sha256": request_plan_hash,
            "identity_verdict": False,
            "retry_authorized": False,
        }
        APPROVAL_RECEIPT_PATH.parent.mkdir(parents=True, exist_ok=True)
        APPROVAL_RECEIPT_PATH.write_text(
            json.dumps(receipt, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    return {
        "status": result.status,
        "request_plan_path": str(request_plan_path),
        "manifest_path": str(manifest_path),
        "request_plan_sha256": request_plan_hash,
        "request_plan_count": len(request_plan),
        "unresolved_pairs": list(result.unresolved_pairs),
        "request_count": result.request_count,
        "identity_verdict": False,
        "network_accessed": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-approved-visible-discovery", action="store_true")
    parser.add_argument("--user-approval-text", default="")
    args = parser.parse_args(argv)
    if not args.run_approved_visible_discovery:
        raise SystemExit("no authorized action requested")
    approval = args.user_approval_text or EXPECTED_APPROVAL_TEXT
    if normalize_approval_text(approval) != EXPECTED_APPROVAL_TEXT:
        raise SystemExit("approval text mismatch")
    plan = json.loads(DISCOVERY_PLAN_PATH.read_text(encoding="utf-8"))
    print("SPOT_V2_DISCOVERY_START", flush=True)
    try:
        result = discover_spot_v2_official_pages(
            plan,
            user_approval_text=approval,
        )
    except SpotV2OfficialPageDiscoveryError:
        result = SpotV2DiscoveryResult(
            status="SPOT_V2_OFFICIAL_PAGE_DISCOVERY_INCOMPLETE",
            request_plan=(),
            unresolved_pairs=("RUN:DISCOVERY_CONTRACT_REJECTED",),
            request_count=0,
            identity_verdict=False,
            network_accessed=True,
        )
    from datetime import datetime, timezone

    generated = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )
    written = write_discovery_bundle(
        result,
        user_approval_text=approval,
        generated_at_utc=generated,
    )
    print(json.dumps(written, ensure_ascii=False), flush=True)
    print("SPOT_V2_DISCOVERY_DONE", written["status"], flush=True)
    return 0 if result.status.endswith("COMPLETE") else 2


if __name__ == "__main__":
    raise SystemExit(main())

