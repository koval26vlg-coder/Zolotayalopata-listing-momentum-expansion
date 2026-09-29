"""Offline identity evidence transformation; no network, state writes or orders."""
from collections import defaultdict
from pathlib import Path
import re

BITGET_SOURCE_URL = 'https://api.bitget.com/api/v3/market/instruments?category=SPOT'


def bitget_identity_overlay(entries, payload, source):
    """Caller first verifies source bytes against the bound evidence manifest.

    Match an exact SPOT market. Absence, duplicate rows, contradictory metadata or
    unsupported classes are not negative identity evidence and remain unclassified.
    Metadata launchTime is deliberately NOT turned into an announcement t0.
    """
    if (source.get('source_url') != BITGET_SOURCE_URL
            or not re.fullmatch('[0-9a-f]{64}', str(source.get('source_sha256', '')))
            or not Path(str(source.get('source_path', ''))).is_absolute()
            or type(source.get('received_ts')) is not int or source['received_ts'] <= 0):
        raise ValueError('Invalid Bitget public source provenance')
    if not isinstance(payload, dict) or payload.get('code') != '00000' or not isinstance(payload.get('data'), list):
        raise ValueError('Bitget instruments response is not a successful list')
    index = defaultdict(list)
    for row in payload['data']:
        if not isinstance(row, dict):
            raise ValueError('Malformed Bitget instrument row')
        index[str(row.get('symbol', '')).upper()].append(row)
    result = {}
    for event_id, entry in entries.items():
        job = entry['job']
        if job.get('exchange') != 'bitget':
            continue
        matches = index.get(job['symbol'], [])
        if len(matches) != 1:
            continue
        row = matches[0]
        if (row.get('category') != 'SPOT' or row.get('quoteCoin') != 'USDT'
                or str(row.get('baseCoin', '')).upper() != job['base'].upper()):
            continue
        asset_class = {('crypto', 'no'): 'crypto_token', ('stock', 'yes'): 'tokenized_equity'}.get(
            (row.get('symbolType'), row.get('isReality')))
        if asset_class is None:
            continue
        launch = str(row.get('launchTime', ''))
        result[event_id] = {
            'status': 'VERIFIED', 'evidence_class': 'OFFICIAL_VENUE_IDENTITY',
            'source_storage': 'raw_http_response',
            'exchange': 'bitget', 'symbol': job['symbol'], 'base': job['base'],
            'asset_class': asset_class,
            **{key: source[key] for key in ('source_url', 'source_path', 'source_sha256', 'received_ts')},
            'metadata_launch_ts': int(launch) // 1000 if launch.isdecimal() and int(launch) > 0 else None,
            'official_spot_t0': None,
            'source_row': dict(row),
            'identity_method': 'exact_market_symbolType_and_isReality',
            'retrospective_classification_only': True,
        }
    return result
