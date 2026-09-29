"""Fail-closed contract for an additive OFFLINE Expansion review bundle.

Validation is read-only. Publication never replaces an existing directory or
file. A failed/partial publication is retained for diagnosis, never retried or
deleted here. The caller must locate output outside source evidence directories.
This module grants no network, collector, scheduler, or trading authority.
"""
from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path


MAX_FILE_BYTES = 32 * 1024 * 1024
IMPLEMENTATION_FILENAMES = {
    'review_contract': 'listing_expansion_review_contract.py',
    'review_queue': 'listing_expansion_review_queue.py',
    'evidence_validator': 'listing_expansion_identity_evidence.py',
    'materializer': 'listing_expansion_review_materializer.py',
    'backlog': 'listing_expansion_backlog.py',
}
REQUIRED_IMPLEMENTATIONS = set(IMPLEMENTATION_FILENAMES)
POLICY = dict(retry_slots=10, max_proposals=50, aging_slots_per_venue=1,
              official_future_horizon_sec=86400, official_active_after_sec=60,
              recent_metadata_horizon_sec=259200, separate_lanes=True,
              migration='LINKED_OVERLAY_NO_SOURCE_REWRITE', selection_not_execution=True)
FLAGS = dict(research_only=True, public_data_only=True, network_allowed=False,
             collector_allowed=False, scheduler_allowed=False, private_api=False,
             live_orders=False)


def _require(ok, reason):
    if not ok:
        raise ValueError('expansion review contract: ' + reason)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _valid_hash(value):
    return (isinstance(value, str) and len(value) == 64
            and all(c in '0123456789abcdef' for c in value))


def _absolute(path):
    _require(isinstance(path, (str, os.PathLike)), 'path must be absolute')
    result = Path(path)
    _require(result.is_absolute() and '..' not in result.parts, 'path must be absolute without parent traversal')
    return result


def _no_links(path):
    """Check each existing ancestor without following its final component."""
    for part in reversed((path, *path.parents)):
        try:
            info = part.lstat()
        except FileNotFoundError:
            continue
        _require(not stat.S_ISLNK(info.st_mode)
                 and not (getattr(info, 'st_file_attributes', 0) & 0x400),
                 'symlink or reparse path is forbidden')


def _signature(info):
    # On this Windows/Python runtime path.stat().st_ctime_ns is creation time
    # while fstat().st_ctime_ns may be change time. They are not comparable.
    # Stable file identity, size, mtime and birthtime plus exact bound bytes are.
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            getattr(info, 'st_birthtime_ns', None))


def _read_stable(path):
    path = _absolute(path)
    _no_links(path)
    before = path.stat()
    _require(stat.S_ISREG(before.st_mode), 'source is not a regular file')
    _require(before.st_size <= MAX_FILE_BYTES, 'source exceeds 32 MiB size limit')
    with path.open('rb') as handle:
        opened = os.fstat(handle.fileno())
        _require(_signature(before) == _signature(opened), 'source changed before read')
        raw = handle.read(MAX_FILE_BYTES + 1)
        after_open = os.fstat(handle.fileno())
    _no_links(path)
    after_path = path.stat()
    _require(len(raw) <= MAX_FILE_BYTES, 'source exceeds 32 MiB size limit')
    _require(_signature(before) == _signature(after_open) == _signature(after_path)
             and len(raw) == before.st_size, 'source changed during read')
    return raw


def _strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, 'duplicate JSON key')
            result[key] = value
        return result

    def invalid(value):
        raise ValueError('expansion review contract: non-finite JSON number')

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


def validate_plan(plan, expected_plan_path=None):
    """Verify an immutable offline plan and return {role: verified_file_bytes}.

    The canonical plan hash excludes only the top-level `plan_hash` member.
    Both file groups share one role namespace. All reads are bounded/stable and
    checked before returning; this function performs no writes. Optional
    expected_plan_path must contain exactly the supplied parsed plan.
    """
    _require(isinstance(plan, dict), 'plan must be an object')
    expected = plan.get('plan_hash')
    _require(_valid_hash(expected), 'invalid plan hash')
    body = {key: value for key, value in plan.items() if key != 'plan_hash'}
    encoded = json.dumps(body, sort_keys=True, ensure_ascii=False,
                         separators=(',', ':'), allow_nan=False).encode('utf-8')
    _require(_sha(encoded) == expected, 'plan hash mismatch')
    _require(plan.get('schema') == 'listing_expansion_review_planonly_v1', 'wrong schema')
    _require(isinstance(plan.get('plan_id'), str) and bool(plan['plan_id'].strip()), 'empty plan identity')
    _require(plan.get('status') == 'OFFLINE_READY_NOT_ACTIVATED', 'wrong plan status')
    for field, value in FLAGS.items():
        _require(plan.get(field) is value, 'unsafe or missing flag: ' + field)
    policy = plan.get('policy')
    _require(isinstance(policy, dict), 'policy missing')
    for field, value in POLICY.items():
        actual = policy.get(field)
        _require(type(actual) is type(value) and actual == value, 'wrong policy: ' + field)
    if expected_plan_path is not None:
        _require(_strict_json(_read_stable(expected_plan_path)) == plan, 'plan file differs from supplied plan')
    bindings = []
    roles = set()
    implementation_roles = set()
    implementation_paths = set()
    for group in ('input_files', 'implementation_files'):
        rows = plan.get(group)
        _require(isinstance(rows, list) and bool(rows), 'empty or invalid ' + group)
        for row in rows:
            _require(isinstance(row, dict), 'invalid file binding')
            role = row.get('role')
            _require(isinstance(role, str) and bool(role.strip()), 'empty file role')
            _require(role not in roles, 'duplicate role: ' + role)
            roles.add(role)
            _require(_valid_hash(row.get('sha256')), 'invalid file hash: ' + role)
            path = _absolute(row.get('path'))
            if group == 'implementation_files':
                _require(role in IMPLEMENTATION_FILENAMES, 'unknown implementation role: ' + role)
                implementation_roles.add(role)
                _no_links(path)
                actual_path = path.resolve()
                _require(actual_path not in implementation_paths, 'duplicate implementation path')
                implementation_paths.add(actual_path)
                expected_path = Path(__file__).resolve().parent / IMPLEMENTATION_FILENAMES[role]
                _require(actual_path == expected_path, 'wrong implementation path: ' + role)
            bindings.append((role, path, row['sha256']))
    _require(REQUIRED_IMPLEMENTATIONS == implementation_roles, 'required implementation binding missing')
    verified = {}
    for role, path, digest in bindings:
        raw = _read_stable(path)
        _require(_sha(raw) == digest, 'file hash mismatch: ' + role)
        verified[role] = raw
    return verified


def _safe_leaf(name):
    _require(isinstance(name, str) and bool(name) and name not in {'.', '..'}, 'invalid leaf filename')
    _require(not any(c in name for c in '\\/:*?"<>|')
             and not any(ord(c) < 32 for c in name)
             and name == name.rstrip(' .'), 'unsafe leaf filename')
    reserved = {'CON', 'PRN', 'AUX', 'NUL', *(f'COM{i}' for i in range(1, 10)),
                *(f'LPT{i}' for i in range(1, 10)),
                *(f'{prefix}{suffix}' for prefix in ('COM', 'LPT') for suffix in '¹²³')}
    _require(name.split('.')[0].rstrip(' .').upper() not in reserved, 'reserved leaf filename')


def write_new_bundle(output_dir, files):
    """Publish bytes to an exclusive, new directory; return filename->SHA256.

    Validate the whole request before creating anything. Parent must already
    exist and no ancestor may be a symlink/reparse point. Existing outputs are
    never overwritten. An I/O failure leaves its partial directory untouched.
    A terminal manifest.json, when supplied, is written and verified last.
    """
    output = _absolute(output_dir)
    _require(isinstance(files, dict) and bool(files), 'bundle files missing')
    names = set()
    for name, raw in files.items():
        _safe_leaf(name)
        _require(name.casefold() not in names, 'case-insensitive duplicate leaf filename')
        names.add(name.casefold())
        _require(type(raw) is bytes and len(raw) <= MAX_FILE_BYTES, 'file must be bytes within 32 MiB size limit')
    _no_links(output)
    _require(output.parent.is_dir(), 'output parent must already exist')
    _require(not os.path.lexists(output), 'output already exists')
    output.mkdir(exist_ok=False)
    hashes = {}
    for name, raw in sorted(files.items(), key=lambda item: (item[0] == 'manifest.json', item[0])):
        _no_links(output)
        target = output / name
        with target.open('xb') as handle:
            _require(handle.write(raw) == len(raw), 'short bundle write')
            handle.flush()
            os.fsync(handle.fileno())
        _require(_read_stable(target) == raw, 'bundle readback mismatch')
        hashes[name] = _sha(raw)
    return hashes
