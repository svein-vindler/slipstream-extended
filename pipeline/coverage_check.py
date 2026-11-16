"""Compare an independent product plan with already validated delivery evidence."""
import json
import re
from hashlib import sha256


def _same(left, right):
    return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(
        right, sort_keys=True, allow_nan=False)


def _product_reason(plan, publication, readback):
    for evidence in (publication, readback):
        if (evidence.get('valid') is not True
                or type(evidence.get('revision')) is not str or not evidence['revision']
                or type(evidence.get('sha256')) is not str
                or type(evidence.get('source_partial', False)) is not bool
                or not re.fullmatch('[a-f0-9]{64}', evidence['sha256'])):
            return 'invalid_product'
        try:
            json.dumps({field: evidence[field] for field in
                        ('args', 'kind', 'schema_version', 'method_version', 'source_identity')
                        if field in evidence}, sort_keys=True, allow_nan=False)
        except (TypeError, ValueError):
            return 'invalid_product'
        for field, reason in (('args', 'wrong_variant'), ('kind', 'wrong_kind'),
                              ('schema_version', 'wrong_schema_version'),
                              ('method_version', 'wrong_method_version')):
            if not _same(plan.get(field), evidence.get(field)):
                return reason
        if 'source_identity' in plan and not _same(
                plan['source_identity'], evidence.get('source_identity')):
            return 'stale_source_identity'
    if any(publication[field] != readback[field] for field in ('revision', 'sha256')):
        return 'publication_verification_mismatch'
    return None


def _index(items):
    by_key, duplicates = {}, set()
    for item in items:
        key = item['key']
        if key in by_key:
            duplicates.add(key)
        by_key[key] = item
    return by_key, duplicates


def _invalid(requested, published, verified, reason):
    counts = {name: len(items) if type(items) in (list, tuple) else None
              for name, items in (('planned', requested), ('published', published),
                                  ('verified', verified))}
    counts.update(requested=None, complete=0, excluded=0, pending=0, blocked=1,
                  source_partial=0)
    return {'coverage_complete': False, 'source_partial': False, 'counts': counts,
            'rows': [{'key_sha256': None, 'status': 'blocked', 'reason': reason}]}


def _valid_plan(row):
    if (type(row.get('args')) is not dict
            or type(row.get('requested', True)) is not bool
            or type(row.get('schema_version')) is not int or row['schema_version'] < 1
            or any(type(row.get(field)) is not str or not row[field]
                   for field in ('kind', 'method_version'))
            or ('source_identity' in row and row['source_identity'] in (None, '', {}))):
        return False
    try:
        json.dumps({field: row[field] for field in ('args', 'source_identity')
                    if field in row}, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError):
        return False
    return True


def check_coverage(requested, published, verified, *, explicit_exclusions=(), pending=()):
    """Check independently planned scope against normalized, validated observations.

    Plan: key,args,kind,schema_version,method_version; optional source_identity
    and requested=False. Evidence: those semantics plus revision,sha256,valid=True.
    The caller authenticates receipts/readbacks and supplies actual body semantics.
    sha256 binds publication to readback, never to a prepared calculation payload.
    Exclusions: key,reason in source_missing/unrequested/open_period, only for
    requested=False. Pending: key,reason=recent_source, only without delivery.
    A true readback source_partial flag records known partial Source separately.
    No I/O, proof validation, product computation, retry or mutation occurs here.
    """
    inputs = (requested, published, verified, explicit_exclusions, pending)
    if any(type(items) not in (list, tuple) or any(
            type(row) is not dict or type(row.get('key')) is not str or not row['key']
            for row in items) for items in inputs):
        return _invalid(requested, published, verified, 'invalid_input')
    if not requested or any(not _valid_plan(row) for row in requested):
        return _invalid(requested, published, verified, 'invalid_requested')
    indexed = [_index(items) for items in
               (requested, published, verified, explicit_exclusions, pending)]
    plans, publications, readbacks, exclusions, waiting = [pair[0] for pair in indexed]
    duplicates = set().union(*(pair[1] for pair in indexed))
    rows = []
    partial_count = 0
    for key in set().union(plans, publications, readbacks, exclusions, waiting):
        item = plans.get(key)
        reason = None
        if key in duplicates:
            reason = 'duplicate_evidence'
        elif item is None:
            reason = 'unexpected_evidence'
        elif ((key in exclusions and (item.get('requested', True) is not False
                                     or key in publications or key in readbacks or key in waiting))
              or (key in waiting and (item.get('requested', True) is not True
                                      or key in publications or key in readbacks))
              or (item.get('requested', True) is False
                  and (key in publications or key in readbacks))):
            reason = 'contradictory_evidence'
        if reason:
            rows.append({'key_sha256': sha256(key.encode()).hexdigest(),
                         'status': 'blocked', 'reason': reason})
            continue
        if item.get('requested', True) is False:
            exclusion = exclusions.get(key, {})
            reason = exclusion.get('reason')
            allowed = reason in ('source_missing', 'unrequested', 'open_period')
            rows.append({'key_sha256': sha256(key.encode()).hexdigest(),
                         'status': 'excluded' if allowed else 'blocked',
                         'reason': reason if allowed else 'unclassified_unrequested'})
            continue
        if key in waiting and key not in publications and key not in readbacks:
            allowed = waiting[key].get('reason') == 'recent_source'
            rows.append({'key_sha256': sha256(key.encode()).hexdigest(),
                         'status': 'pending' if allowed else 'blocked',
                         'reason': 'recent_source' if allowed else 'invalid_pending'})
            continue
        reason = ('missing_publication' if key not in publications else
                  'missing_verification' if key not in readbacks else None)
        if reason is None:
            reason = _product_reason(item, publications[key], readbacks[key])
        if reason is None and readbacks[key].get('source_partial') is True:
            partial_count += 1
        rows.append({'key_sha256': sha256(key.encode()).hexdigest(),
                     'status': 'blocked' if reason else 'complete', 'reason': reason})
    counts = {'planned': len(requested),
              'requested': sum(row.get('requested', True) is True for row in requested),
              'published': len(published), 'verified': len(verified),
              **{status: sum(row['status'] == status for row in rows)
                 for status in ('complete', 'excluded', 'pending', 'blocked')},
              'source_partial': partial_count}
    return {'coverage_complete': bool(rows) and not counts['blocked'] and not counts['pending'],
            'source_partial': bool(partial_count), 'counts': counts,
            'rows': sorted(rows, key=lambda row: row['key_sha256'])}
