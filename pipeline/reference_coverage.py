"""Normalize existing bounded plans and already authenticated readback evidence."""
import json

from .coverage_check import check_coverage
from .history_state import digest
from .night_products import METHOD, normalize_args, storage_key
from .rolling_span_source_proof import pin, value


def declaration(key,args,source_identity=None,*,requested=True):
    row={'key':key,'args':args,'kind':'night-baseline','schema_version':1,
         'method_version':METHOD,'requested':requested}
    if source_identity is not None:
        row['source_identity']=source_identity
    return row


def observation(target,product):
    return {'key':target['key'],'args':product.get('args'),'kind':product.get('kind'),
        'schema_version':product.get('schema_version'),'method_version':product.get('method_version'),
        'source_identity':product.get('source_identity'),'revision':target['revision'],'sha256':target['sha256'],
        'valid':True,'source_partial':(type(product.get('source_nights')) is int
            and type(product.get('nights_requested')) is int and product['source_nights']<product['nights_requested'])}


def normalized_delivery(targets,readbacks):
    from .night_baseline_refresh import decode_product_snapshot
    from .r2_store import R2ObjectSnapshot
    rows=[]
    for target in targets:
        matches=[row for row in readbacks if row.get('keySHA256')==digest(target['key'].encode())]
        if len(matches)!=1:
            # Do not manufacture an observation for absent/ambiguous proof.
            continue
        check=matches[0]
        data=pin(check.get('decoded_body',check['body']))
        if data.startswith(b'\x1f\x8b'):
            data=decode_product_snapshot(R2ObjectSnapshot(data,target['revision'],'application/json','gzip'))
        rows.append(observation(target,json.loads(data)))
    return rows


def bulk_coverage(workset,proof):
    """Call only after the existing producer/SDK/reader proof validator succeeds."""
    sources={row['key'] for row in workset.required_sources()}
    requested=[]
    excluded=[]
    # Availability comes from the independent source seal/inventory, never successes.
    for target in workset.targets:
        day=target['endpoint']
        sleep=f'health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json'
        eligible=sleep in sources or sleep+'.gz' in sources
        identity=None
        if eligible and target['key'] in workset._products:
            identity=json.loads(workset.product_for(target['key'])['body'])['source_identity']
        requested.append(declaration(target['key'],target['args'],identity,requested=eligible))
        if not eligible:
            excluded.append({'key':target['key'],'reason':'source_missing'})
    verified=normalized_delivery(proof['generated_targets'],proof['readbacks'])
    # Publication semantics are the same pinned canonical bytes whose SHA/revision
    # the existing validator joined to both producer receipt and SDK postimage.
    return check_coverage(requested,verified,verified,explicit_exclusions=excluded)


def calendar_coverage(result,postcheck):
    scope=value(result['calendar_scope'])
    requested=[]
    for name in scope['selected_periods']:
        first,last=scope['periods'][name]
        for channel in ('heart_rate','hrv'):
            args=normalize_args(start_date=first,end_date=last,period='custom',channel=channel)
            requested.append(declaration(storage_key(args),args))
    if not requested:
        # Empty scope is explicit here; the helper never infers no-work from output.
        return {'coverage_complete':True,'source_partial':False,'scope':'no_closed_calendar_period_requested',
                'counts':{'planned':0,'requested':0,'published':0,'verified':0},'rows':[]}
    rows=normalized_delivery(result['generated_targets'],postcheck['readbacks'])
    return check_coverage(requested,rows,rows)


class CalendarCoverage:
    """Observe existing producer readbacks; never request another storage read."""
    def __init__(self):
        self.plan={}
        self.staged={}
        self.delivered={}

    def request_period(self,name,row):
        for channel in ((row['channel'],) if row.get('rolling_window') else ('heart_rate','hrv')):
            args=normalize_args(start_date=row['start_date'],end_date=row['end_date'],period='custom',channel=channel)
            key=storage_key(args)
            self.plan[key]=declaration(key,args)

    def expect_source(self,key,product):
        if key in self.plan:
            self.plan[key]['source_identity']=product['source_identity']

    def stage(self,name,key,snapshot):
        if key not in self.plan:
            return
        from .night_baseline_refresh import decode_product_snapshot
        product=json.loads(decode_product_snapshot(snapshot))
        target={'key':key,'revision':snapshot.revision,'sha256':digest(snapshot.data)}
        self.staged.setdefault(name,{})[key]=observation(target,product)

    def confirm(self,name):
        self.delivered.update(self.staged.get(name,{}))

    def result(self):
        if not self.plan:
            return {'coverage_complete':True,'source_partial':False,'scope':'no_selected_calendar_products',
                'counts':{'planned':0,'requested':0,'published':0,'verified':0},'rows':[]}
        return check_coverage(list(self.plan.values()),list(self.delivered.values()),list(self.delivered.values()))


def producer_coverage(workset,published):
    """Morning producer contract remains distinct from independent bulk ACK."""
    requested=[]
    rows=[]
    for target in workset.targets:
        source=json.loads(workset.product_for(target['key'])['body'])['source_identity']
        requested.append(declaration(target['key'],target['args'],source))
    batches=[]
    for reference in published['publication_receipts']:
        receipt=value(reference)
        batch=value(receipt['result'])
        qualified=(receipt.get('kind')=='reference-bulk-publication-receipt'
            and receipt.get('scope_sha256')==workset.scope_sha256
            and receipt.get('status')=='complete' and receipt.get('ownedSDKClosed') is True
            and receipt.get('componentCleanupConfirmed') is True
            and batch.get('scope_sha256')==workset.scope_sha256
            and batch.get('build_seal')==published['build_seal'])
        batches.append((reference,batch,qualified))
    for target in published['generated_targets']:
        matches=[(batch,qualified) for reference,batch,qualified in batches
                 if reference==target['producer_receipt']]
        if len(matches)!=1:
            continue
        batch,qualified=matches[0]
        actual=[row for row in batch['generated_targets'] if row['key']==target['key']]
        if len(actual)!=1:
            continue
        actual=actual[0]
        obs=value(actual['producer_observation'])
        fields=('key','args','revision','sha256','content_type','encoding','decoded_sha256')
        qualified=qualified and all(actual.get(field)==target.get(field)
            and obs.get(field)==target.get(field) for field in fields)
        product=obs.get('coverage_product',{})
        rows.append({**observation(target,product),'valid':qualified,
                     'source_partial':product.get('source_partial',False)})
    return check_coverage(requested,rows,rows)
