# Fresh-night reference products

Normal scheduled sleep/HRV jobs finish their existing source operation before
building references for the current local wake date. Their internal default-false
defer option suppresses the earlier legacy tail only in this scheduled enclosure,
so it does not race or precede the ten-product producer. Normal general Refresh
and targeted current-night Refresh use the same stored-only adapter after the
sleep, HRV and required HR source checks qualify. Manual historical backfill,
activity-only and repair inputs retain their existing entry guards.

The adapter explicitly selects 7,28,30,90 and 365 days for both HR and HRV.
It does not change the legacy default (28,30,90), select other endpoints, or
drain older calendar work. Existing workflow serialization remains in force.
Source checks do not become fresh Garmin checks merely because references run.

The existing reference_bulk engine inventories and collects the selected
stored sources into one local immutable workset. Multiple bounded collection
stages keep the same durable ACCOUNT, target, deadline and cumulative usage.
Every successful collection stage must make progress; denied, unknown and
held outcomes stop immediately. All ten products are built and sealed before
the first product upload. Ordinary prepared-manifest publication uses at most
two eight-row passes with the existing source fences, conditional writes,
history, queue receipts and readback. The caller's original R2Store write and
byte allowances are shared across the phase views rather than renewed.

A complete normal producer result means all ten canonical products received
the ordinary producer/queue ACK contract. It does not assert independent
Root SDK/MCP verification or set a bulk verified checkpoint. Budget deferral
remains pending; it is never reported as full publication. There is no retry,
new scheduler, extra Garmin reference fetch, new credential or quota increase.
The local workset belongs to this running job; it is not a new historical job
or a promise of cross-job recovery after a runner is destroyed.
