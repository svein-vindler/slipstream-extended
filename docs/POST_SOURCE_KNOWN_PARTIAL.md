# Known partial DAILY closure for post-Source span proof

The existing `ROOT_CLOSED_DAILY_FOR_POST_SOURCE_SPAN_V1` contract is unchanged.
A controller result of 1 continues to fail that contract.

The separate `ROOT_KNOWN_PARTIAL_DAILY_FOR_POST_SOURCE_SPAN_V1` closure contains
the same fields plus one `rootCompletion` immutable reference. Its outer result
is exactly 1 and its managed data result is exactly 2. The completion joins the
closed physical Root observation, independent restoration, restoration lease,
control finalization/disposition, finalizer lease and raw-delivery observation.
Their kinds, binding, invocation, native results, PIDs and cross-references must
agree. Every listed native tree must be physically terminal; restored workflow
states must equal all fourteen original states.

This contract retains `baselines_pending`, a positive remaining reference
cohort, `sourceFailed: false`, the unknown original Hold cause and the earlier
full-span hold. It qualifies raw delivery and physical closure only. It never
claims whole-reference success, resets counters, adopts unknown work, removes a
lock or takes over ownership. Existing raw producer, SDK, normal-reader and
source-meter proof validation still runs. The original span prefix remains
separate from the later append-only Source prefix.

The control finalizer's `before` snapshot is immutable; its Root owner,
invocation, binding and PID must match the same DAILY and physical controller.
That historical snapshot need not be terminal. Its `after`
reference names mutable CONTROL and is not opened during historical replay.
The transition independently pins its current state; ordinary Root ownership,
fresh permission and CAS remain the caller's responsibility.

## Focused opt-in component regression

`tests/test_post_source_known_partial.py` calls the public
`validate_span_release_transition` interface. Set `SLIPSTREAM_FROZEN_POST_SOURCE`
to the separately frozen synthetic post-Source component package. It reuses the
already observed 178 -> 187 proof graph without running Source, R2, a native
publisher or a full history rebuild. The new external Root records are synthetic
and use the actual known-partial evidence shapes. Tests cover the new positive,
the existing legacy positive, source failure, unknown cleanup, unverified raw
delivery, incorrect restoration, a foreign finalizer-before executor and
rejection of an untyped outer failure.
They assert that old/current source and span ledger bytes, checkpoint and Root
state remain unchanged.

Without that external synthetic package these opt-in tests skip explicitly.
They are local component regression evidence, not self-contained public CI or
an end-to-end operational run. Extra local metadata reads are not Garmin or R2
requests; no resource saving or full historical completion is claimed.
