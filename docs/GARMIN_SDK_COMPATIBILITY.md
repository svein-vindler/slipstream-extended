# Garmin SDK compatibility

The runtime and development locks pin `garminconnect` 0.3.20. Both locks are
regenerated from the inputs and retain Windows-only `colorama` and `tzdata`
markers and hashes. Other package versions, Worker dependencies, configuration,
permissions and collection schedules are unchanged.

Upstream [0.3.19](https://github.com/cyberjunky/python-garminconnect/releases/tag/0.3.19)
split the API/client into modules and introduced profile-timezone date helpers.
[0.3.20](https://github.com/cyberjunky/python-garminconnect/releases/tag/0.3.20)
fixes login handling and adds an MFA-resumption API. Slipstream does not adopt
new API methods, MFA state, automatic date helpers or endpoint permissions.
Existing explicit dates, serialized-token handling and authentication separation
remain in place. Validation uses synthetic transports, not live Garmin logins.

## Read-only and logging boundaries

The reviewed `Client`, requests session, headers and zero-retry adapter are
still required by bounded training-readiness reads. The supported runtime version
changes to exactly 0.3.20, not an arbitrary range. GET/path/host/parameter guards,
no redirects, response byte limits and the one-fitness/zero-component-auth budget
remain unchanged. The general activity-page reader still bypasses SDK outer
retry decorators without bypassing either read-only guard.

The SDK now logs from `garminconnect.api._common` and the client's `sso`,
`session`, `tokens` and `transport` modules. Hosted CLI projection covers those
reviewed names plus the two previous names. Levels, handlers, propagation and
prior filters are restored; private in-process errors retain their old behavior.

## Stored provenance and retained measurements

Canonical training days and archived Coach context accept only `0.3.17`,
`0.3.20` and the explicit `mixed` provenance value. Fresh observations use
0.3.20. A failed check retaining only old data keeps its old version. A partial
update retaining data from an earlier SDK is `mixed`, including conservatively
retained data from an already mixed object. A complete replacement can become
0.3.20 again. Measurement/check clocks, units, statuses and retention flags are
preserved. Historical objects and genuine historical fixtures are not rewritten.
Unknown SDK versions and malformed metadata remain invalid.

Training-history MCP output includes each day's SDK in its provenance. Its
shared SDK value is the one observed version, `mixed` for different versions,
or null when no valid canonical source was read. There is no new provider call,
index, cache, discovery, output ceiling or storage migration.

## Upgrade order and recovery

Keep producers paused while installing compatible Worker readers. Merge only
after correct-head CI passes, verify new main CI, deploy the compatible private
reader with existing config/bindings, and check stored data before resuming the
previously active producers. Keep training-readiness collection disabled unless
separately approved. Never activate or deploy public data workflows.

After new-version data has been stored, do not roll back to readers or backup
tools that recognize only 0.3.17. Keep the compatible readers and correct forward;
do not delete, regenerate, relabel or migrate stored history as a rollback step.
Synthetic tests and CI establish compatibility, not provider availability,
successful real authentication or reduced live request volume.
