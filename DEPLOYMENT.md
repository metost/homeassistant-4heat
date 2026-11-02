# Local transport beta (0.7.0b5)

This branch starts at upstream commit `3b3382653ea1daaa203fe70d9e4a7e46910105b3`.

## Compatibility

Retain the existing integration entry, title, domain `4heat`, config flow version
1, entity names, unique IDs, device identifiers, state labels, register mapping
and `4heat.set_value` action. Do not remove/re-add the entry or replace heating
configuration. No Tado configuration or heating policy changes are included.

## Authoritative telemetry (beta 5)

Beta 4 could report a frozen module cache after a command's first direct read
confirmed ignition. Its optional direct read also depended on a successful cached
poll, and failures could substitute old cached critical readings. Beta 5 removes
that dependency when direct telemetry is enabled:

- Every primary poll directly requests board status, error and water temperature.
  It publishes only a complete, validated response to that request. A failed
  direct read cannot fall back to SEL cache or a previous direct snapshot.
- An independent background SEL read refreshes legacy noncritical fields at most
  once per minute. Its status, error and water values are discarded. It merges
  against the latest coordinator data, so a delayed cache response cannot undo a
  newer board observation. Cache failure never invalidates successful core data.
- Extra reads wait briefly after core publication and skip starting while the
  transport is busy. All transactions share one lock; an already-running optional
  transaction cannot be preempted and can delay the next primary request.
- Direct telemetry remains an explicit model-support option. Enable it for this
  installation. If disabled, normal SEL polling remains available for legacy
  devices, and cannot guarantee physical freshness. Pending commands still force
  direct observations; periodic direct reads continue after confirmation when
  the option is enabled.

`critical_data_source` identifies `direct_read` versus `module_cache`.
`last_direct_read` advances only after a successful board request;
`last_cached_read` describes the independent cache transaction. These timestamps
are acquisition times, not measurement timestamps supplied by physical probes.
Unchanged values with advancing direct timestamps mean successful repeated
observations; an old direct timestamp must never be treated as fresh.

## Timing and failures

Defaults are 10 seconds active (including ignition, extinguishing and standby),
60 seconds OFF/Safety/Block, 5-second transaction deadline, 0.5-second connection
settling, and one second between transactions. The current deployment is tuned
to a seven-second deadline. Saving integration options reloads the entry.
The direct-read minimum interval can lengthen the primary interval. Intervals
and backoff are scheduling targets; network latency and shared transactions add
time. A short phase occurring entirely between observations may be missed.

Only zero-valued A/I reads and SEL/0 can retry once, after one second outside the
transport lock. Writes are single-attempt and never automatically repeated.
If both primary attempts fail, coordinator entities become unavailable. Recovery
starts at the active interval even after OFF, doubling up to 300 seconds when
idle or the configured active cap (default 60 seconds) when active/unknown.

Attributes include retry count, last transport failure/time and consecutive poll
failures. Failure details identify connection/send/receive phase and byte count.
Malformed or incomplete replies are not successful updates. Explicit SEC packets
are limited to 14 items; larger read collections split into validated chunks.
Writes are never split or retried. Existing longer cached SEL replies are allowed.

## Command receipts and observed results

The official app accepts valid SEC receipts without requiring an exact echo.
Beta 4's strict echo check rejected some legitimate receipts. Beta 5 records
matching echoes as `accepted`, and other valid SEC receipts as
`received_unverified`, retaining `protocol_response` for diagnosis. ERR is rejected;
network/protocol failures raise an action error and keep observing a possibly
transmitted command. No receipt fabricates an operating state.

`last_command` records action, receipt, pending/confirmed/unconfirmed status and
timestamps. Only a later direct read can confirm an observable result. ON means
a starting/running phase was observed, not necessarily normal Run. OFF remains
pending until OFF is observed. Unblock needs a cleared error and a state other
than Block. Set-value reads back and compares the requested register. A matching
value is an observation, not proof of causation. Pending identical calls coalesce;
different actions supersede tracking. Tracking expires after 600 seconds by
default, without resending. Unknown technical parameter addresses and safe write
ranges are not inferred.

## Optional SYEVO0000513/1 diagnostics

Enable the model-specific diagnostics only for that confirmed profile. Existing
entity identities remain unchanged; selected pellet power, actual combustion
power, auger and combustion fan are four additional entities. Selected power is
Auto when it equals the maximum manual setting plus one. Actual power 9 is P Mod.
The auger uses the app's base signed type-5 /100 decoder and retains its raw
register; its physical unit and the fan's physical unit remain unverified. These
are diagnostic readings, not measured kW or fuel consumption.

A separate three-register read is scheduled at most every 30 seconds by default,
following successful core polls; actual cadence can be longer. Optional errors
clear these readings without invalidating core observations. Tasks are cancelled
on unload. The extra data cannot overwrite the primary snapshot.

## Verification and installation

Run `python3 -m unittest discover -s tests -v`,
`python3 -m compileall -q custom_components tests` and `git diff --check`.
Transport tests use localhost TCP streams for fragmentation, cancellation,
timeouts, reconnect and serialization. Coordinator tests stub the HA boundary.
Regression tests keep SEL frozen while direct status progresses through ignition,
stabilization, Run, modulation, extinguishing and OFF; ensure reads continue
following command confirmation; ensure delayed cache responses cannot regress
critical data; and ensure direct failure cannot publish cached critical values.
Identity tests preserve upstream registry formulas and mappings. These tests do
not replace validation in a running HA instance or a normal physical burn cycle.

Before deployment, privately back up configuration, integration code, options and
entity/device inventory. Replace only `custom_components/4heat`, retaining the
entry and registries, and restart HA. Enable both confirmed direct/model options.
Check the installed version, original entity identities, source/timestamp
progression and agreement with direct read-only observations. Verify operating
transitions during normal use; do not ignite, extinguish or reset the stove just
for a software smoke test. Roll back by restoring the saved integration directory
and restarting HA, retaining the same entry.
