# Local transport beta (0.7.0b3)

This branch starts at original upstream commit
`3b3382653ea1daaa203fe70d9e4a7e46910105b3`.

## Compatibility contract

Keep the existing integration entry. Do not remove/re-add it, rename its title,
change its domain, delete registry entries, or replace configuration YAML.
The integration directory/domain remains `4heat`. Config flow version remains 1.
Existing sensor and switch names, unique-ID formulas, device identifiers,
state labels, register mapping and `4heat.set_value` action remain unchanged.
No Tado configuration or heating policy is part of this change.

Commands are no longer silently successful after network errors. Failures raise
an action error and ambiguous commands are never automatically repeated.
Only a one-item SEC echo matching the submitted command is recorded as an
acknowledgment. Other responses are rejected or unconfirmed; a successful
acknowledgment still does not prove that the requested action has completed.
Actual operating state comes from telemetry, not an optimistic switch update.
Failed polls mark coordinator entities unavailable instead of claiming stale
data is fresh. These intentional behavior changes should be observed in HA
before enabling new heating policies.

## Communication

- Defaults: 10 seconds active (including ignition, extinguishing and standby),
  60 seconds OFF/Safety/Block, 5-second transaction deadline, 0.5-second
  connection settling.
- Configure these under the integration's options; saving options reloads it.
- Active/unknown-state failures back off from the active interval to the
  configured active maximum (default 10, 20, 40, 60 seconds). Known idle failures
  retain the idle backoff, capped at 300 seconds. Pending commands use active
  polling/backoff once a coordinator poll runs.
- One async lock serializes all requests. Commands wait for an in-flight poll;
  there is no command priority/preemption and no background retry worker.
- State, error and water sensors include communication/source timestamps and
  the last command result. Module-cache communication is explicitly distinguished
  from a direct board read. A partial explicit fallback may retain older
  noncritical registers.
- Integer-register interpretation is retained. Wider signed/scaled protocol
  formats and model-specific parameters are deliberately not introduced here.
- No write ranges are inferred for arbitrary model-specific registers. The
  service validates integer shape, not whether a value is safe for a stove.

## Validation completed locally

Run `python3 -m unittest discover -s tests -v` and
`python3 -m compileall -q custom_components tests`.
Transport tests use actual localhost TCP streams, including fragmentation,
cancellation, disconnection, timeouts and read/write serialization.
Coordinator tests stub the HA boundary; they are not a full HA runtime test.
Compatibility tests freeze upstream identity expressions and state mappings.
The status fixture retains a real packet's structure with synthetic values.
One real read-only SEL/0 query passed through the new transport. No live control
commands have been sent. Full HA reload/UI tests and live command tests are
still required before treating this beta as production validated.

## Deployment procedure (not yet performed)

1. Back up the current custom_components/4heat directory, current integration
   config entry and entity/device registry snapshots privately. Record all
   existing 4heat entity IDs, unique IDs and device IDs for comparison.
2. Arrange a suitable maintenance window. Copy only the candidate integration
   directory into /config/custom_components/4heat; retain the existing entry.
   Remove stale Python bytecode from that directory if needed.
3. Restart HA to ensure the new Python modules are loaded. Check logs and compare
   the entity/device inventory with the pre-deploy snapshot. No duplicate or
   suffixed entities should appear. Check the set_value service's existing
   register attributes and the options UI.
4. Observe successful communication and polling intervals, including availability
   during a network fault. Verify a real control action only when appropriate
   for the stove; distinguish command acceptance from phase transitions.
5. If needed, restore the saved integration directory and restart HA. Retain
   the original entry/registries. Do not reset the integration to roll back.

No HA files, room targets or automations were modified while developing this beta.


## Optional SYEVO0000513 diagnostics (beta 2)

Only enable `Read additional diagnostics (SYEVO0000513 only)` for product
SYEVO0000513 version 1. This is an explicit model profile, not automatic discovery.
It is OFF by default and saving options reloads the integration. Existing entities
retain their names and unique IDs. Four new diagnostic entities distinguish
selected pellet power (Auto = maximum manual setting + 1), actual power/status,
auger reading and combustion fan reading.

One read-only SEC packet with three I-prefixed registers is sent at most every
30 seconds by default, scheduled after successful normal coordinator polls. With 10-second active
polling this adds up to two local requests per minute. The actual interval can be
longer when the main poll interval or failure backoff is longer. The task runs in the background, not inside the main update awaited by HA.
It notifies diagnostic entities without replacing the main snapshot, and is
cancelled on unload. An already-running transaction still holds the shared TCP
lock until completion/timeout; it cannot be preempted. No new cloud requests or
write actions are introduced. A failed extra query makes these extra
readings unavailable, while successful normal state/water telemetry stays usable.

Actual power 9 is P Mod on this profile. The auger reading uses the app's base
signed type-5 /100 decoder and retains its raw register attribute. Its physical
unit is deliberately unset: no fuel consumption, kW, duty percent or duration is
inferred, and the map's additional multiplier is not applied without validation.
The fan reading is also unitless pending validation. These are diagnostic values,
not confirmed energy/performance measurements.

Local read-only validation returned values for all three registers. The code has
not been deployed to HA, and its options/entities still need an HA runtime check.


## Firmware-informed reads and command tracking (beta 3)

The firmware 4.75 analysis supports retaining the normal cached `SEL/0` poll and
using small explicit `SEC` reads. New options are available under Local
communication; existing entry data and identities do not need migration:

| Option | Default | Meaning |
| --- | --- | --- |
| Read state, error and water temperature directly | Off | Opt in to a separate three-register board request; validate support on the installed model. |
| Minimum interval between direct reads | 10 seconds | Minimum spacing; actual cadence follows main polling and backoff. |
| Maximum retry interval while active | 60 seconds | Cap, never below the configured active interval. |
| Wait for observed command result | 600 seconds | Stop tracking an unconfirmed result after this window; never resend it automatically. |

`critical_data_source` is `module_cache` or `direct_read`. The latter can reuse
the last direct values between direct polls so an older SEL cache cannot
immediately overwrite them. `last_direct_read` advances only after a successful
explicit read, while `last_successful_communication` can advance on a cached
response. These are transaction timestamps, not timestamps supplied by physical
probes. For freshness-dependent automations, compare the timestamp to the current
time as well as checking availability, source and `direct_read_status`. A failed
direct read falls back to labelled cached values; it never advances the direct
timestamp. Required main-poll failures still make entities unavailable.

Commands expose `last_command` on the state switch and state/error/water sensors:
`action`, `requested_at`, `acknowledgment`, `status`, and confirmation timestamps.
Only a subsequent explicit board read can confirm the observable result. ON is
confirmed when a starting/running phase is observed, not when normal Run is
necessarily reached. OFF remains pending through ignition, Block or
extinguishing until state OFF is observed. Unblock needs a cleared error and a
state other than Block. A set-value command reads back its specific register and
compares the integer value. Pending commands request these direct reads even
when periodic direct telemetry is disabled.

A matching acknowledgment records receipt, not causal proof that a command
produced a transition. A missing/unrecognized acknowledgment raises an action
error but keeps observing a possibly transmitted command. Unknown acknowledgment
forms are deliberately unconfirmed pending live validation. Repeated identical
calls while pending are coalesced; a later explicitly invoked command after the
window expires may send again. Different requested actions supersede tracking of
the previous action. There is no autonomous write retry, technical-parameter
write discovery or change to existing switch state semantics.

Explicit outbound SEC packets are capped at **14 items**, conservatively matching
the ordinary queue worker found in firmware 4.75. Oversized write packets fail
before connecting. A read-only helper validates the entire A/I collection first,
then splits it into chunks of at most 14, validates each response against the
requested registers, and returns no partial collection on failure. It never
splits or retries writes. This restriction does not cap the existing 18-value
cached SEL response. No boundary-size packet has been sent to the real stove.

Validation remains simulated at the HA boundary. Full HA options/entity lifecycle
checks and verification of actual command acknowledgment forms and observed
transitions are still deployment prerequisites. No new live control test has
been performed for beta 3.
