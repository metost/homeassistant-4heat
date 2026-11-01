# Local transport beta (0.7.0b1)

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
A SEC response acknowledges receipt at the protocol level; actual operating
state still comes from subsequent telemetry, not an optimistic switch update.
Failed polls mark coordinator entities unavailable instead of claiming stale
data is fresh. These intentional behavior changes should be observed in HA
before enabling new heating policies.

## Communication

- Defaults: 10 seconds active (including ignition, extinguishing and standby),
  60 seconds OFF/Safety/Block, 5-second transaction deadline, 0.5-second
  connection settling.
- Configure these under the integration's options; saving options reloads it.
- Failures back off from the configured idle interval to at most 300 seconds.
- One async lock serializes all requests. Commands wait for an in-flight poll;
  there is no command priority/preemption and no background retry worker.
- Status includes `last_successful_communication` and `poll_interval_seconds`.
  A successful partial SEC fallback freshens required state/error/water fields
  but may retain older noncritical registers.
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
30 seconds by default, during normal coordinator polls. With 10-second active
polling this adds up to two local requests per minute. The actual interval can be
longer when the main poll interval or failure backoff is longer. No new cloud
requests or write actions are introduced. A failed extra query makes these extra
readings unavailable, while successful normal state/water telemetry stays usable.

Actual power 9 is P Mod on this profile. The auger reading uses the app's base
signed type-5 /100 decoder and retains its raw register attribute. Its physical
unit is deliberately unset: no fuel consumption, kW, duty percent or duration is
inferred, and the map's additional multiplier is not applied without validation.
The fan reading is also unitless pending validation. These are diagnostic values,
not confirmed energy/performance measurements.

Local read-only validation returned values for all three registers. The code has
not been deployed to HA, and its options/entities still need an HA runtime check.
