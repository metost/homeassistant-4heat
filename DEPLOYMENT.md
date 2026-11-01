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
