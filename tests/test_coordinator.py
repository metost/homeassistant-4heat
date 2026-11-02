"""Coordinator behavior tests with a minimal HA boundary, not a full HA runtime."""
import asyncio
import importlib.util
import logging
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "custom_components/4heat"


def module(name, **attrs):
    value = types.ModuleType(name)
    value.__dict__.update(attrs)
    sys.modules[name] = value
    return value


class FakeCoordinator:
    def __init__(self, hass, logger, **kwargs):
        self.data = None
        self.update_interval = kwargs['update_interval']
        self.listener_updates = 0
        self.shutdown_called = False

    def async_update_listeners(self):
        self.listener_updates += 1

    async def async_shutdown(self):
        self.shutdown_called = True


class HAError(Exception):
    pass


module('homeassistant')
module('homeassistant.const', CONF_HOST='host', CONF_MONITORED_CONDITIONS='monitored_conditions',
       UnitOfTemperature=types.SimpleNamespace(CELSIUS='°C'),
       UnitOfPressure=types.SimpleNamespace(MBAR='mbar', PA='Pa'),
       UnitOfVolumeFlowRate=types.SimpleNamespace(CUBIC_METERS_PER_HOUR='m³/h'))
module('homeassistant.exceptions', HomeAssistantError=HAError)
module('homeassistant.helpers')
module('homeassistant.helpers.update_coordinator', DataUpdateCoordinator=FakeCoordinator, UpdateFailed=HAError)
module('homeassistant.util')
module('homeassistant.util.dt', utcnow=lambda: datetime.now(timezone.utc))
package = module('fourheat_test')
package.__path__ = [str(ROOT)]
spec = importlib.util.spec_from_file_location('fourheat_test.coordinator', ROOT/'coordinator.py')
coordinator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(coordinator)


def status(state=5, water=65):
    return ['SEL', '3', f'J30001{state:012d}', 'J30002000000000000', f'J30017{water:012d}']


class CoordinatorTests(unittest.IsolatedAsyncioTestCase):
    def create(self, mode=False):
        c = coordinator.FourHeatDataUpdateCoordinator(None, config={
            'host':'test', 'mode':mode, 'monitored_conditions':status()[2:]}, options={}, id='existing-entry')
        c.transport.read_retry_delay = 0
        return c

    def reply(self, c, response):
        async def request(payload):
            if isinstance(response, Exception):
                raise response
            return response
        c.transport.request = request

    async def test_active_idle_and_fresh_snapshot(self):
        c=self.create()
        old={'30017':[40,'J']}
        c.data=old
        self.reply(c,status())
        data=await c._async_update_data()
        self.assertEqual(data['30017'],[65,'J'])
        self.assertEqual(old['30017'],[40,'J'])
        self.assertEqual(c.update_interval.total_seconds(),10)
        self.assertIsNotNone(c.last_successful_update)
        self.reply(c,status(0))
        await c._async_update_data()
        self.assertEqual(c.update_interval.total_seconds(),60)

    async def test_failure_not_fresh_and_backoff_recovers(self):
        c=self.create()
        self.reply(c,status())
        c.data=await c._async_update_data()
        last=c.last_successful_update
        self.reply(c,OSError('unreachable'))
        intervals=[]
        for _ in range(5):
            with self.assertRaises(HAError):
                await c._async_update_data()
            intervals.append(c.update_interval.total_seconds())
        self.assertEqual(intervals,[10,20,40,60,60])
        self.assertEqual(c.last_successful_update,last)
        self.reply(c,status())
        await c._async_update_data()
        self.assertEqual(c.update_interval.total_seconds(),10)

    async def test_missing_critical_data_is_failure(self):
        c=self.create()
        self.reply(c,['SEL','1','J30001000000000005'])
        with self.assertRaises(HAError):
            await c._async_update_data()

    async def test_rejected_or_unconfirmed_commands_not_retried(self):
        for response in [['ERR','0'],OSError('lost'),asyncio.TimeoutError()]:
            c=self.create()
            self.reply(c,response)
            with self.assertRaises(HAError):
                await c.async_turn_off()
            self.assertIsNone(c.last_command_accepted)

    async def test_command_acceptance_does_not_fabricate_state(self):
        c=self.create()
        c.data={'30001':[2,'J']}
        self.reply(c,['SEC','1','J30254000000000001'])
        self.assertTrue(await c.async_turn_off())
        self.assertEqual(c.data['30001'][0],2)

    async def test_no_legacy_unblock_and_bad_write_values(self):
        c=self.create(True)
        with self.assertRaises(HAError):
            await c.async_unblock()
        for bad in [True,-1,1.5,10**12]:
            with self.assertRaises(HAError):
                await c.async_set_value('20180',bad)

    async def test_idle_backoff_keeps_existing_cap(self):
        c = self.create()
        self.reply(c, status(0))
        c.data = await c._async_update_data()
        self.reply(c, OSError('lost'))
        intervals = []
        for _ in range(5):
            with self.assertRaises(HAError):
                await c._async_update_data()
            intervals.append(c.update_interval.total_seconds())
        self.assertEqual(intervals, [10, 20, 40, 80, 160])

    async def test_configured_active_backoff_cap(self):
        c = self.create()
        c.active_backoff_max = 20
        self.reply(c, OSError('lost'))
        for _ in range(5):
            with self.assertRaises(HAError):
                await c._async_update_data()
        self.assertEqual(c.update_interval.total_seconds(), 20)

    async def test_direct_telemetry_throttles_and_cache_is_not_called_fresh(self):
        c = self.create()
        c.direct_enabled = True
        c.direct_interval = 30
        calls = []
        fail = False
        async def request(payload):
            calls.append(payload)
            if payload == coordinator.DATA_QUERY:
                return status(water=60)
            if fail:
                raise OSError('board read unavailable')
            return ['SEC', '3', *status(6, 67)[2:]]
        c.transport.request = request
        c.data = await c._async_update_data()
        last = c.last_direct_update
        self.assertEqual(c.data['30017'][0], 67)
        self.assertEqual(c.core_source, 'direct_read')
        self.assertEqual(c.direct_read_status, 'ok')
        c.data = await c._async_update_data()
        self.assertEqual(c.data['30017'][0], 67)
        self.assertEqual(len(calls), 3)  # SEL+SEC, then only SEL.
        self.assertEqual(c.last_direct_update, last)
        fail = True
        c._direct_attempt = float('-inf')
        c.data = await c._async_update_data()
        self.assertEqual(c.data['30017'][0], 60)
        self.assertEqual(c.core_source, 'module_cache')
        self.assertEqual(c.direct_read_status, 'failed')
        self.assertEqual(c.last_direct_update, last)
        self.assertIsNotNone(c.last_successful_update)
        fail = False
        c._direct_attempt = float('-inf')
        c.data = await c._async_update_data()
        self.assertEqual(c.data['30017'][0], 67)
        self.assertEqual(c.direct_read_status, 'ok')

    async def test_explicit_fallback_retains_noncritical_fields(self):
        c = self.create()
        c.data = {'20180': [65, 'B']}
        async def request(payload):
            if payload == coordinator.DATA_QUERY:
                return ['ERR', '0']
            return ['SEC', '3', *status()[2:]]
        c.transport.request = request
        data = await c._async_update_data()
        self.assertEqual(data['20180'], [65, 'B'])
        self.assertEqual(data['30017'], [65, 'J'])
        self.assertEqual(c.core_source, 'direct_read')

    async def test_off_ack_and_extinguishing_are_not_shutdown_confirmation(self):
        import json
        c = self.create()
        c.data = {'30001': [2, 'J']}
        direct_state = 7
        writes = []
        async def request(payload):
            message = json.loads(payload)
            if payload == coordinator.DATA_QUERY:
                return status(0)  # An older cache must not confirm OFF.
            if message[2].startswith('J'):
                writes.append(payload)
                return message
            return ['SEC', '3', *status(direct_state)[2:]]
        c.transport.request = request
        await c.async_turn_off()
        await c.async_turn_off()
        self.assertEqual(len(writes), 1)
        self.assertEqual(c.last_command['status'], 'pending')
        self.assertEqual(c.last_command['acknowledgment'], 'accepted')
        c.data = await c._async_update_data()
        self.assertEqual(c.last_command['status'], 'pending')
        self.assertEqual(c.data['30001'][0], 7)
        direct_state = 0
        c.data = await c._async_update_data()
        self.assertEqual(c.last_command['status'], 'confirmed')
        self.assertIsNotNone(c.last_command['confirmed_at'])
        self.assertIsNone(c._pending)
        self.assertEqual(len(writes), 1)

    async def test_ambiguous_ack_can_be_observed_without_resending(self):
        c = self.create()
        writes = []
        async def request(payload):
            if payload == c._off_cmd:
                writes.append(payload)
                return ['SEC', '0']
            if payload == coordinator.DATA_QUERY:
                return status(5)
            return ['SEC', '3', *status(0)[2:]]
        c.transport.request = request
        with self.assertRaises(HAError):
            await c.async_turn_off()
        self.assertIsNone(c.last_command_accepted)
        await c.async_turn_off()
        c.data = await c._async_update_data()
        self.assertEqual(c.last_command['status'], 'confirmed')
        self.assertEqual(c.last_command['acknowledgment'], 'unconfirmed')
        self.assertEqual(len(writes), 1)

    async def test_pending_expires_without_retrying_or_fabricating_state(self):
        c = self.create()
        calls = []
        async def request(payload):
            calls.append(payload)
            return ['SEC', '1', 'J30254000000000001'] if payload == c._off_cmd else status(5)
        c.transport.request = request
        await c.async_turn_off()
        c._pending['deadline'] = 0
        c.data = await c._async_update_data()
        self.assertEqual(c.last_command['status'], 'unconfirmed')
        self.assertIsNone(c._pending)
        self.assertEqual(c.data['30001'][0], 5)
        self.assertEqual(calls.count(c._off_cmd), 1)

    async def test_set_value_needs_direct_register_readback(self):
        import json
        c = self.create()
        actual = 60
        reads = []
        async def request(payload):
            message = json.loads(payload)
            if payload == coordinator.DATA_QUERY:
                return ['SEL', '4', *status()[2:], 'B20180000000000065']
            if message[2].startswith('B'):
                return message
            reads.append(message[2:])
            return ['SEC', '4', *status()[2:], f'B20180{actual:012d}']
        c.transport.request = request
        await c.async_set_value('20180', 65)
        c.data = await c._async_update_data()
        self.assertEqual(c.last_command['status'], 'pending')
        self.assertEqual(c.data['20180'][0], 60)
        self.assertIn('A20180000000000000', reads[0])
        actual = 65
        c.data = await c._async_update_data()
        self.assertEqual(c.last_command['status'], 'confirmed')

    async def test_wrong_echo_and_rejection_never_count_as_acceptance(self):
        for response in [['SEC', '1', 'J30253000000000001'], ['SEC', '1', 'OK'], ['ERR', '0']]:
            c = self.create()
            self.reply(c, response)
            with self.assertRaises(HAError):
                await c.async_turn_off()
            self.assertIsNone(c.last_command_accepted)
            if response[0] == 'ERR':
                self.assertEqual(c.last_command['status'], 'rejected')
                self.assertIsNone(c._pending)

    async def test_on_and_unblock_confirm_only_observed_states(self):
        import json
        for action, state in [('async_turn_on', 2), ('async_unblock', 0)]:
            c = self.create()
            async def request(payload):
                message = json.loads(payload)
                if payload == coordinator.DATA_QUERY:
                    return status(9)
                if message[2].startswith('J'):
                    return message
                return ['SEC', '3', *status(state)[2:]]
            c.transport.request = request
            await getattr(c, action)()
            self.assertEqual(c.last_command['status'], 'pending')
            c.data = await c._async_update_data()
            self.assertEqual(c.last_command['status'], 'confirmed')

    async def test_old_inflight_read_cannot_confirm_a_superseding_command(self):
        import json
        c = self.create()
        entered, release = asyncio.Event(), asyncio.Event()
        first_read = True
        async def request(payload):
            nonlocal first_read
            message = json.loads(payload)
            if payload == coordinator.DATA_QUERY:
                return status(5)
            if message[2].startswith('J'):
                return message
            if first_read:
                first_read = False
                entered.set()
                await release.wait()
            return ['SEC', '3', *status(2)[2:]]
        c.transport.request = request
        await c.async_turn_off()
        poll = asyncio.create_task(c._async_update_data())
        await entered.wait()
        await c.async_turn_on()
        release.set()
        c.data = await poll
        self.assertEqual(c.last_command['action'], 'turn_on')
        self.assertEqual(c.last_command['status'], 'pending')
        c.data = await c._async_update_data()
        self.assertEqual(c.last_command['status'], 'confirmed')

    async def test_read_started_before_command_exchange_finishes_cannot_confirm(self):
        import json
        c = self.create()
        entered, release = asyncio.Event(), asyncio.Event()
        async def request(payload):
            message = json.loads(payload)
            if payload == c._on_cmd:
                entered.set()
                await release.wait()
                return message
            if payload == coordinator.DATA_QUERY:
                return status(2)
            return ['SEC', '3', *status(2)[2:]]
        c.transport.request = request
        command = asyncio.create_task(c.async_turn_on())
        await entered.wait()
        c.data = await c._async_update_data()
        self.assertEqual(c.last_command['status'], 'pending')
        release.set()
        await command
        self.assertEqual(c.last_command['status'], 'pending')
        c.data = await c._async_update_data()
        self.assertEqual(c.last_command['status'], 'confirmed')

    async def test_transient_primary_timeout_recovers_before_unavailable(self):
        c = self.create()
        calls = []
        async def request(payload):
            calls.append(payload)
            if len(calls) == 1:
                raise asyncio.TimeoutError()
            return status(water=64)
        c.transport.request = request
        data = await c._async_update_data()
        self.assertEqual(data['30017'][0], 64)
        self.assertEqual(c._failures, 0)
        self.assertEqual(len(calls), 2)
        self.assertEqual(c.communication_attributes['read_retries_total'], 1)

    async def test_malformed_primary_read_is_retried_without_publishing_bad_data(self):
        c = self.create()
        c.data = {'30001': [0, 'J'], '30017': [40, 'J']}
        calls = []
        async def request(payload):
            calls.append(payload)
            return ['SEL', '1', 'bad'] if len(calls) == 1 else status(5, 66)
        c.transport.request = request
        data = await c._async_update_data()
        self.assertEqual(data['30017'][0], 66)
        self.assertEqual(c.data['30017'][0], 40)
        self.assertEqual(len(calls), 2)
