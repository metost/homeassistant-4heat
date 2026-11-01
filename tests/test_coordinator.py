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
        return coordinator.FourHeatDataUpdateCoordinator(None, config={
            'host':'test', 'mode':mode, 'monitored_conditions':status()[2:]}, options={}, id='existing-entry')

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
        self.assertEqual(intervals,[60,120,240,300,300])
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
