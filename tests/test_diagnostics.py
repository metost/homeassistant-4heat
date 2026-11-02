"""Verified supplemental packets, typed decoding and optional failure isolation."""
import asyncio
import importlib.util
from pathlib import Path
import types
import unittest
from test_coordinator import coordinator, status, module, HAError
from fourheat_test.supplemental import decode_supplemental, SUPPLEMENTAL_QUERY
from fourheat_test.transport import ProtocolError

EXTRA = ["SEC", "3", "J30011000000000009", "J50010000000000050",
         "J30025000000000750"]


class FakeEntity:
    def __init__(self, coordinator):
        self.coordinator = coordinator


module('homeassistant.components')
module('homeassistant.components.sensor', SensorEntity=type('SensorEntity', (), {}))
module('homeassistant.helpers.entity', EntityCategory=types.SimpleNamespace(DIAGNOSTIC='diagnostic'))
# Keep the coordinator stub, adding only the entity boundary needed here.
import sys
sys.modules['homeassistant.helpers.update_coordinator'].CoordinatorEntity = FakeEntity
path = Path(__file__).resolve().parents[1] / 'custom_components/4heat/diagnostic_sensor.py'
spec = importlib.util.spec_from_file_location('fourheat_test.diagnostic_sensor', path)
entities = importlib.util.module_from_spec(spec)
spec.loader.exec_module(entities)


class DecoderTests(unittest.TestCase):
    def test_real_values_and_separate_signed_decoder(self):
        decoded = decode_supplemental(EXTRA)
        self.assertEqual(decoded['30011']['value'], 9)
        self.assertEqual(decoded['50010']['value'], .5)
        self.assertEqual(decoded['30025']['value'], 750)
        negative = list(EXTRA)
        raw = list(negative[3]); raw[10] = '1'; negative[3] = ''.join(raw)
        raw = list(negative[4]); raw[12] = '1'; negative[4] = ''.join(raw)
        decoded = decode_supplemental(negative)
        self.assertEqual(decoded['50010']['value'], -.5)
        self.assertEqual(decoded['30025']['value'], -750)
        # Existing entity decoding stays byte-for-byte compatible.
        self.assertEqual(coordinator.decode_registers(EXTRA)['50010'][0], 50)

    def test_reject_missing_duplicate_wrong_marker_and_bad_count(self):
        for bad in [['SEC','0'], ['ERR','0'], ['SEC','2',*EXTRA[2:]],
                    ['SEC','3',EXTRA[2],EXTRA[2],EXTRA[4]],
                    ['SEC','3',EXTRA[2],EXTRA[3].replace('J','B'),EXTRA[4]]]:
            with self.subTest(bad=bad), self.assertRaises(ProtocolError):
                decode_supplemental(bad)


class SupplementalTests(unittest.IsolatedAsyncioTestCase):
    def create(self, enabled=True):
        c = coordinator.FourHeatDataUpdateCoordinator(None, config={
            'host':'test', 'monitored_conditions':status()[2:]},
            options={'supplemental_telemetry':enabled}, id='existing-entry')
        c.transport.read_retry_delay = 0
        return c

    async def test_opt_in_throttling_and_optional_failure_recovery(self):
        c = self.create(); calls=[]; fail=False
        async def request(payload):
            calls.append(payload)
            if payload == SUPPLEMENTAL_QUERY:
                if fail: raise OSError('optional unavailable')
                return EXTRA
            return status()
        c.transport.request = request
        c.data = await c._async_update_data()
        await c._supplemental_task
        self.assertEqual(c.supplemental_data['30011']['value'],9)
        await c._async_update_data()
        self.assertEqual(calls.count(SUPPLEMENTAL_QUERY),1)
        fail=True; c._supplemental_attempt=float('-inf')
        snapshot=await c._async_update_data()
        await c._supplemental_task
        self.assertEqual(snapshot['30017'][0],65)
        self.assertEqual(c.supplemental_data,{})
        self.assertIsNone(c.supplemental_updated_at)
        fail=False; c._supplemental_attempt=float('-inf')
        await c._async_update_data()
        await c._supplemental_task
        self.assertIsNotNone(c.supplemental_updated_at)

    async def test_default_profile_off_and_primary_failure_still_fails(self):
        c = self.create(False); calls=[]
        async def request(payload): calls.append(payload); return status()
        c.transport.request=request
        await c._async_update_data()
        self.assertEqual(len(calls),1)
        async def broken(payload): raise OSError('normal telemetry lost')
        c.transport.request=broken
        with self.assertRaises(HAError): await c._async_update_data()

    async def test_entities_distinguish_auto_modulation_and_stale_fields(self):
        c=self.create(); c.last_update_success=True
        c.data={'20813':[6,'B'], '20211':[5,'B']}
        c.fresh_registers=set(c.data)
        c.supplemental_data=decode_supplemental(EXTRA)
        c.supplemental_updated_at=coordinator.utcnow()
        entry=types.SimpleNamespace(title='Existing Stove',entry_id='existing-entry')
        selected=entities.FourHeatDiagnosticSensor(c,entry,'selected_power')
        actual=entities.FourHeatDiagnosticSensor(c,entry,'30011')
        auger=entities.FourHeatDiagnosticSensor(c,entry,'50010')
        self.assertEqual(selected.native_value,'Auto')
        self.assertEqual(actual.native_value,'P Mod')
        self.assertEqual(auger.native_value,.5)
        self.assertEqual(auger.extra_state_attributes['raw_register'],EXTRA[3])
        c.fresh_registers={'30001','30002','30017'}
        self.assertFalse(selected.available)
        self.assertIsNone(selected.native_value)
        c.supplemental_data={}
        self.assertFalse(actual.available)
        self.assertIsNone(actual.native_value)
        c.last_update_success=False
        self.assertFalse(auger.available)

    async def test_slow_diagnostics_do_not_delay_core_publication_and_unload_cancels(self):
        c = self.create()
        entered = asyncio.Event()
        never = asyncio.Event()
        async def request(payload):
            if payload == SUPPLEMENTAL_QUERY:
                entered.set()
                await never.wait()
            return status()
        c.transport.request = request
        c.data = await asyncio.wait_for(c._async_update_data(), .2)
        self.assertEqual(c.data['30017'][0], 65)
        await asyncio.wait_for(entered.wait(), .2)
        task = c._supplemental_task
        self.assertFalse(task.done())
        await c.async_shutdown()
        self.assertTrue(task.cancelled())
        self.assertTrue(c.shutdown_called)
        self.assertIsNone(c._supplemental_task)
        c._schedule_supplemental()
        self.assertIsNone(c._supplemental_task)

    async def test_background_result_notifies_without_overwriting_new_core_snapshot(self):
        c = self.create()
        entered, release = asyncio.Event(), asyncio.Event()
        async def request(payload):
            if payload == SUPPLEMENTAL_QUERY:
                entered.set()
                await release.wait()
                return EXTRA
            return status()
        c.transport.request = request
        c.data = await c._async_update_data()
        await entered.wait()
        c.data = {'30001': [6, 'J'], '30017': [69, 'J']}
        release.set()
        await c._supplemental_task
        self.assertEqual(c.data['30017'][0], 69)
        self.assertEqual(c.listener_updates, 1)
        self.assertEqual(c.supplemental_data['30011']['value'], 9)

    async def test_unload_before_background_task_starts_clears_reference(self):
        c = self.create()
        async def request(payload):
            return status()
        c.transport.request = request
        c.data = await c._async_update_data()
        task = c._supplemental_task
        await c.async_shutdown()
        self.assertTrue(task.cancelled())
        self.assertIsNone(c._supplemental_task)

    async def test_single_optional_timeout_recovers_without_unavailable_notification(self):
        c = self.create()
        attempts = 0
        async def request(payload):
            nonlocal attempts
            if payload == SUPPLEMENTAL_QUERY:
                attempts += 1
                if attempts == 1:
                    raise asyncio.TimeoutError()
                return EXTRA
            return status()
        c.transport.request = request
        c.data = await c._async_update_data()
        await c._supplemental_task
        self.assertEqual(attempts, 2)
        self.assertEqual(c.listener_updates, 1)
        self.assertEqual(c.supplemental_data['30011']['value'], 9)
        self.assertIsNotNone(c.supplemental_updated_at)
