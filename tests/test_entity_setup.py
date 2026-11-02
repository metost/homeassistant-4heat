"""Real duplicate SEL fixture must create each legacy sensor exactly once."""
import importlib.util
import json
from pathlib import Path
import types
import unittest
from test_coordinator import ROOT
# Establish the same minimal entity boundary used by diagnostic tests.
from test_diagnostics import FakeEntity

spec = importlib.util.spec_from_file_location('fourheat_test.sensor', ROOT/'sensor.py')
sensor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sensor)


class EntitySetupTests(unittest.IsolatedAsyncioTestCase):
    async def test_duplicate_monitor_register_preserves_one_original_entity(self):
        packet = json.loads((Path(__file__).parent/'status_fixture.json').read_text())
        coord = types.SimpleNamespace(stove_id='entry', serial_number='1', model='Basic',
                                      supplemental_enabled=False)
        entry = types.SimpleNamespace(entry_id='entry', title='Stove',
                                      data={'monitored_conditions':packet[2:]})
        hass = types.SimpleNamespace(data={sensor.DOMAIN:{'entry':{sensor.DATA_COORDINATOR:coord}}})
        added = []
        await sensor.async_setup_entry(hass, entry, added.extend)
        self.assertEqual(len(packet[2:]), 18)
        self.assertEqual(len(added), 17)
        self.assertEqual(sum(e.type == '20211' for e in added), 1)
        self.assertEqual(next(e for e in added if e.type == '20211').unique_id, 'Stove_20211')

    async def test_legacy_sensor_waits_for_its_first_noncritical_read(self):
        coord = types.SimpleNamespace(stove_id='entry', serial_number='1', model='Basic',
                                      last_update_success=True, data={'30001':[0,'J']})
        entity = sensor.FourHeatDevice(coord, '30005', 'Stove')
        self.assertFalse(entity.available)
        self.assertIsNone(entity.state)
        self.assertIsNone(entity.maker)
        self.assertIsNone(entity.extra_state_attributes)
        coord.data['30005'] = [26,'J']
        self.assertTrue(entity.available)
        self.assertEqual(entity.state,26)
        self.assertEqual(entity.maker,'J')
        self.assertIsNotNone(entity.extra_state_attributes)
        coord.last_update_success = False
        self.assertFalse(entity.available)
