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
