"""Freeze original entity and state naming contracts against accidental migration."""
import ast
import copy
import textwrap
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class CompatibilityTests(unittest.TestCase):
    def test_original_entity_identity_and_state_mappings(self):
        expected = json.loads((ROOT/'tests/identity_contract.json').read_text())
        for filename, methods in expected.items():
            tree = ast.parse((ROOT/'custom_components/4heat'/filename).read_text())
            for name, frozen in methods.items():
                matches = [n for n in ast.walk(tree) if
                    (isinstance(n, ast.FunctionDef) and n.name == name) or
                    (isinstance(n, ast.Assign) and any(
                        isinstance(t, ast.Name) and t.id == name for t in n.targets))]
                with self.subTest(file=filename, name=name):
                    self.assertEqual(len(matches), 1)
                    actual = copy.deepcopy(matches[0])
                    if isinstance(actual, ast.FunctionDef):
                        self.assertEqual([d.id for d in actual.decorator_list], ['property'])
                        actual.decorator_list = []
                    baseline = ast.parse(textwrap.dedent(frozen)).body[0]
                    self.assertEqual(ast.dump(actual, include_attributes=False),
                                     ast.dump(baseline, include_attributes=False))

    def test_domain_and_config_flow_version_preserved(self):
        manifest = json.loads((ROOT/'custom_components/4heat/manifest.json').read_text())
        self.assertEqual(manifest['domain'], '4heat')
        tree = ast.parse((ROOT/'custom_components/4heat/config_flow.py').read_text())
        versions = [n.value.value for n in ast.walk(tree) if isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == 'VERSION' for t in n.targets)]
        self.assertEqual(versions, [1])
