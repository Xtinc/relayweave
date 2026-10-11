"""Check Node/Agent maintainer scripts with temporary units and a fake systemctl."""
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class IcmpPackagingTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.work = Path(directory.name)
        self.calls = self.work / 'systemctl.calls'
        systemctl = self.work / 'systemctl'
        systemctl.write_text('''#!/bin/sh
printf '%s\\n' "$*" >> "$MOCK_CALLS"
case "$1" in
    show)
        case "$3" in
            --property=AmbientCapabilities) printf '%s\\n' "$MOCK_AMBIENT" ;;
            --property=CapabilityBoundingSet) printf '%s\\n' "$MOCK_BOUNDING" ;;
        esac ;;
    is-active) [ "$MOCK_ACTIVE" = yes ] ;;
esac
''')
        systemctl.chmod(0o755)

    def configure(self, role, *, upgrade=False, active=False, ambient=True,
                  bounding=True, unit_capability=True):
        self.calls.write_text('')
        source = ROOT / 'packaging' / role
        unit = (source / f'relayweave-{role}.service').read_text()
        if not unit_capability:
            unit = unit.replace('CAP_NET_RAW', '')
        (self.work / f'relayweave-{role}.service').write_text(unit)
        # Redirect only the package's fixed unit location; all capability checks
        # and lifecycle decisions run from the actual maintainer script.
        script = (source / 'postinst').read_text().replace(
            'unit_file=/lib/systemd/system/$service_name',
            f'unit_file={shlex.quote(str(self.work))}/$service_name')
        postinst = self.work / 'postinst'
        postinst.write_text(script)
        environment = dict(os.environ, PATH=f'{self.work}{os.pathsep}{os.environ["PATH"]}',
                           MOCK_CALLS=str(self.calls), MOCK_ACTIVE='yes' if active else 'no',
                           MOCK_AMBIENT='cap_net_raw cap_net_bind_service' if ambient else '',
                           MOCK_BOUNDING='cap_net_raw cap_net_bind_service' if bounding else '')
        arguments = ['sh', str(postinst), 'configure']
        if upgrade:
            arguments.append('1.0.1-1')
        result = subprocess.run(arguments, env=environment, capture_output=True, text=True)
        return result, self.calls.read_text().splitlines()

    def test_first_install_enables_without_starting(self):
        for role in ('node', 'agent'):
            with self.subTest(role=role):
                result, calls = self.configure(role)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f'enable relayweave-{role}.service', calls)
                self.assertFalse(any(call.startswith(('start ', 'restart ')) for call in calls))

    def test_upgrade_restarts_only_active_service(self):
        for role in ('node', 'agent'):
            for active in (False, True):
                with self.subTest(role=role, active=active):
                    result, calls = self.configure(role, upgrade=True, active=active)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(f'restart relayweave-{role}.service' in calls, active)
                    self.assertFalse(any(call.startswith('enable ') for call in calls))

    def test_missing_packaged_capability_rejects_configuration(self):
        for role in ('node', 'agent'):
            with self.subTest(role=role):
                result, calls = self.configure(role, unit_capability=False)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('CAP_NET_RAW', result.stderr)
                self.assertEqual(calls, [])

    def test_override_removing_either_capability_rejects_before_restart(self):
        for role in ('node', 'agent'):
            for missing in ('ambient', 'bounding'):
                with self.subTest(role=role, missing=missing):
                    result, calls = self.configure(role, upgrade=True, active=True,
                                                   **{missing: False})
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn('CAP_NET_RAW', result.stderr)
                    self.assertFalse(any(call.startswith(('enable ', 'restart ')) for call in calls))


if __name__ == '__main__':
    unittest.main()
