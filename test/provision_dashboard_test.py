"""Run provisioning validation without writing host configuration or services."""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/provision_relayweave.sh'
DATA = ROOT / 'test/data'


class DashboardProvisionTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.work = Path(directory.name)
        for source, target in [
            ('tls_channel_test_ca.pem', 'server-ca.pem'),
            ('tls_channel_test_client_ca.pem', 'client-ca.pem'),
            ('tls_channel_test_client.pem', 'shared-client.pem'),
            ('tls_channel_test_client.key', 'shared-client.key'),
        ]:
            shutil.copyfile(DATA / source, self.work / target)
        self.config = self.work / 'dashboard.json'
        self.config.write_text(json.dumps({'server': {'host': '127.0.0.1'}}))

    def run_script(self, *extra, alias=False, role='dashboard'):
        if alias:
            executable = self.work / 'relayweave-provision-dashboard'
            executable.symlink_to(SCRIPT)
            command = ['bash', str(executable)]
        else:
            command = ['bash', str(SCRIPT), role]
        return subprocess.run(command + ['--cert-dir', str(self.work), '--config',
            str(self.config), '--dry-run', *extra], capture_output=True, text=True)

    def test_dashboard_and_installed_alias_validate_without_installing(self):
        for alias in [False, True]:
            with self.subTest(alias=alias):
                result = self.run_script(alias=alias)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn('Dry run complete', result.stdout)
                self.assertNotIn('Installed configuration', result.stdout)

    def test_mismatched_private_key_is_rejected(self):
        shutil.copyfile(DATA / 'tls_channel_test_server.key', self.work / 'shared-client.key')
        result = self.run_script()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('do not match', result.stderr)

    def test_untrusted_client_certificate_is_rejected(self):
        shutil.copyfile(DATA / 'tls_channel_test_ca.pem', self.work / 'client-ca.pem')
        result = self.run_script()
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn('Dry run complete', result.stdout)

    def test_missing_certificate_is_rejected(self):
        (self.work / 'shared-client.pem').unlink()
        result = self.run_script()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Required certificate file', result.stderr)

    def test_server_verification_option_is_rejected_for_dashboard(self):
        result = self.run_script('--verify-host', 'localhost')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('only to RelayNode', result.stderr)

    def test_agent_validation_still_works(self):
        result = self.run_script(role='agent')
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
