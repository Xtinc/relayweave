"""Check Debian package contents and maintainer scripts without installing them."""
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(all(shutil.which(tool) for tool in ('cmake', 'cpack', 'dpkg-deb')),
                     'Debian packaging tools are required')
class DebianControlFilesTest(unittest.TestCase):
    def test_crlf_checkout_produces_executable_lf_maintainer_scripts(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            source = work / 'source'
            build = work / 'build'
            shutil.copytree(ROOT / 'packaging', source / 'packaging')
            shutil.copytree(ROOT / 'docs', source / 'docs')
            shutil.copytree(ROOT / 'cmake', source / 'cmake')
            shutil.copytree(ROOT / 'dashboard', source / 'dashboard',
                            ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
            (source / 'scripts').mkdir()
            shutil.copyfile(ROOT / 'scripts' / 'provision_relayweave.sh',
                            source / 'scripts' / 'provision_relayweave.sh')
            components = ('agent', 'node', 'proxy', 'dashboard')
            controls = ('conffiles', 'postinst', 'prerm', 'postrm')
            for component in components:
                for name in controls:
                    path = source / 'packaging' / component / name
                    path.write_bytes(path.read_bytes().replace(b'\r\n', b'\n')
                                     .replace(b'\n', b'\r\n'))
            launcher = source / 'packaging' / 'dashboard' / 'relayweave-dashboard'
            launcher.write_bytes(launcher.read_bytes().replace(b'\r\n', b'\n')
                                 .replace(b'\n', b'\r\n'))
            # Exercise the real packaging rules with a tiny payload instead of
            # compiling C++. No packages or services are installed on the host.
            (source / 'payload').write_text('test payload\n')
            (source / 'CMakeLists.txt').write_text('''cmake_minimum_required(VERSION 3.21)
project(relayweave VERSION 1.0.4 LANGUAGES NONE)
foreach(component IN ITEMS agent node proxy dashboard)
    install(FILES payload DESTINATION usr/share/relayweave COMPONENT ${component})
endforeach()
add_subdirectory(dashboard)
include(cmake/Packaging.cmake)
''')
            for command in (
                ['cmake', '-S', str(source), '-B', str(build)],
                ['cpack', '--config', str(build / 'CPackConfig.cmake'), '-G', 'DEB'],
            ):
                result = subprocess.run(command, cwd=work, capture_output=True, text=True,
                                        timeout=20)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            packages = list((build / 'packages').glob('*.deb'))
            self.assertEqual(len(packages), len(components))
            for component in components:
                with self.subTest(component=component):
                    package, = (build / 'packages').glob(f'relayweave-{component}_*.deb')
                    extracted = work / component
                    subprocess.run(['dpkg-deb', '--control', str(package), str(extracted)],
                                   check=True, capture_output=True, timeout=10)
                    for name in controls:
                        with self.subTest(control=name):
                            path = extracted / name
                            expected = (ROOT / 'packaging' / component / name).read_bytes()
                            self.assertEqual(path.read_bytes(), expected.replace(b'\r\n', b'\n'))
                            if name != 'conffiles':
                                self.assertEqual(path.stat().st_mode & 0o777, 0o755)
                                # An unused action skips service operations and
                                # lets the kernel resolve the packaged shebang.
                                result = subprocess.run([str(path), 'test-noop'],
                                                        capture_output=True, timeout=5)
                                self.assertEqual(result.returncode, 0, result.stderr)
            dashboard_package, = (build / 'packages').glob('relayweave-dashboard_*.deb')
            payload = work / 'dashboard-payload'
            subprocess.run(['dpkg-deb', '--extract', str(dashboard_package), str(payload)],
                           check=True, capture_output=True, timeout=10)
            installed_launcher = payload / 'usr' / 'bin' / 'relayweave-dashboard'
            expected = (ROOT / 'packaging' / 'dashboard' / 'relayweave-dashboard').read_bytes()
            self.assertEqual(installed_launcher.read_bytes(), expected.replace(b'\r\n', b'\n'))
            self.assertEqual(installed_launcher.stat().st_mode & 0o777, 0o755)


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
