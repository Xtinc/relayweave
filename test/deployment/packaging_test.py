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


class MaintainerScriptsTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.work = Path(directory.name)
        self.calls = self.work / 'systemctl.calls'
        self.system_calls = self.work / 'system.calls'
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
    is-enabled) [ "$MOCK_ENABLED" = yes ] ;;
esac
''')
        systemctl.chmod(0o755)
        # Dashboard configuration must never create host users or directories.
        for command in ('getent', 'adduser', 'install'):
            mock = self.work / command
            mock.write_text('''#!/bin/sh
printf '%s\\n' "${0##*/} $*" >> "$MOCK_SYSTEM_CALLS"
if [ "${0##*/}" = getent ]; then
    [ "$MOCK_DASHBOARD_USER_EXISTS" = yes ]
fi
''')
            mock.chmod(0o755)

    def configure(self, role, *, previous_version=None, active=False, enabled=True,
                  ambient=True, bounding=True, unit_capability=True,
                  dashboard_user_exists=True):
        self.calls.write_text('')
        self.system_calls.write_text('')
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
                           MOCK_ENABLED='yes' if enabled else 'no',
                           MOCK_SYSTEM_CALLS=str(self.system_calls),
                           MOCK_DASHBOARD_USER_EXISTS='yes' if dashboard_user_exists else 'no',
                           MOCK_AMBIENT='cap_net_raw cap_net_bind_service' if ambient else '',
                           MOCK_BOUNDING='cap_net_raw cap_net_bind_service' if bounding else '')
        arguments = ['sh', str(postinst), 'configure']
        if previous_version is not None:
            arguments.append(previous_version)
        result = subprocess.run(arguments, env=environment, capture_output=True, text=True,
                                timeout=5)
        return result, self.calls.read_text().splitlines()

    def expected_reload_calls(self, role):
        calls = ['daemon-reload']
        if role in ('node', 'agent'):
            calls.extend([
                f'show relayweave-{role}.service --property=AmbientCapabilities --value',
                f'show relayweave-{role}.service --property=CapabilityBoundingSet --value',
            ])
        return calls

    def test_first_install_enables_without_starting(self):
        for role in ('node', 'agent', 'proxy', 'dashboard'):
            with self.subTest(role=role):
                result, calls = self.configure(role)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(calls, self.expected_reload_calls(role) +
                                 [f'enable relayweave-{role}.service'])

    def test_upgrade_and_reinstall_preserve_service_state(self):
        for role in ('node', 'agent', 'proxy', 'dashboard'):
            for previous_version in ('1.0.5-2', '1.0.5-3'):
                for active in (False, True):
                    for enabled in (False, True):
                        with self.subTest(role=role, previous_version=previous_version,
                                          active=active, enabled=enabled):
                            result, calls = self.configure(
                                role, previous_version=previous_version,
                                active=active, enabled=enabled)
                            self.assertEqual(result.returncode, 0, result.stderr)
                            # Only read/reload operations are allowed: no start,
                            # stop, restart, reload, enable or disable commands.
                            self.assertEqual(calls, self.expected_reload_calls(role))

    def test_dashboard_initializes_user_and_certificate_directory(self):
        for user_exists in (False, True):
            with self.subTest(user_exists=user_exists):
                result, _ = self.configure('dashboard', dashboard_user_exists=user_exists)
                self.assertEqual(result.returncode, 0, result.stderr)
                expected = ['getent passwd relayweave-dashboard']
                if not user_exists:
                    expected.append('adduser --system --group --home '
                                    '/var/lib/relayweave-dashboard --no-create-home '
                                    'relayweave-dashboard')
                expected.append('install -d -o root -g relayweave-dashboard -m 0750 '
                                '/etc/relayweave/dashboard-certs')
                self.assertEqual(self.system_calls.read_text().splitlines(), expected)

    def test_missing_packaged_capability_rejects_configuration(self):
        for role in ('node', 'agent'):
            with self.subTest(role=role):
                result, calls = self.configure(role, unit_capability=False)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('CAP_NET_RAW', result.stderr)
                self.assertEqual(calls, [])

    def test_override_removing_either_capability_rejects_configuration(self):
        for role in ('node', 'agent'):
            for missing in ('ambient', 'bounding'):
                with self.subTest(role=role, missing=missing):
                    result, calls = self.configure(role, previous_version='1.0.5-2', active=True,
                                                   **{missing: False})
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn('CAP_NET_RAW', result.stderr)
                    self.assertEqual(calls, self.expected_reload_calls(role))


if __name__ == '__main__':
    unittest.main()
