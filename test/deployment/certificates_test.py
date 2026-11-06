"""Exercise certificate packages and provisioning in isolated directories."""
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
GENERATOR = ROOT / 'scripts/generate_certificates.sh'
PROVISION = ROOT / 'scripts/provision_relayweave.sh'
NODE_FILES = {'server.pem', 'server.key', 'server-ca.pem', 'client-ca.pem'}
AGENT_FILES = {'shared-client.pem', 'shared-client.key', 'server-ca.pem'}
CA_FILES = {'server-ca.pem', 'server-ca.key', 'client-ca.pem', 'client-ca.key',
            'shared-client.pem', 'shared-client.key'}


def run(*args, **kwargs):
    return subprocess.run(args, capture_output=True, text=True, timeout=90, **kwargs)


@unittest.skipUnless(all(shutil.which(name) for name in ('openssl', 'tar', 'bash')),
                     'OpenSSL, tar and Bash are required')
class CertificatesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.directory.cleanup)
        cls.shared = Path(cls.directory.name)
        cls.first = cls.shared / 'first'
        cls.passwords = dict(os.environ, PROXY_SERVER_CA_PASSWORD='test-server-password',
                             PROXY_CLIENT_CA_PASSWORD='test-client-password')
        answers = ['1', str(cls.first), '', '', 'master-1',
                   '127.0.0.1,::1,node.example.test', 'slave-1', '192.0.2.3', '', 'y']
        result = run('bash', str(GENERATOR), input='\n'.join(answers) + '\n',
                     env=cls.passwords)
        if result.returncode:
            raise AssertionError(result.stdout + result.stderr)
        cls.node = cls.shared / 'node'
        cls.agent = cls.shared / 'agent'
        cls.authority = cls.shared / 'authority'
        for package, destination in (
                ('node-master-1.tar.gz', cls.node),
                ('client.tar.gz', cls.agent), ('ca-backup.tar.gz', cls.authority)):
            destination.mkdir()
            with tarfile.open(cls.first / package) as archive:
                archive.extractall(destination, filter='data')

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.work = Path(directory.name)
        self.tools = self.work / 'bin'
        self.tools.mkdir()
        self.target = self.work / 'certs'
        self.environment = dict(os.environ, PATH=f'{self.tools}:{os.environ["PATH"]}',
                                MOCK_WORK=str(self.work))
        # Rebase privileged writes and ownership operations to this test's
        # directory. No product test switches or real root privileges are used.
        real_tools = {name: shutil.which(name) for name in
                      ('install', 'mktemp', 'cp', 'mv', 'rm')}
        wrapper = self.tools / 'mock-tool'
        wrapper.write_text('''#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
work = pathlib.Path(os.environ['MOCK_WORK'])
name = pathlib.Path(sys.argv[0]).name
args = [arg.replace('/var/backups/relayweave', str(work / 'backups'))
        for arg in sys.argv[1:]]
with (work / 'commands').open('a') as log:
    log.write(json.dumps([name, *args]) + '\\n')
if name == 'id':
    print(0)
    sys.exit(0)
if name == 'systemctl':
    sys.exit('certificate operations must not call systemctl')
if name == 'relayweave-node':
    sys.exit(0)
if name == 'install':
    cleaned = []
    i = 0
    while i < len(args):
        if args[i] in ('-o', '-g'):
            i += 2
        else:
            cleaned.append(args[i])
            i += 1
    args = cleaned
failure = os.environ.get('MOCK_FAIL')
marker = work / 'failed-once'
if not marker.exists() and (
        (failure == 'mv' and name == 'mv' and args[-2].endswith('/1')) or
        (failure == 'rm' and name == 'rm' and args[0] == '--'
         and args[-1].endswith('/client-ca.pem'))):
    marker.touch()
    sys.exit(1)
tools = ''' + repr(real_tools) + '''
sys.exit(subprocess.run([tools[name], *args]).returncode)
''')
        wrapper.chmod(0o755)
        for name in (*real_tools, 'id', 'systemctl', 'relayweave-node'):
            (self.tools / name).symlink_to(wrapper)

    def provision(self, role='node', *options, answers=None, alias=False):
        executable = PROVISION
        prefix = [role]
        if alias:
            executable = self.tools / f'relayweave-provision-{role}'
            executable.symlink_to(PROVISION)
            prefix = []
        return run('bash', str(executable), *prefix, *options,
                   input=answers, env=self.environment)

    def install(self, role='node', archive=None, *options):
        archive = archive or self.first / ('client.tar.gz' if role == 'agent'
                                          else 'node-master-1.tar.gz')
        return self.provision(role, '--install-certs', str(archive),
                              '--cert-dir', str(self.target), *options)

    def commands(self):
        path = self.work / 'commands'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def snapshot(self):
        return {path.name: (path.read_bytes(), stat.S_IMODE(path.stat().st_mode))
                for path in self.target.iterdir() if path.is_file()}

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def make_archive(self, name, files):
        path = self.work / name
        with tarfile.open(path, 'w:gz') as archive:
            for filename, content in files:
                if isinstance(content, tarfile.TarInfo):
                    archive.addfile(content)
                else:
                    entry = tarfile.TarInfo(filename)
                    entry.size = len(content)
                    archive.addfile(entry, io.BytesIO(content))
        return path

    def test_packages_contain_only_runtime_files_and_encrypted_ca_keys(self):
        for package, expected in [('node-master-1.tar.gz', NODE_FILES),
                                  ('node-slave-1.tar.gz', NODE_FILES),
                                  ('client.tar.gz', AGENT_FILES),
                                  ('ca-backup.tar.gz', CA_FILES)]:
            with self.subTest(package=package):
                path = self.first / package
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                with tarfile.open(path) as archive:
                    self.assertEqual(set(archive.getnames()), expected)
                    self.assertEqual(len(archive.getmembers()), len(expected))
                    self.assertTrue(all(entry.isfile() for entry in archive))
                    for entry in archive:
                        self.assertEqual(entry.mode, 0o600 if entry.name.endswith('.key') else 0o644)
        for name in ('server-ca.key', 'client-ca.key'):
            self.assertIn(b'ENCRYPTED PRIVATE KEY', (self.authority / name).read_bytes())
        self.assertEqual(set(path.name for path in self.first.iterdir()),
                         {'node-master-1.tar.gz', 'node-slave-1.tar.gz',
                          'client.tar.gz', 'ca-backup.tar.gz'})

    def test_reuse_renewal_and_explicit_client_rotation(self):
        original_key = (self.agent / 'shared-client.key').read_bytes()
        for renew, rotate in ((False, False), (True, False), (True, True)):
            with self.subTest(renew=renew, rotate=rotate):
                output = self.work / f'reuse-{renew}-{rotate}'
                answers = ['2', str(self.first / 'ca-backup.tar.gz'), 'y' if renew else 'n']
                if renew:
                    answers.append('y' if rotate else 'n')
                answers += [str(output), '', 'packages' if not renew else '']
                if not renew:
                    answers += ['198.51.100.2', '']
                answers += ['y']
                result = run('bash', str(GENERATOR), input='\n'.join(answers) + '\n',
                             env=self.passwords)
                self.assert_success(result)
                with tarfile.open(output / 'client.tar.gz') as archive:
                    key = archive.extractfile('shared-client.key').read()
                    certificate = archive.extractfile('shared-client.pem').read()
                    self.assertEqual(archive.extractfile('server-ca.pem').read(),
                                     (self.agent / 'server-ca.pem').read_bytes())
                self.assertEqual(key == original_key, not rotate)
                self.assertEqual(certificate == (self.agent / 'shared-client.pem').read_bytes(),
                                 not renew)
                if not renew:
                    with tarfile.open(output / 'node-packages.tar.gz') as archive:
                        self.assertEqual(archive.extractfile('server-ca.pem').read(),
                                         (self.node / 'server-ca.pem').read_bytes())

    def test_wrong_ca_password_does_not_publish_output(self):
        output = self.work / 'bad-password'
        answers = ['2', str(self.first / 'ca-backup.tar.gz'), 'n', str(output), '', '', 'y']
        environment = dict(self.passwords, PROXY_SERVER_CA_PASSWORD='wrong')
        result = run('bash', str(GENERATOR), input='\n'.join(answers) + '\n', env=environment)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())
        self.assertFalse(list(self.work.glob('.generate-certificates.*')))

    def test_leaf_validity_cannot_exceed_ca_validity(self):
        output = self.work / 'too-long'
        answers = ['2', str(self.first / 'ca-backup.tar.gz'), 'y', 'n',
                   str(output), '36500', '', 'y']
        result = run('bash', str(GENERATOR), input='\n'.join(answers) + '\n', env=self.passwords)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('CA expires too soon', result.stderr)
        self.assertFalse(output.exists())

    def test_generation_refuses_existing_output_and_reprompts_invalid_hosts(self):
        link = self.work / 'output-link'
        link.symlink_to(self.work / 'missing')
        for output in (self.first, link):
            result = run('bash', str(GENERATOR), input=f'1\n{output}\n', env=self.passwords)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('Refusing to overwrite', result.stderr)
        self.assertFalse((self.work / 'missing').exists())
        # Cancel before key generation: exercise input validation without costly RSA work.
        answers = ['1', str(self.work / 'cancelled'), '', '',
                   'node', '999.1.1.1', 'node', ':::1', 'node', 'bad..name',
                   'node', '127.0.0.1,127.0.0.1', 'node', '::1', '', 'n']
        result = run('bash', str(GENERATOR), input='\n'.join(answers) + '\n', env=self.passwords)
        self.assert_success(result)
        self.assertEqual(result.stderr.count('Invalid or duplicate addresses'), 4)
        self.assertFalse((self.work / 'cancelled').exists())

    def test_node_install_update_and_permissions_without_service_or_config_changes(self):
        result = self.install()
        self.assert_success(result)
        self.assertIn('IP Address:127.0.0.1', result.stdout)
        self.assertEqual(set(self.snapshot()), NODE_FILES)
        self.assertEqual(self.snapshot()['server.key'][1], 0o600)
        self.assertTrue(all(mode == 0o644 for name, (_, mode) in self.snapshot().items()
                            if name.endswith('.pem')))
        before = self.snapshot()
        (self.target / 'unrelated').write_text('keep')
        config = self.work / 'node.json'
        config.write_text('do not touch')
        self.assert_success(self.install())
        backups = list((self.work / 'backups').iterdir())
        self.assertEqual(len(backups), 1)
        self.assertEqual(set(p.name for p in backups[0].iterdir()), NODE_FILES)
        for name, (contents, _) in before.items():
            self.assertEqual((backups[0] / name).read_bytes(), contents)
        self.assertEqual(config.read_text(), 'do not touch')
        self.assertEqual((self.target / 'unrelated').read_text(), 'keep')
        self.assertNotIn('systemctl', [cmd[0] for cmd in self.commands()])
        self.assertNotIn('relayweave-node', [cmd[0] for cmd in self.commands()])

    def test_agent_requires_and_installs_only_three_files(self):
        self.assert_success(self.install('agent'))
        self.assertEqual(set(self.snapshot()), AGENT_FILES)
        self.assertEqual(self.snapshot()['shared-client.key'][1], 0o600)

    def test_interactive_alias_and_source_entry(self):
        answers = f'1\n{self.first / "client.tar.gz"}\n{self.target}\ny\n'
        self.assert_success(self.provision('agent', answers=answers, alias=True))
        self.assertEqual(set(self.snapshot()), AGENT_FILES)
        dry_target = self.work / 'dry'
        result = run('bash', str(PROVISION), '--dry-run',
                     input=f'1\n1\n{self.first / "node-master-1.tar.gz"}\n{dry_target}\n',
                     env=self.environment)
        self.assert_success(result)
        self.assertFalse(dry_target.exists())

    def test_dry_run_has_no_destination_backup_or_service_side_effects(self):
        self.assert_success(self.install('node', None, '--dry-run'))
        self.assertFalse(self.target.exists())
        self.assertFalse((self.work / 'backups').exists())
        self.assertFalse({'install', 'mv', 'systemctl'} & {cmd[0] for cmd in self.commands()})

    def test_remove_backs_up_preserves_shared_ca_and_is_idempotent(self):
        self.assert_success(self.install())
        self.assert_success(self.install('agent'))
        (self.target / 'unrelated').write_text('keep')
        self.assert_success(self.provision('node', '--remove-certs', '--cert-dir', str(self.target)))
        self.assertEqual(set(self.snapshot()), AGENT_FILES | {'unrelated'})
        self.assert_success(self.provision('node', '--remove-certs', '--cert-dir', str(self.target)))
        self.assert_success(self.provision('agent', '--remove-certs', '--cert-dir', str(self.target)))
        self.assertEqual(set(self.snapshot()), {'unrelated'})
        self.assertNotIn('systemctl', [cmd[0] for cmd in self.commands()])

    def test_remove_agent_first_preserves_node_files(self):
        self.assert_success(self.install())
        self.assert_success(self.install('agent'))
        self.assert_success(self.provision('agent', '--remove-certs', '--cert-dir', str(self.target)))
        self.assertEqual(set(self.snapshot()), NODE_FILES)
        self.assert_success(self.provision('node', '--remove-certs', '--cert-dir', str(self.target)))
        self.assertEqual(self.snapshot(), {})

    def test_install_and_remove_failures_restore_old_files_and_permissions(self):
        for operation in ('mv', 'rm'):
            with self.subTest(operation=operation):
                self.assert_success(self.install())
                ca = self.target / 'server-ca.pem'
                ca.write_bytes(ca.read_bytes() + b'\n# previous certificate bundle\n')
                ca.chmod(0o600)
                before = self.snapshot()
                (self.work / 'failed-once').unlink(missing_ok=True)
                self.environment['MOCK_FAIL'] = operation
                if operation == 'mv':
                    result = self.install()
                else:
                    result = self.provision('node', '--remove-certs',
                                            '--cert-dir', str(self.target))
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.snapshot(), before)
                self.assertFalse(list(self.target.glob('.provision.*')))
                del self.environment['MOCK_FAIL']

    def test_failed_first_install_removes_new_files(self):
        self.environment['MOCK_FAIL'] = 'mv'
        self.assertNotEqual(self.install().returncode, 0)
        self.assertEqual(self.snapshot(), {})

    def test_archive_rejects_extra_missing_duplicate_paths_and_links(self):
        files = [(name, (self.agent / name).read_bytes()) for name in sorted(AGENT_FILES)]
        cases = [files + [('extra', b'no')], files[:-1], files + [files[0]],
                 files + [('../escape', b'no')], files + [('/absolute', b'no')],
                 files + [('./server-ca.pem', b'no')]]
        for link_type in (tarfile.SYMTYPE, tarfile.LNKTYPE):
            entry = tarfile.TarInfo('shared-client.key')
            entry.type = link_type
            entry.linkname = '/etc/passwd'
            cases.append([(name, data) for name, data in files if name != entry.name]
                         + [(entry.name, entry)])
        for i, contents in enumerate(cases):
            with self.subTest(case=i):
                archive = self.make_archive(f'bad-{i}.tar.gz', contents)
                self.assertNotEqual(self.install('agent', archive, '--dry-run').returncode, 0)
                self.assertFalse(self.target.exists())

    def test_ca_backup_cannot_be_installed_as_deployment_package(self):
        self.assertNotEqual(self.install('node', self.first / 'ca-backup.tar.gz').returncode, 0)
        self.assertFalse(self.target.exists())

    def test_corrupt_archive_bad_identity_and_mismatched_key_are_rejected(self):
        corrupt = self.work / 'corrupt.tar.gz'
        corrupt.write_bytes(b'not an archive')
        self.assertNotEqual(self.install('node', corrupt, '--dry-run').returncode, 0)
        self.assert_success(self.install())
        before = self.snapshot()
        result = self.install('node', self.first / 'node-slave-1.tar.gz')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('identity mismatch', result.stderr)
        self.assertEqual(self.snapshot(), before)
        files = [(name, (self.agent / name).read_bytes()) for name in sorted(AGENT_FILES)]
        files = [(name, (self.node / 'server.key').read_bytes() if name.endswith('.key') else data)
                 for name, data in files]
        result = self.install('agent', self.make_archive('bad-key.tar.gz', files), '--dry-run')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('do not match', result.stderr)

    def test_symlink_destination_is_rejected(self):
        real = self.work / 'real'
        real.mkdir()
        self.target.symlink_to(real)
        self.assertNotEqual(self.install().returncode, 0)
        self.assertEqual(list(real.iterdir()), [])

    def test_expired_certificate_and_wrong_client_purpose_are_rejected(self):
        csr = self.work / 'client.csr'
        self.assert_success(run('openssl', 'req', '-new', '-batch', '-key',
                                str(self.agent / 'shared-client.key'), '-subj',
                                '/CN=client', '-out', str(csr)))
        for purpose, days in (('clientAuth', '0'), ('serverAuth', '365')):
            with self.subTest(purpose=purpose, days=days):
                extensions = self.work / 'leaf.ext'
                extensions.write_text(f'extendedKeyUsage={purpose}\nbasicConstraints=CA:FALSE\n')
                certificate = self.work / 'client.pem'
                self.assert_success(run('openssl', 'x509', '-req', '-in', str(csr),
                    '-CA', str(self.authority / 'client-ca.pem'), '-CAkey',
                    str(self.authority / 'client-ca.key'), '-passin',
                    'env:PROXY_CLIENT_CA_PASSWORD', '-set_serial', '0x123456',
                    '-days', days, '-extfile', str(extensions), '-out', str(certificate),
                    env=self.passwords))
                files = [(name, certificate.read_bytes() if name == 'shared-client.pem'
                          else (self.agent / name).read_bytes()) for name in sorted(AGENT_FILES)]
                package = self.make_archive(f'bad-{purpose}.tar.gz', files)
                result = self.install('agent', package, '--dry-run')
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('expired' if days == '0' else 'purpose', result.stderr)
                if days == '0':
                    backup = self.make_archive('expired-ca-backup.tar.gz', [
                        (name, certificate.read_bytes() if name == 'shared-client.pem'
                         else (self.authority / name).read_bytes()) for name in sorted(CA_FILES)])
                    output = self.work / 'renewed-expired'
                    answers = ['2', str(backup), 'y', 'n', str(output), '', '', 'y']
                    result = run('bash', str(GENERATOR), input='\n'.join(answers) + '\n',
                                 env=self.passwords)
                    self.assert_success(result)
                    with tarfile.open(output / 'client.tar.gz') as archive:
                        self.assertEqual(archive.extractfile('shared-client.key').read(),
                                         (self.agent / 'shared-client.key').read_bytes())

    @unittest.skipIf(os.geteuid() == 0, 'test requires an unprivileged user')
    def test_actual_install_requires_root(self):
        environment = dict(os.environ)
        result = run('bash', str(PROVISION), 'node', '--install-certs',
                     str(self.first / 'node-master-1.tar.gz'), '--cert-dir', str(self.target),
                     env=environment)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('must run as root', result.stderr)
        self.assertFalse(self.target.exists())

    def test_legacy_agent_node_and_proxy_dry_run_interfaces(self):
        config = self.work / 'config.json'
        config.write_text('{}')
        for role, cert_dir in (('agent', self.agent), ('node', self.node), ('proxy', None)):
            with self.subTest(role=role):
                options = ['--config', str(config), '--dry-run']
                if cert_dir:
                    options += ['--cert-dir', str(cert_dir)]
                self.assert_success(self.provision(role, *options))


if __name__ == '__main__':
    unittest.main()
