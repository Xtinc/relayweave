import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(shutil.which('cmake') and os.name == 'posix',
                     'CMake and a native Unix toolchain are required')
class OpenSSLBuildTest(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory(prefix='relayweave-openssl-')
        self.addCleanup(self.workspace.cleanup)
        self.source = Path(self.workspace.name) / 'source'
        self.source.mkdir()
        (self.source / 'vendor').mkdir()
        shutil.copy(ROOT / 'cmake/OpenSSL.cmake', self.source / 'OpenSSL.cmake')
        (self.source / 'CMakeLists.txt').write_text('''cmake_minimum_required(VERSION 3.24)
project(openssl_selection LANGUAGES CXX)
find_package(Threads REQUIRED)
option(OPENSSL_USE_STATIC_LIBS "Link OpenSSL statically" ON)
include(OpenSSL.cmake)
relayweave_setup_openssl()
get_target_property(crypto_path OpenSSL::Crypto IMPORTED_LOCATION)
message(STATUS "Selected crypto: ${crypto_path}")
if(TARGET relayweave_openssl)
    message(STATUS "Vendored build enabled")
endif()
''')

    def vendor(self, version):
        source = self.source / 'vendor' / f'openssl-{version}'
        source.mkdir()
        (source / 'Configure').touch()
        major, minor, patch = version.split('.')
        (source / 'VERSION.dat').write_text(
            f'MAJOR={major}\nMINOR={minor}\nPATCH={patch}\nPRE_RELEASE_TAG=\n')

    def configure(self, *options):
        result = subprocess.run(
            ['cmake', '-S', str(self.source), '-B', str(self.source / 'build'), *options],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return result.returncode, result.stdout

    def system(self, version='3.5.9'):
        prefix = self.source / 'system-openssl'
        include = prefix / 'include/openssl'
        include.mkdir(parents=True)
        (prefix / 'lib').mkdir()
        (include / 'ssl.h').touch()
        (include / 'opensslv.h').write_text(
            f'#define OPENSSL_VERSION_STR "{version}"\n')
        suffix = '.dylib' if sys.platform == 'darwin' else '.so'
        for name in ('ssl', 'crypto'):
            for extension in ('.a', suffix):
                (prefix / f'lib/lib{name}{extension}').touch()
        return prefix, suffix

    def test_selects_highest_eligible_vendor(self):
        self.vendor('3.1.9')
        self.vendor('3.5.9')
        self.vendor('3.2.10')
        code, output = self.configure()
        self.assertEqual(code, 0, output)
        self.assertIn('Using vendored OpenSSL 3.5.9 (static)', output)
        self.assertIn('openssl-3.5.9-static/install/lib/libcrypto.a', output)

    def test_missing_vendor_fails_instead_of_using_system(self):
        code, output = self.configure()
        self.assertNotEqual(code, 0, output)
        self.assertIn('OpenSSL >= 3.2 source directory is required', output)
        self.assertIn('system OpenSSL is not used', output)

    def test_old_vendor_fails_instead_of_using_system(self):
        self.vendor('3.1.9')
        code, output = self.configure()
        self.assertNotEqual(code, 0, output)
        self.assertIn('OpenSSL >= 3.2 source directory is required', output)

    def test_cached_system_paths_do_not_override_vendor(self):
        self.vendor('3.5.9')
        code, output = self.configure('-DOPENSSL_ROOT_DIR=/unused-system-openssl',
                                      '-DOPENSSL_INCLUDE_DIR=/unused-system-openssl/include',
                                      '-DOPENSSL_SSL_LIBRARY=/unused-system-openssl/libssl.so',
                                      '-DOPENSSL_CRYPTO_LIBRARY=/unused-system-openssl/libcrypto.so')
        self.assertEqual(code, 0, output)
        self.assertIn('openssl-3.5.9-static/install/lib/libcrypto.a', output)

    def test_system_and_compression_packages_are_not_looked_up(self):
        self.vendor('3.5.9')
        for name in ('OpenSSL', 'ZLIB', 'zstd'):
            (self.source / f'Find{name}.cmake').write_text(
                'message(FATAL_ERROR "Unexpected system dependency lookup")\n')
        code, output = self.configure(f'-DCMAKE_MODULE_PATH={self.source}',
                                      '-DCMAKE_DISABLE_FIND_PACKAGE_zstd=TRUE')
        self.assertEqual(code, 0, output)

    def test_incomplete_source_directory_is_rejected(self):
        (self.source / 'vendor/openssl-3.5.9').mkdir()
        code, output = self.configure()
        self.assertNotEqual(code, 0, output)
        self.assertIn('OpenSSL >= 3.2 source directory is required', output)

    def test_archive_is_not_accepted_as_source(self):
        (self.source / 'vendor/openssl-3.5.9.tar.gz').touch()
        code, output = self.configure()
        self.assertNotEqual(code, 0, output)
        self.assertIn('source directory is required', output)

    def test_actual_source_version_is_checked(self):
        self.vendor('3.1.9')
        (self.source / 'vendor/openssl-3.1.9').rename(
            self.source / 'vendor/openssl-3.5.9')
        code, output = self.configure()
        self.assertNotEqual(code, 0, output)
        self.assertIn('OpenSSL >= 3.2 source directory is required', output)

    def test_dynamic_uses_system_despite_vendor(self):
        self.vendor('3.5.9')
        prefix, suffix = self.system('3.2.8')
        code, output = self.configure('-DOPENSSL_USE_STATIC_LIBS=OFF',
                                      f'-DOPENSSL_ROOT_DIR={prefix}')
        self.assertEqual(code, 0, output)
        self.assertIn('Using system OpenSSL 3.2.8 (shared)', output)
        self.assertIn(f'Selected crypto: {prefix}/lib/libcrypto{suffix}', output)
        self.assertNotIn('Vendored build enabled', output)

    def test_dynamic_works_without_vendor(self):
        prefix, _ = self.system()
        code, output = self.configure('-DOPENSSL_USE_STATIC_LIBS=OFF',
                                      f'-DOPENSSL_ROOT_DIR={prefix}')
        self.assertEqual(code, 0, output)
        self.assertIn('Using system OpenSSL 3.5.9 (shared)', output)

    def test_dynamic_rejects_old_system_despite_vendor(self):
        self.vendor('3.5.9')
        prefix, _ = self.system('3.1.9')
        code, output = self.configure('-DOPENSSL_USE_STATIC_LIBS=OFF',
                                      f'-DOPENSSL_ROOT_DIR={prefix}')
        self.assertNotEqual(code, 0, output)
        self.assertIn('3.1.9', output)
        self.assertIn('3.2', output)

    def test_dynamic_replaces_cached_static_libraries(self):
        prefix, suffix = self.system()
        code, output = self.configure('-DOPENSSL_USE_STATIC_LIBS=OFF',
                                      f'-DOPENSSL_ROOT_DIR={prefix}',
                                      f'-DOPENSSL_SSL_LIBRARY={prefix}/lib/libssl.a',
                                      f'-DOPENSSL_CRYPTO_LIBRARY={prefix}/lib/libcrypto.a')
        self.assertEqual(code, 0, output)
        self.assertIn(f'Selected crypto: {prefix}/lib/libcrypto{suffix}', output)

    def test_switching_modes_in_same_build_directory(self):
        self.vendor('3.5.9')
        prefix, suffix = self.system()
        for static in ('ON', 'OFF', 'ON'):
            code, output = self.configure(f'-DOPENSSL_USE_STATIC_LIBS={static}',
                                          f'-DOPENSSL_ROOT_DIR={prefix}')
            self.assertEqual(code, 0, output)
            if static == 'ON':
                self.assertIn('openssl-3.5.9-static/install/lib/libcrypto.a', output)
                self.assertIn('Vendored build enabled', output)
            else:
                self.assertIn(f'Selected crypto: {prefix}/lib/libcrypto{suffix}', output)
                self.assertNotIn('Vendored build enabled', output)


if __name__ == '__main__':
    unittest.main()
