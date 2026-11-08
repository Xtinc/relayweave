import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(shutil.which('cmake'), 'CMake is required')
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
get_target_property(crypto_path OpenSSL::Crypto LOCATION)
message(STATUS "Selected crypto: ${crypto_path}")
if(TARGET relayweave_openssl)
    message(STATUS "Vendored build enabled")
endif()
file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/selected-$<CONFIG>.txt"
    CONTENT "$<TARGET_FILE:OpenSSL::Crypto>\\n$<TARGET_PROPERTY:OpenSSL::Crypto,INTERFACE_INCLUDE_DIRECTORIES>\\n")
''')

    def vendor(self, version):
        source = self.source / 'vendor' / f'openssl-{version}'
        source.mkdir()
        (source / 'Configure').touch()
        major, minor, patch = version.split('.')
        (source / 'VERSION.dat').write_text(
            f'MAJOR={major}\nMINOR={minor}\nPATCH={patch}\nPRE_RELEASE_TAG=\n')

    def configure(self, *options):
        platform_options = (['-DCMAKE_BUILD_TYPE=Debug',
                             '-DCMAKE_CONFIGURATION_TYPES=Debug;Release']
                            if os.name == 'nt' else [])
        result = subprocess.run(
            ['cmake', '-S', str(self.source), '-B', str(self.source / 'build'),
             *platform_options, *options],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return result.returncode, result.stdout

    def vendor_crypto_path(self, version='3.5.9'):
        library = 'Debug/lib/libcrypto.lib' if os.name == 'nt' else 'lib/libcrypto.a'
        return f'openssl-{version}-static/install/{library}'

    def system(self, version='3.5.9'):
        prefix = self.source / 'system-openssl'
        include = prefix / 'include/openssl'
        include.mkdir(parents=True)
        (prefix / 'lib').mkdir()
        (include / 'ssl.h').touch()
        (include / 'opensslv.h').write_text(
            f'#define OPENSSL_VERSION_STR "{version}"\n')
        suffix = '.lib' if os.name == 'nt' else ('.dylib' if sys.platform == 'darwin' else '.so')
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
        self.assertIn(self.vendor_crypto_path(), output)

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
        self.assertIn(self.vendor_crypto_path(), output)

    def test_system_and_compression_packages_are_not_looked_up(self):
        self.vendor('3.5.9')
        for name in ('OpenSSL', 'ZLIB', 'zstd'):
            (self.source / f'Find{name}.cmake').write_text(
                'message(FATAL_ERROR "Unexpected system dependency lookup")\n')
        code, output = self.configure(f'-DCMAKE_MODULE_PATH={self.source.as_posix()}',
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
        self.assertIn(f'Selected crypto: {prefix.as_posix()}/lib/libcrypto{suffix}', output)
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
        self.assertIn(f'Selected crypto: {prefix.as_posix()}/lib/libcrypto{suffix}', output)

    def test_switching_modes_in_same_build_directory(self):
        self.vendor('3.5.9')
        prefix, suffix = self.system()
        for static in ('ON', 'OFF', 'ON'):
            code, output = self.configure(f'-DOPENSSL_USE_STATIC_LIBS={static}',
                                          f'-DOPENSSL_ROOT_DIR={prefix}')
            self.assertEqual(code, 0, output)
            if static == 'ON':
                self.assertIn(self.vendor_crypto_path(), output)
                self.assertIn('Vendored build enabled', output)
            else:
                self.assertIn(f'Selected crypto: {prefix.as_posix()}/lib/libcrypto{suffix}', output)
                self.assertNotIn('Vendored build enabled', output)

    @unittest.skipUnless(os.name == 'nt', 'Windows MSVC configuration test')
    def test_windows_configurations_use_matching_headers_and_libraries(self):
        self.vendor('3.5.9')
        code, output = self.configure()
        self.assertEqual(code, 0, output)
        selected_files = list((self.source / 'build').glob('selected-*.txt'))
        self.assertTrue(selected_files)
        for selected in selected_files:
            config = selected.stem.removeprefix('selected-')
            library, include = selected.read_text().splitlines()
            self.assertTrue(library.endswith(f'/install/{config}/lib/libcrypto.lib'), library)
            self.assertTrue(include.endswith(f'/install/{config}/include'), include)

    @unittest.skipUnless(os.name == 'nt', 'Windows MSVC configuration test')
    def test_windows_single_config_without_build_type(self):
        self.vendor('3.5.9')
        # A stale CMAKE_CONFIGURATION_TYPES must not affect a single-config generator.
        code, output = self.configure('-G', 'NMake Makefiles', '-DCMAKE_BUILD_TYPE=')
        self.assertEqual(code, 0, output)
        selected = (self.source / 'build/selected-.txt').read_text().splitlines()
        self.assertTrue(selected[0].endswith('/install/Release/lib/libcrypto.lib'), selected)
        self.assertTrue(selected[1].endswith('/install/Release/include'), selected)

    @unittest.skipUnless(os.name == 'nt', 'Windows MSVC configuration test')
    def test_windows_runtime_library_is_propagated(self):
        self.vendor('3.5.9')
        for runtime, debug_flag, release_flag in (
                ('MultiThreaded$<$<CONFIG:Debug>:Debug>DLL', '/MDd', '/MD'),
                ('MultiThreaded$<$<CONFIG:Debug>:Debug>', '/MTd', '/MT'),
                ('', '', '')):
            code, output = self.configure(f'-DCMAKE_MSVC_RUNTIME_LIBRARY={runtime}')
            self.assertEqual(code, 0, output)
            commands = list((self.source / 'build/_deps').rglob('*-configure-*.cmake'))
            self.assertTrue(commands)
            for command in commands:
                debug = command.stem.endswith('-Debug')
                content = command.read_text()
                flag = debug_flag if debug else release_flag
                if flag:
                    self.assertIn(f';{flag}', content)
                else:
                    for runtime_flag in ('/MD', '/MDd', '/MT', '/MTd'):
                        self.assertNotIn(runtime_flag, content)
                self.assertNotIn('CFLAGS=', content)
                self.assertIn('--debug' if debug else '--release', content)
                self.assertNotIn('-fPIC', content)


if __name__ == '__main__':
    unittest.main()
