import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "configure_tcp.py"
SPEC = importlib.util.spec_from_file_location("configure_tcp", SCRIPT)
tcp = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tcp)
REAL_APPLY_KERNEL = tcp.apply_kernel


class ConfigureTcpTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = self.root / "sysctl.d" / "99-zz-relayweave-tcp.conf"
        self.backups = self.root / "backups"
        self.original = dict(zip(tcp.KEYS, (
            "4194304", "4194304", "16384 16384 4194304", "16384 131072 32856192", "0", "bbr",
        )))
        self.desired = dict(zip(tcp.KEYS, (
            "33554432", "33554432", "16384 16384 33554432", "16384 131072 33554432", "1", "cubic",
        )))
        self.live = self.original.copy()
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(tcp, "read_settings", side_effect=lambda: self.live.copy()).start()
        mock.patch.object(tcp, "apply_kernel", side_effect=self.apply_kernel).start()

    def apply_kernel(self, settings):
        self.live = settings.copy()

    def install(self, settings, content):
        with contextlib.redirect_stdout(io.StringIO()):
            return tcp.install_settings(settings, content, self.config, self.backups)

    def test_apply_and_restore_existing_config_and_live_values(self):
        self.config.parent.mkdir()
        self.config.write_text("# previous persistent configuration\n")
        backup = self.install(self.desired, tcp.render_settings(self.desired))
        snapshot = json.loads((backup / "snapshot.json").read_text())
        self.assertEqual(self.live, self.desired)
        self.assertEqual(snapshot["settings"], self.original)
        self.assertEqual(snapshot["config"], "# previous persistent configuration\n")
        restore_backup = self.install(snapshot["settings"], snapshot["config"])
        self.assertNotEqual(backup, restore_backup)
        self.assertEqual(self.live, self.original)
        self.assertEqual(self.config.read_text(), snapshot["config"])

    def test_restore_removes_config_if_it_was_originally_absent(self):
        backup = self.install(self.desired, tcp.render_settings(self.desired))
        snapshot = json.loads((backup / "snapshot.json").read_text())
        self.assertIsNone(snapshot["config"])
        self.install(snapshot["settings"], snapshot["config"])
        self.assertFalse(self.config.exists())
        self.assertEqual(self.live, self.original)

    def test_partial_failure_rolls_back_live_and_persistent_settings(self):
        def partial_apply(settings):
            if settings == self.desired:
                self.live["net.core.wmem_max"] = settings["net.core.wmem_max"]
                raise RuntimeError("injected sysctl write failure")
            self.apply_kernel(settings)

        with mock.patch.object(tcp, "apply_kernel", side_effect=partial_apply):
            with self.assertRaisesRegex(RuntimeError, "was rolled back"):
                self.install(self.desired, tcp.render_settings(self.desired))
        self.assertEqual(self.live, self.original)
        self.assertFalse(self.config.exists())
        self.assertEqual(len(list(self.backups.glob("*/snapshot.json"))), 1)

    def test_dry_run_preserves_initial_sizes_and_existing_larger_core_limit(self):
        self.live["net.core.wmem_max"] = "67108864"
        output = io.StringIO()
        with mock.patch("sys.argv", [str(SCRIPT), "--dry-run"]), \
                mock.patch.object(tcp, "command", return_value="reno cubic bbr"), \
                mock.patch.object(tcp, "install_settings") as install, \
                contextlib.redirect_stdout(output):
            tcp.main()
        install.assert_not_called()
        self.assertIn("net.core.wmem_max = 67108864", output.getvalue())
        self.assertIn("net.ipv4.tcp_wmem = 16384 16384 33554432", output.getvalue())
        self.assertIn("net.ipv4.tcp_rmem = 16384 131072 33554432", output.getvalue())
        self.assertIn("net.ipv4.tcp_congestion_control = cubic", output.getvalue())

    def test_unavailable_algorithm_fails_before_install(self):
        with mock.patch("sys.argv", [str(SCRIPT), "--congestion-control", "unknown"]), \
                mock.patch.object(tcp, "command", return_value="reno cubic bbr"), \
                mock.patch.object(tcp, "install_settings") as install:
            with self.assertRaisesRegex(ValueError, "not loaded"):
                tcp.main()
        install.assert_not_called()

    def test_reject_invalid_backup_values_and_buffer_sizes(self):
        for value in ("0", "-1", "1025", "1.5", "foo"):
            with self.subTest(value=value), self.assertRaises(tcp.argparse.ArgumentTypeError):
                tcp.buffer_mib(value)
        for key, value in (("net.ipv4.tcp_wmem", "16384 16384 1"),
                           ("net.ipv4.tcp_congestion_control", "cubic\nnet.ipv4.ip_forward=1")):
            invalid = self.desired.copy()
            invalid[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.install(invalid, "invalid")
        self.assertFalse(self.backups.exists())

    def test_kernel_apply_verifies_readback(self):
        with mock.patch.object(tcp, "command", return_value="") as command:
            with self.assertRaisesRegex(RuntimeError, "did not match"):
                REAL_APPLY_KERNEL(self.desired)
        command.assert_called_once_with(["sysctl", "--load=-"], input=tcp.render_settings(self.desired))


if __name__ == "__main__":
    unittest.main()
