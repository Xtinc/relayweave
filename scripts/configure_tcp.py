#!/usr/bin/env python3
"""Persist and apply TCP congestion control and socket buffer limits on Linux."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


CONFIG_PATH = Path("/etc/sysctl.d/99-zz-relayweave-tcp.conf")
BACKUP_ROOT = Path("/var/backups/relayweave/tcp")
KEYS = (
    "net.core.wmem_max",
    "net.core.rmem_max",
    "net.ipv4.tcp_wmem",
    "net.ipv4.tcp_rmem",
    "net.ipv4.tcp_moderate_rcvbuf",
    "net.ipv4.tcp_congestion_control",
)


def command(args, **kwargs):
    result = subprocess.run(args, text=True, capture_output=True, **kwargs)
    if result.returncode:
        raise RuntimeError(f"{' '.join(args)}: {result.stderr.strip() or result.stdout.strip()}")
    return result.stdout


def read_settings():
    lines = command(["sysctl", "-n", *KEYS]).splitlines()
    if len(lines) != len(KEYS):
        raise RuntimeError("sysctl returned an incomplete TCP configuration")
    return dict(zip(KEYS, (" ".join(line.split()) for line in lines)))


def validate_settings(settings):
    if set(settings) != set(KEYS):
        raise ValueError("configuration must contain exactly the supported TCP settings")
    for key in KEYS:
        value = settings[key]
        if not isinstance(value, str):
            raise ValueError(f"invalid value for {key}")
        if key == "net.ipv4.tcp_congestion_control":
            if not re.fullmatch(r"[a-zA-Z0-9_]{1,31}", value):
                raise ValueError("invalid congestion control name")
        else:
            count = 3 if key in ("net.ipv4.tcp_wmem", "net.ipv4.tcp_rmem") else 1
            if not re.fullmatch(r"[0-9]+(?: [0-9]+){" + str(count - 1) + "}", value):
                raise ValueError(f"invalid value for {key}")
            numbers = [int(item) for item in value.split()]
            if any(number > 2147483647 for number in numbers):
                raise ValueError(f"value too large for {key}")
            if count == 3 and (numbers != sorted(numbers) or numbers[0] == 0):
                raise ValueError(f"{key} must satisfy 0 < min <= default <= max")
    if settings["net.ipv4.tcp_moderate_rcvbuf"] not in ("0", "1"):
        raise ValueError("tcp_moderate_rcvbuf must be 0 or 1")


def render_settings(settings):
    validate_settings(settings)
    return "".join(f"{key} = {settings[key]}\n" for key in KEYS)


def apply_kernel(settings):
    command(["sysctl", "--load=-"], input=render_settings(settings))
    actual = read_settings()
    differences = [f"{key}: requested {settings[key]!r}, got {actual[key]!r}"
                   for key in KEYS if actual[key] != settings[key]]
    if differences:
        raise RuntimeError("TCP settings did not match after loading: " + "; ".join(differences))


def write_config(path, content):
    if content is None:
        path.unlink(missing_ok=True)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(content)
            stream.flush()
            os.fchmod(stream.fileno(), 0o644)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def install_settings(settings, content, config_path=CONFIG_PATH, backup_root=BACKUP_ROOT):
    validate_settings(settings)
    if config_path.is_symlink():
        raise ValueError(f"managed configuration must not be a symlink: {config_path}")
    before = {
        "settings": read_settings(),
        "config": config_path.read_text() if config_path.exists() else None,
    }
    backup_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-")
    backup = Path(tempfile.mkdtemp(prefix=stamp, dir=backup_root))
    (backup / "snapshot.json").write_text(json.dumps(before, indent=2) + "\n")
    print(f"Backup: {backup}", flush=True)
    try:
        write_config(config_path, content)
        apply_kernel(settings)
    except BaseException as error:
        # Restore both persistence and any settings applied before a partial sysctl failure.
        failures = []
        for restore in (
            lambda: write_config(config_path, before["config"]),
            lambda: apply_kernel(before["settings"]),
        ):
            try:
                restore()
            except Exception as rollback_error:
                failures.append(str(rollback_error))
        if failures:
            raise RuntimeError(f"Apply failed: {error}; rollback incomplete: {'; '.join(failures)}; backup: {backup}") from error
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise RuntimeError(f"Apply failed and was rolled back: {error}; backup: {backup}") from error
    return backup


def buffer_mib(value):
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("buffer size must be an integer in MiB") from error
    if not 1 <= number <= 1024:
        raise argparse.ArgumentTypeError("buffer size must be 1..1024 MiB")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--congestion-control", default="cubic", help="loaded TCP algorithm (default: cubic)")
    parser.add_argument("--send-buffer-mib", type=buffer_mib, default=32, help="TCP send buffer maximum (default: 32)")
    parser.add_argument("--receive-buffer-mib", type=buffer_mib, default=32, help="TCP receive buffer maximum (default: 32)")
    parser.add_argument("--dry-run", action="store_true", help="show changes without writing files or kernel parameters")
    parser.add_argument("--restore", type=Path, metavar="BACKUP_DIRECTORY", help="restore a backup printed by this script")
    args = parser.parse_args()
    if not sys.platform.startswith("linux"):
        parser.error("this script requires Linux")
    if args.restore:
        snapshot = json.loads((args.restore / "snapshot.json").read_text())
        settings = snapshot["settings"]
        content = snapshot["config"]
        if content is not None and not isinstance(content, str):
            raise ValueError("invalid saved configuration content")
    else:
        settings = read_settings()
        for direction, mib in (("w", args.send_buffer_mib), ("r", args.receive_buffer_mib)):
            key = f"net.ipv4.tcp_{direction}mem"
            minimum, initial, _ = settings[key].split()
            maximum = mib * 1024 * 1024
            settings[key] = f"{minimum} {initial} {maximum}"
            key = f"net.core.{direction}mem_max"
            settings[key] = str(max(int(settings[key]), maximum))
        settings["net.ipv4.tcp_moderate_rcvbuf"] = "1"
        settings["net.ipv4.tcp_congestion_control"] = args.congestion_control
        content = "# Managed by RelayWeave scripts/configure_tcp.py\n" + render_settings(settings)
    validate_settings(settings)
    algorithm = settings["net.ipv4.tcp_congestion_control"]
    available = command(["sysctl", "-n", "net.ipv4.tcp_available_congestion_control"]).split()
    if algorithm not in available:
        raise ValueError(f"TCP algorithm {algorithm!r} is not loaded; available: {', '.join(available)}")
    print(f"Configuration: {CONFIG_PATH}")
    print(render_settings(settings), end="")
    if args.dry_run:
        print("Dry run: no changes applied.")
        return
    if os.geteuid() != 0:
        parser.error("run with sudo to apply changes, or use --dry-run")
    backup = install_settings(settings, content)
    print("Applied and verified. Settings persist across reboot.")
    print("Restart benchmark/node processes to recreate listeners and TCP connections.")
    print(f"Restore with: sudo python3 {Path(__file__).resolve()} --restore {backup}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)
