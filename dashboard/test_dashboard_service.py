"""Process-level checks for the independently managed dashboard service."""
from __future__ import annotations

import gzip
import json
from http.client import RemoteDisconnected
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'test' / 'data'


class DashboardServiceTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.work = Path(self.directory.name)
        self.http_reservation = socket.socket()
        self.http_reservation.bind(('127.0.0.1', 0))
        self.http_port = self.http_reservation.getsockname()[1]
        # Keep the control port bound without listening: deterministic offline Node.
        self.control_reservation = socket.socket()
        self.control_reservation.bind(('127.0.0.1', 0))
        self.addCleanup(self.control_reservation.close)
        config = {
            'server': {'host': '127.0.0.1', 'port': self.control_reservation.getsockname()[1]},
            'certificate': {
                'ca_file': str(DATA / 'tls_channel_test_ca.pem'),
                'certificate_chain': str(DATA / 'tls_channel_test_client.pem'),
                'private_key': str(DATA / 'tls_channel_test_client.key'),
            },
        }
        self.config = self.work / 'dashboard.json'
        self.config.write_text(json.dumps(config))
        self.database = self.work / 'state' / 'history.sqlite3'
        self.process = None
        self.log = (self.work / 'service.log').open('w+')
        self.addCleanup(self.log.close)
        self.addCleanup(self.stop)

    def start(self, launcher=None):
        self.http_reservation.close()
        self.process = subprocess.Popen(
            [sys.executable, *(launcher or [str(ROOT / 'dashboard' / 'dashboard.py')]), str(self.config),
             '--database', str(self.database), '--http-port', str(self.http_port), '--poll-interval', '0.1'],
            cwd=self.work, stdout=self.log, stderr=subprocess.STDOUT,
            env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PYTHONUNBUFFERED='1'),
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                return self.get('/api/health')
            except OSError:
                if self.process.poll() is not None:
                    break
                time.sleep(0.05)
        self.log.flush()
        self.log.seek(0)
        self.fail('service did not start: ' + self.log.read())

    def get(self, route, timeout=1, headers=None):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        request = urllib.request.Request(
            f'http://127.0.0.1:{self.http_port}{route}', headers=headers or {}
        )
        with opener.open(request, timeout=timeout) as response:
            return response.headers, response.read()

    def stop(self):
        self.http_reservation.close()
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                self.fail('dashboard did not stop within 10 seconds')

    def test_production_http_works_while_node_is_offline(self):
        headers, body = self.start()
        self.assertIn('waitress', headers['Server'].lower())
        self.assertFalse(json.loads(body)['ok'])
        page = self.get('/')[1]
        self.assertIn(b'Registered services', page)
        self.assertIn(b'Node health', page)
        self.assertNotIn(b'Cluster topology', page)
        self.assertEqual(json.loads(self.get('/api/snapshot')[1])['connected_nodes'], 0)
        gzip_headers, gzip_body = self.get('/', headers={'Accept-Encoding': 'gzip'})
        self.assertEqual(gzip_headers['Content-Encoding'], 'gzip')
        self.assertIn(b'Registered services', gzip.decompress(gzip_body))

    @unittest.skipUnless(hasattr(signal, 'SIGTERM'), 'requires service termination signal')
    def test_sigterm_is_clean_and_database_survives_restart(self):
        self.start()
        self.stop()
        self.assertEqual(self.process.returncode, 0)
        with closing(sqlite3.connect(self.database)) as database, database:
            database.execute("INSERT INTO node_queue_history VALUES ('saved-node', 100, 1, 2, 3)")
        self.start()
        self.stop()
        self.assertEqual(self.process.returncode, 0)
        with closing(sqlite3.connect(self.database)) as database, database:
            self.assertEqual(database.execute('PRAGMA quick_check').fetchone()[0], 'ok')
            self.assertEqual(database.execute('SELECT COUNT(*) FROM node_queue_history').fetchone()[0], 1)


    def test_sigterm_drains_an_inflight_request_before_closing_history(self):
        started = self.work / 'request-started'
        # A test-only route exercises shared state after Waitress's five-second grace period.
        launcher = "\n".join([
            'import sys, time',
            'from pathlib import Path',
            f'sys.path.insert(0, {str(ROOT / "dashboard")!r})',
            'import dashboard as service',
            '@service.app.route("/slow-history")',
            'def slow_history():',
            f'    Path({str(started)!r}).touch()',
            '    time.sleep(6)',
            '    service._client._history_store.append_queue("slow-request", 100, 1, 2, 3)',
            '    return "completed"',
            'raise SystemExit(service.main())',
        ])
        self.start(['-c', launcher])
        with ThreadPoolExecutor(max_workers=1) as executor:
            response = executor.submit(self.get, '/slow-history', 12)
            deadline = time.monotonic() + 3
            while not started.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(started.exists(), 'slow request did not start')
            self.stop()
            try:
                self.assertEqual(response.result(timeout=2)[1], b'completed')
            except RemoteDisconnected:
                # Restart may close the transport, but the admitted handler must finish
                # using the database before it is closed (verified below).
                pass
        self.assertEqual(self.process.returncode, 0)
        with closing(sqlite3.connect(self.database)) as database, database:
            self.assertEqual(database.execute(
                "SELECT COUNT(*) FROM node_queue_history WHERE node_key='slow-request'").fetchone()[0], 1)


class CollectorStopTest(unittest.TestCase):
    def test_stop_waits_for_pending_tls_handshake_before_releasing_history(self):
        from history_store import HistoryStore
        from proxy_client import ProxyControlClient
        with tempfile.TemporaryDirectory() as directory, socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen()
            listener.settimeout(3)
            store = HistoryStore(Path(directory) / "history.sqlite3")
            self.addCleanup(store.close)
            client = ProxyControlClient(
                host='127.0.0.1', port=listener.getsockname()[1],
                ca_file=str(DATA / 'tls_channel_test_ca.pem'),
                cert_file=str(DATA / 'tls_channel_test_client.pem'),
                key_file=str(DATA / 'tls_channel_test_client.key'),
                history_store=store, handshake_timeout=3,
            )
            client.start()
            peer, _ = listener.accept()
            try:
                client.stop()
                self.assertFalse(client._thread.is_alive(), 'stop returned with an active database user')
            finally:
                peer.close()
                client.stop()
                store.close()


if __name__ == '__main__':
    unittest.main()
