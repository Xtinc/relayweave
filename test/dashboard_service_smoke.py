#!/usr/bin/env python3
"""Real Dashboard -> publisher Agent -> Node -> consumer Agent smoke test (requires dashboard/requirements.txt).

Run with the Python environment used by the dashboard:
    python test/dashboard_service_smoke.py --build-dir build
"""
from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'test' / 'data'


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, default=ROOT / 'build')
    parser.add_argument('--two-nodes', action='store_true',
                        help='Collect through a Slave, verify directed quality and entry restart')
    args = parser.parse_args()
    build = args.build_dir.resolve()
    # Fail before starting any process if the dashboard environment is incomplete.
    import flask  # noqa: F401
    import cbor2  # noqa: F401

    probes_available = False
    if args.two_nodes:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP):
                probes_available = True
        except PermissionError:
            print('SKIP: real ICMP quality assertions require CAP_NET_RAW; two-Node service checks still run')

    with tempfile.TemporaryDirectory(prefix='relayweave-dashboard-smoke-') as directory:
        work = Path(directory)
        reserved = []
        for _ in range(12 if args.two_nodes else 8):
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            reserved.append(sock)
        control, tcp, tls, udp, cluster, http, forward_tcp, forward_tls = [
            sock.getsockname()[1] for sock in reserved
        ][:8]
        config = json.loads((ROOT / 'node' / 'node.example.json').read_text())
        config['cluster'].update(role='master', node_id='dashboard-smoke', address='127.0.0.1', port=cluster)
        config['control'].update(address='127.0.0.1', advertise_address='127.0.0.1', port=control)
        for name, port in [('tcp', tcp), ('tls', tls), ('udp', udp)]:
            config[name].update(address='127.0.0.1', port=port)
        config['certificate'] = {
            'server_ca_file': str(DATA / 'tls_channel_test_ca.pem'),
            'ca_file': str(DATA / 'tls_channel_test_client_ca.pem'),
            'certificate_chain': str(DATA / 'tls_channel_test_server.pem'),
            'private_key': str(DATA / 'tls_channel_test_server.key'),
        }
        slave_config = None
        if args.two_nodes:
            slave_config = json.loads(json.dumps(config))
            slave_config['cluster'].update(role='slave', node_id='dashboard-slave')
            slave_control, slave_tcp, slave_tls, slave_udp = [sock.getsockname()[1] for sock in reserved[8:]]
            slave_config['control']['port'] = slave_control
            for name, port in [('tcp', slave_tcp), ('tls', slave_tls), ('udp', slave_udp)]:
                slave_config[name]['port'] = port
            (work / 'slave.json').write_text(json.dumps(slave_config))
        published_services = [
            dict(name=f'dashboard-{protocol}', target_host='127.0.0.1', target_port=http, protocol=protocol)
            for protocol in ('tcp', 'tls')
        ]
        client = {
            'log': {'debug_enable': True},
            'server': {'host': '127.0.0.1', 'port': control, 'connect_timeout_ms': 1000},
            'certificate': {
                'ca_file': str(DATA / 'tls_channel_test_ca.pem'),
                'certificate_chain': str(DATA / 'tls_channel_test_client.pem'),
                'private_key': str(DATA / 'tls_channel_test_client.key'),
            },
            'forwards': [
                dict(service=f'dashboard-{protocol}', listen_address='127.0.0.1',
                     listen_port=port, protocol=protocol)
                for protocol, port in [('tcp', forward_tcp), ('tls', forward_tls)]
            ],
        }
        publisher = dict(client, forwards=[], services=published_services)
        if args.two_nodes:
            client['server'] = dict(client['server'], port=slave_control)
        # Dashboard has its own control-channel configuration, independent of either Agent.
        collector = {'server': client['server'], 'certificate': client['certificate']}
        (work / 'node.json').write_text(json.dumps(config))
        (work / 'publisher.json').write_text(json.dumps(publisher))
        (work / 'dashboard.json').write_text(json.dumps(collector))
        (work / 'client.json').write_text(json.dumps(client))
        for sock in reserved:
            sock.close()
        processes = []
        logs = []
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PYTHONUNBUFFERED='1')

        def start(command: list[str], name: str) -> subprocess.Popen:
            log = (work / f'{name}.log').open('w+')
            logs.append(log)
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env)
            processes.append(process)
            return process

        def stop(process: subprocess.Popen) -> None:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM if process.args[0] == sys.executable else signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise AssertionError('process did not stop within 10 seconds')
                assert process.returncode == 0, f'process stopped with {process.returncode}'

        def get(port: int, route: str) -> bytes:
            # Explicitly bypass any HTTP proxy configured in the developer environment.
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(f'http://127.0.0.1:{port}{route}', timeout=2) as response:
                assert response.status == 200
                return response.read()

        def ready(port: int) -> dict:
            deadline = time.monotonic() + 20
            last_error = None
            while time.monotonic() < deadline:
                try:
                    snapshot = json.loads(get(port, '/api/snapshot?queue_range=all&bandwidth_range=all'))
                    expected_count = 2 if args.two_nodes else 1
                    if snapshot['connected_nodes'] == expected_count and all(n['history'] for n in snapshot['nodes']):
                        names = {s['service'] for n in snapshot['nodes'] for s in n['service_traffic']}
                        topology = snapshot.get('topology')
                        topology_ready = (
                            topology
                            and topology['version'] > 0
                            and len(topology['nodes']) == expected_count
                            and all(n['control_queue_delay_us'] is not None for n in topology['nodes'])
                            and not topology['stale']
                        )
                        if probes_available and topology_ready:
                            links = topology['links']
                            topology_ready = (
                                len(links) == 2 and all(link['usable'] for link in links)
                                and all(link['quality_score'] is not None and 0 <= link['confidence'] <= 1 for link in links)
                                and {(link['source'], link['destination']) for link in links} == {
                                    ('dashboard-smoke', 'dashboard-slave'), ('dashboard-slave', 'dashboard-smoke')}
                            )
                        if names == {'dashboard-tcp', 'dashboard-tls'} and topology_ready:
                            return snapshot
                        last_error = f'waiting for services/topology in snapshot: services={names}, topology={topology}'
                except Exception as error:
                    last_error = error
                if any(p.poll() is not None for p in processes):
                    raise AssertionError('a service exited before readiness')
                time.sleep(0.1)
            raise AssertionError(f'dashboard did not become ready: {last_error}')

        dashboard_command = [sys.executable, str(ROOT / 'dashboard' / 'dashboard.py'),
                             str(work / 'dashboard.json'), '--http-port', str(http),
                             '--poll-interval', '0.2', '--database', str(work / 'history.sqlite3')]
        try:
            start([str(build / 'node' / 'relayweave-node'), str(work / 'node.json')], 'node')
            slave = None
            if args.two_nodes:
                slave = start([str(build / 'node' / 'relayweave-node'), str(work / 'slave.json')], 'slave')
            start([str(build / 'agent' / 'relayweave-agent'), str(work / 'publisher.json')], 'publisher')
            dashboard = start(dashboard_command, 'dashboard')
            consumer = start([str(build / 'agent' / 'relayweave-agent'), str(work / 'client.json')], 'consumer')
            consumer_log = next(log for log in logs if log.name.endswith('/consumer.log'))

            def consumer_output() -> str:
                return Path(consumer_log.name).read_text()

            # Allow discovery, control connections and a background poll without business traffic.
            ready(http)
            time.sleep(5.2)
            assert 'Routes service=' not in consumer_output(), 'idle Agent still calculates service paths'
            for protocol, port in (('tcp', forward_tcp), ('tls', forward_tls)):
                snapshot = ready(port)
                prefix = f'Routes service=dashboard-{protocol}/{protocol} ->'
                # Consecutive connections share the destination cache.
                for _ in range(2):
                    before = consumer_output().count(prefix)
                    page = get(port, '/')
                    assert b'Registered services' in page and b'Node health' in page
                    assert b'Cluster topology' not in page
                    assert consumer_output().count(prefix) == before, 'cached destination was recalculated'
                names = {s['service'] for n in snapshot['nodes'] for s in n['service_traffic']}
                assert names == {'dashboard-tcp', 'dashboard-tls'}, names
            if probes_available:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    get(forward_tls, '/')
                    if 'Route #1 cost=' in consumer_output():
                        break
                    time.sleep(0.1)
                else:
                    raise AssertionError('Agent did not report a measured service recommendation')
            if slave is not None:
                stop(slave)
                processes.remove(slave)
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline:
                    disconnected = json.loads(get(http, '/api/snapshot'))
                    if not disconnected['entry_connected']:
                        assert not disconnected['topology'] or all(not link['usable'] for link in disconnected['topology']['links'])
                        break
                    time.sleep(0.1)
                else:
                    raise AssertionError('Collector did not invalidate disconnected ingress')
                start([str(build / 'node' / 'relayweave-node'), str(work / 'slave.json')], 'slave-restarted')
                snapshot = ready(forward_tls)
            old_timestamps = {p['t'] for p in snapshot['nodes'][0]['history']}
            stop(dashboard)
            processes.remove(dashboard)
            start(dashboard_command, 'dashboard-restarted')
            restarted = ready(forward_tls)
            assert old_timestamps & {p['t'] for p in restarted['nodes'][0]['history']}, 'history lost on restart'
            # Graceful exit flushes C++ stdout even without ICMP permissions.
            # Verify actual connection events independently of measured route costs.
            stop(consumer)
            processes.remove(consumer)
            consumer_log = next(log for log in logs if log.name.endswith('/consumer.log'))
            consumer_log.seek(0)
            output = consumer_log.read()
            lines = output.splitlines()
            calculations = [index for index, line in enumerate(lines) if 'Routes service=' in line]
            assert calculations, 'missing route calculation result'
            for index in calculations:
                assert lines[index].startswith('[DEB]'), 'route diagnostics leaked into INF'
                service = lines[index].split('Routes service=', 1)[1].split(' ->', 1)[0]
                if 'candidates=0' in lines[index]:
                    assert 'cost=unavailable' in lines[index], 'missing unavailable route result'
                    continue
                paths = []
                for line in lines[index + 1:]:
                    if 'Routes service=' in line:
                        break
                    if f'service={service}' in line and 'Route #' in line:
                        paths.append(line)
                assert paths, 'route calculation has no result'
                assert len(paths) <= 3, 'too many logged route candidates'
                costs = []
                for rank, line in enumerate(paths, 1):
                    match = re.search(r'Route #(\d+) cost=(\d+\.\d{3}) service=\S+: (agent -> .+?)(?: (\*))?$', line)
                    assert match and int(match[1]) == rank, 'route candidates lack ranks or ASCII paths'
                    assert (match[4] == '*') == (rank == 1), 'recommended route marker is not at the end of rank 1'
                    costs.append(float(match[2]))
                assert costs == sorted(costs), 'logged candidates are not ordered by cost'
            assert 'Control [+]' in output, 'missing control connection log'
            relay_events = []
            for line in lines:
                if 'Relay [+]' in line or 'Relay [x]' in line:
                    assert line.startswith('[INF]'), 'Relay lifecycle progress leaked out of INF'
                    match = re.search(
                        r'Relay (\[\+\]|\[x\]) (tcp|tls) consumer '
                        r'service=(\S+) uuid=(\d+)(?: reason=.*)?$', line)
                    assert match, 'Relay lifecycle fields lack a stable order'
                    relay_events.append((match[1], match.groups()[1:]))
            opened = [identity for event, identity in relay_events if event == '[+]']
            closed = [identity for event, identity in relay_events if event == '[x]']
            assert opened, 'missing Relay connection log'
            assert sorted(opened) == sorted(closed), 'Relay lifecycle logs do not pair with identical identifiers'
            if args.two_nodes:
                print('PASS: two Nodes, Slave entry, directed quality, measured recommendation and ingress restart'
                      if probes_available else 'PASS: two Nodes, Slave entry and ingress restart; ICMP assertions skipped')
            print('PASS: real Dashboard page/API and topology snapshot through publisher Agent -> Node -> consumer Agent over TCP/TLS; SIGTERM and restart preserve shared history')
        except Exception:
            for log in logs:
                log.flush()
                log.seek(0)
                print(f'--- {log.name} ---\n{log.read()}', file=sys.stderr)
            raise
        finally:
            failure = None
            for process in reversed(processes):
                try:
                    stop(process)
                except Exception as error:
                    failure = failure or error
            for log in logs:
                log.close()
            if failure:
                raise failure


if __name__ == '__main__':
    main()
