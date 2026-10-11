"""Current relay collection, route grouping and deterministic metro-map SVG.

All input records describe Ready business instances. NodeLink state and
predicted routes are deliberately not used to infer an active business.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
import hashlib
import json
import threading
import xml.etree.ElementTree as ET


def _identity(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value < 2**64:
        raise ValueError("relay identity must be a positive uint64")
    return str(value)


def _text(record: dict, name: str) -> str:
    value = record.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"relay {name} must be a non-empty string")
    return value


def validate_relays(node: str, raw: object) -> tuple[dict, ...]:
    """Validate and own a report before committing any collection state."""
    if not isinstance(raw, list):
        raise ValueError("server.status.reported.relays must be an array")
    result = []
    identities = set()
    for record in raw:
        if not isinstance(record, dict):
            raise ValueError("relay must be an object")
        common = {name: _text(record, name) for name in ("mode", "service", "protocol")}
        if common["protocol"] not in {"tcp", "tls", "udp"}:
            raise ValueError("invalid relay protocol")
        if common["mode"] == "single":
            allowed = {"mode", "service", "protocol", "uuid", "consumer_peer", "producer_peer"}
            common.update(uuid=_identity(record.get("uuid")),
                          consumer_peer=_text(record, "consumer_peer"),
                          producer_peer=_text(record, "producer_peer"))
            identity = ("single", common["uuid"])
        elif common["mode"] == "multi":
            allowed = {"mode", "service", "protocol", "epoch", "flow_id", "role", "agent_peer"}
            common.update(epoch=_identity(record.get("epoch")), flow_id=_identity(record.get("flow_id")),
                          role=_text(record, "role"), agent_peer=_text(record, "agent_peer"))
            if common["role"] == "ingress":
                allowed.add("path")
                path = record.get("path")
                if (not isinstance(path, list) or not 2 <= len(path) <= 8
                        or any(not isinstance(item, str) or not item for item in path)
                        or len(set(path)) != len(path) or path[0] != node):
                    raise ValueError("invalid relay ingress path")
                common["path"] = tuple(path)
            elif common["role"] != "egress":
                raise ValueError("invalid relay role")
            identity = ("multi", common["epoch"], common["flow_id"])
        else:
            raise ValueError("invalid relay mode")
        if set(record) != allowed or identity in identities:
            raise ValueError("unexpected relay fields or repeated relay identity")
        identities.add(identity)
        result.append(common)
    return tuple(result)


@dataclass
class _Round:
    number: int
    members: frozenset[str] | None
    epoch: str | None
    reports: dict[str, tuple[dict, ...]] = field(default_factory=dict)


class FlowCollection:
    """Serial access under ProxyControlClient's lock; retain only three rounds."""

    def __init__(self):
        self.members: frozenset[str] | None = None
        self.epoch: str | None = None
        self.pending: dict[int, _Round] = {}
        self.published: _Round | None = None

    def begin(self, request: int, number: int) -> None:
        for key, collection in list(self.pending.items()):
            if number - collection.number >= 3:
                self._publish(collection)
                del self.pending[key]
        self.pending[request] = _Round(number, self.members, self.epoch)

    def topology(self, members: set[str], epoch: int) -> None:
        next_members = frozenset(members)
        next_epoch = str(epoch)
        if self.members is not None and (next_members != self.members or next_epoch != self.epoch):
            self.pending.clear()
            self.published = None
        self.members, self.epoch = next_members, next_epoch
        for collection in self.pending.values():
            collection.members, collection.epoch = next_members, next_epoch
            collection.reports = {node: records for node, records in collection.reports.items()
                                  if node in next_members}
            self._try_publish(collection)

    def record(self, request: int, node: str, relays: tuple[dict, ...]) -> None:
        collection = self.pending.get(request)
        if collection is None or node in collection.reports:
            return
        if collection.members is not None and node not in collection.members:
            return
        collection.reports[node] = relays
        self._try_publish(collection)

    def _try_publish(self, collection: _Round) -> None:
        if collection.members is not None and collection.members <= collection.reports.keys():
            self._publish(collection)

    def _publish(self, collection: _Round) -> None:
        if collection.members is not None and (self.published is None or collection.number > self.published.number):
            self.published = collection

    def snapshot(self, connected: bool) -> dict:
        collection = self.published if connected else None
        if collection is None:
            return {"state": "pending" if connected else "disconnected", "partial": False,
                    "round": None, "missing_nodes": [], "unmatched_count": 0,
                    "flow_count": 0, "route_count": 0, "routes": []}
        missing = sorted(collection.members - collection.reports.keys())
        flows = []
        halves = defaultdict(list)
        for node, records in sorted(collection.reports.items()):
            for record in records:
                if record["mode"] == "single":
                    flows.append({"service": record["service"], "protocol": record["protocol"],
                                  "path": [node], "consumer": {"node": node, "peer": record["consumer_peer"]},
                                  "producer": {"node": node, "peer": record["producer_peer"]},
                                  "business": {"mode": "single", "node": node, "uuid": record["uuid"]}})
                elif record["epoch"] == collection.epoch:
                    halves[(record["epoch"], record["flow_id"])].append((node, record))
        unmatched = 0
        for (epoch, flow_id), records in sorted(halves.items()):
            heads = [(node, r) for node, r in records if r["role"] == "ingress"]
            tails = [(node, r) for node, r in records if r["role"] == "egress"]
            if len(heads) != 1 or len(tails) != 1:
                unmatched += 1
                continue
            head_node, head = heads[0]
            tail_node, tail = tails[0]
            if (head["service"] != tail["service"] or head["protocol"] != tail["protocol"]
                    or head["path"][-1] != tail_node or not set(head["path"]) <= collection.members):
                unmatched += 1
                continue
            flows.append({"service": head["service"], "protocol": head["protocol"],
                          "path": list(head["path"]),
                          "consumer": {"node": head_node, "peer": head["agent_peer"]},
                          "producer": {"node": tail_node, "peer": tail["agent_peer"]},
                          "business": {"mode": "multi", "epoch": epoch, "flow_id": flow_id}})
        routes = {}
        for flow in flows:
            key = json.dumps([flow["consumer"], flow["producer"], flow["service"], flow["protocol"], flow["path"]],
                             sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            route = routes.setdefault(key, {k: v for k, v in flow.items() if k != "business"})
            if "id" not in route:
                digest = hashlib.sha256(key.encode()).hexdigest()
                route.update(id=digest, color=f"hsl({int(digest[:8], 16) % 360} 65% 45%)", businesses=[])
            route["businesses"].append(flow["business"])
        for route in routes.values():
            route["businesses"].sort(key=lambda item: json.dumps(item, sort_keys=True))
            route["count"] = len(route["businesses"])
        return {"state": "partial" if missing or unmatched else "ready", "partial": bool(missing or unmatched),
                "round": str(collection.number), "missing_nodes": missing, "unmatched_count": unmatched,
                "flow_count": len(flows), "route_count": len(routes),
                "routes": sorted(routes.values(), key=lambda r: (r["service"], r["protocol"], r["id"]))}


def _agent_key(endpoint: dict) -> str:
    return "agent:" + json.dumps([endpoint["node"], endpoint["peer"]], ensure_ascii=False)


def _stations(route: dict) -> list[str]:
    return [_agent_key(route["consumer"]), *("node:" + node for node in route["path"]),
            _agent_key(route["producer"])]


def _rounded_path(points: list[tuple[float, float]]) -> str:
    """Round orthogonal corners without changing station endpoints."""
    points = [point for index, point in enumerate(points) if not index or point != points[index - 1]]
    result = [f"M{points[0][0]},{points[0][1]}"]
    for previous, corner, following in zip(points, points[1:], points[2:]):
        before = abs(corner[0] - previous[0]) + abs(corner[1] - previous[1])
        after = abs(following[0] - corner[0]) + abs(following[1] - corner[1])
        radius = min(12, before / 2, after / 2)
        entry = tuple(corner[i] + (previous[i] - corner[i]) * radius / before for i in (0, 1))
        leave = tuple(corner[i] + (following[i] - corner[i]) * radius / after for i in (0, 1))
        result.append(f"L{entry[0]},{entry[1]} Q{corner[0]},{corner[1]} {leave[0]},{leave[1]}")
    result.append(f"L{points[-1][0]},{points[-1][1]}")
    return " ".join(result)


class FlowMapRenderer:
    """Cache geometry independently of changing counts; safe for Waitress threads."""

    def __init__(self):
        self._lock = threading.Lock()
        self._signature = None
        self._geometry = None

    def render(self, snapshot: dict, service: str = "", protocol: str = "") -> dict:
        routes = [route for route in snapshot["routes"]
                  if (not service or route["service"] == service) and (not protocol or route["protocol"] == protocol)]
        signature = tuple(sorted(route["id"] for route in routes))
        with self._lock:
            if signature != self._signature:
                self._signature, self._geometry = signature, self._layout(routes)
            geometry = self._geometry
        return {**snapshot, "routes": routes, "visible_route_count": len(routes),
                "visible_flow_count": sum(route["count"] for route in routes),
                "services": sorted({route["service"] for route in snapshot["routes"]}),
                "svg": self._svg(routes, geometry)}

    @staticmethod
    def _layout(routes: list[dict]) -> tuple:
        graph = defaultdict(set)
        agents = {}
        for route in routes:
            for node in route["path"]:
                graph[node]
            for left, right in zip(route["path"], route["path"][1:]):
                graph[left].add(right)
                graph[right].add(left)
            for role in ("consumer", "producer"):
                key = _agent_key(route[role])
                agent = agents.setdefault(key, {**route[role], "roles": set()})
                agent["roles"].add(role)
        by_node = defaultdict(list)
        for key, agent in sorted(agents.items()):
            by_node[agent["node"]].append(key)
        positions = {}
        remaining = set(graph)
        base = 65
        while remaining:
            root = min(remaining)
            levels = {root: 0}
            queue = deque([root])
            while queue:
                node = queue.popleft()
                for peer in sorted(graph[node]):
                    if peer not in levels:
                        levels[peer] = levels[node] + 1
                        queue.append(peer)
            remaining.difference_update(levels)
            columns = defaultdict(list)
            for node in sorted(levels):
                columns[levels[node]].append(node)
            rows = max(map(len, columns.values()))
            for row in range(rows):
                row_nodes = [nodes[row] for nodes in columns.values() if row < len(nodes)]
                for node in row_nodes:
                    x, y = 225 + levels[node] * 430, base
                    positions["node:" + node] = (x, y)
                    for index, key in enumerate(by_node[node]):
                        side = -1 if agents[key]["roles"] == {"consumer"} else 1
                        positions[key] = (x + side * 120, y + 85 + index * 65)
                base += 160 + max((len(by_node[node]) for node in row_nodes), default=0) * 65
            base += 70
        lanes = defaultdict(list)
        for route in sorted(routes, key=lambda r: r["id"]):
            stations = _stations(route)
            for index, (left, right) in enumerate(zip(stations, stations[1:])):
                lanes[tuple(sorted((left, right)))].append((route["id"], index))
        offsets = {track: (index - (len(tracks) - 1) / 2) * 7
                   for tracks in lanes.values() for index, track in enumerate(tracks)}
        polylines = {}
        for route in routes:
            stations = _stations(route)
            segments = []
            for index, (left, right) in enumerate(zip(stations, stations[1:])):
                offset = offsets[route["id"], index]
                x1, y1 = positions[left]
                x2, y2 = positions[right]
                if x1 == x2:
                    bend = x1 + 38 + offset
                    points = [(x1, y1), (bend, y1), (bend, y2), (x2, y2)]
                else:
                    bend = (x1 + x2) / 2 + offset
                    points = [(x1, y1), (x1, y1 + offset), (bend, y1 + offset),
                              (bend, y2 + offset), (x2, y2 + offset), (x2, y2)]
                segments.append(points)
            polylines[route["id"]] = segments
        all_points = [point for segments in polylines.values() for points in segments for point in points]
        shift_x = max(0, 35 - min((x for x, y in all_points), default=35))
        shift_y = max(0, 35 - min((y for x, y in all_points), default=35))
        positions = {key: (x + shift_x, y + shift_y) for key, (x, y) in positions.items()}
        width = max(max((x for x, y in positions.values()), default=480) + 215,
                    max((x + shift_x for x, y in all_points), default=0) + 35)
        height = max(max((y for x, y in positions.values()), default=100) + 65,
                     max((y + shift_y for x, y in all_points), default=0) + 35)
        paths = {key: " ".join(_rounded_path([(x + shift_x, y + shift_y) for x, y in points])
                              for points in segments) for key, segments in polylines.items()}
        return positions, agents, paths, width, height

    @staticmethod
    def _svg(routes: list[dict], geometry: tuple) -> str:
        positions, agents, paths, width, height = geometry
        svg = ET.Element("svg", {"xmlns": "http://www.w3.org/2000/svg", "class": "flow-svg",
                                 "viewBox": f"0 0 {width} {height}", "role": "img",
                                 "aria-label": "Current relay routes"})
        layer = ET.SubElement(svg, "g", {"class": "flow-scene"})
        for route in routes:
            line = ET.SubElement(layer, "g", {"class": "flow-line", "data-route": route["id"],
                                               "tabindex": "0", "role": "button",
                                               "aria-label": f'{route["service"]} {route["protocol"]}: {route["count"]} connections'})
            ET.SubElement(line, "title").text = (
                f'{route["service"]} / {route["protocol"].upper()} · {route["count"]} connections\n'
                + route["consumer"]["peer"] + " → " + " → ".join(route["path"]) + " → " + route["producer"]["peer"])
            ET.SubElement(line, "path", {"d": paths[route["id"]], "class": "flow-hit"})
            ET.SubElement(line, "path", {"d": paths[route["id"]], "stroke": route["color"], "class": "flow-track"})
        for key, (x, y) in sorted(positions.items()):
            station = ET.SubElement(layer, "g", {"class": "flow-station", "transform": f"translate({x} {y})"})
            if key.startswith("node:"):
                name = key[5:]
                ET.SubElement(station, "circle", {"r": "7", "class": "flow-node"})
                title = "Node " + name
            else:
                agent = agents[key]
                name = agent["peer"]
                ET.SubElement(station, "rect", {"x": "-7", "y": "-7", "width": "14", "height": "14",
                                                "rx": "4", "class": "flow-agent"})
                title = f'Agent {name} · {agent["node"]} · {" / ".join(sorted(agent["roles"]))}'
            ET.SubElement(station, "title").text = title
            label = ET.SubElement(station, "text", {"x": "0", "y": "25", "text-anchor": "middle"})
            label.text = name if len(name) <= 25 else name[:22] + "…"
        return ET.tostring(svg, encoding="unicode")
