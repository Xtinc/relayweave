"""Current relay collection, route grouping and deterministic metro-map SVG.

All input records describe Ready business instances. NodeLink state and
predicted routes are deliberately not used to infer an active business.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
import hashlib
import json
import math
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


def _label(name: str) -> tuple[str, float]:
    """Bound labels by display width, including wide Unicode Node names."""
    text, width = "", 0
    for character in name:
        advance = (12 if not character.isascii() or character in "MW@" else
                   3.6 if character in "il.,:;!'| " else 9 if character.isupper() else 7.2)
        if width + advance > 168:
            return text + "…", width + 12
        text += character
        width += advance
    return text, width


def _station_boxes(key: str, point: tuple[float, float], agents: dict) -> list[tuple]:
    x, y = point
    is_node = key.startswith("node:")
    name = key[5:] if is_node else agents[key]["peer"]
    half_width = _label(name)[1] / 2 + 10
    label_y = y + (46 if is_node else 25)
    radius = 34 if is_node else 17
    return [(x - radius, y - radius, x + radius, y + radius),
            (x - half_width, label_y - 21, x + half_width, label_y + 10)]


def _compact_path(points: list[tuple[float, float]]) -> list[tuple[float, float]]:
    compact = []
    for point in points:
        if compact and point == compact[-1]:
            continue
        while len(compact) >= 2 and (compact[-2][0] == compact[-1][0] == point[0]
                                     or compact[-2][1] == compact[-1][1] == point[1]):
            compact.pop()
        compact.append(point)
    return compact


def _rounded_path(points: list[tuple[float, float]]) -> str:
    """Round every actual turn after removing redundant points and tiny spurs."""
    points = _compact_path(points)
    result = [f"M{points[0][0]},{points[0][1]}"]
    for previous, corner, following in zip(points, points[1:], points[2:]):
        before = math.dist(previous, corner)
        after = math.dist(corner, following)
        radius = min(24, before / 2, after / 2)
        entry = tuple(corner[i] + (previous[i] - corner[i]) * radius / before for i in (0, 1))
        leave = tuple(corner[i] + (following[i] - corner[i]) * radius / after for i in (0, 1))
        result.append(f"L{entry[0]},{entry[1]} Q{corner[0]},{corner[1]} {leave[0]},{leave[1]}")
    result.append(f"L{points[-1][0]},{points[-1][1]}")
    return " ".join(result)


def _metro_path(start: tuple[float, float], end: tuple[float, float], offset: float,
                obstacles: list[tuple[float, float, float]]) -> tuple[str, list[tuple[float, float]]]:
    """Prefer one simple metro track; avoid symbols without detouring around text."""
    x1, y1 = start
    x2, y2 = end
    offset = max(-96, min(96, offset))

    def horizontal(bend):
        return _compact_path([start, (bend, y1), (bend, y2), end])

    def vertical(bend):
        return _compact_path([start, (x1, bend), (x2, bend), end])

    if y1 == y2:
        points = vertical(y1 + offset)
    elif x1 == x2:
        points = horizontal(x1 + offset)
    elif abs(x2 - x1) >= abs(y2 - y1):
        points = horizontal((x1 + x2) / 2 + offset)
    else:
        points = vertical((y1 + y2) / 2 + offset)

    def collisions(candidate):
        return sum(any(math.hypot(x - max(min(x, max(a[0], b[0])), min(a[0], b[0])),
                                  y - max(min(y, max(a[1], b[1])), min(a[1], b[1]))) < radius
                       for a, b in zip(candidate, candidate[1:])) for x, y, radius in obstacles)

    hits = collisions(points)
    if hits:
        xs = [min(x1, x2) - 48, (x1 + x2) / 2 + offset, max(x1, x2) + 48]
        ys = [min(y1, y2) - 48, (y1 + y2) / 2 + offset, max(y1, y2) + 48]
        if obstacles:
            xs.extend((min(x - r for x, y, r in obstacles) - 24, max(x + r for x, y, r in obstacles) + 24))
            ys.extend((min(y - r for x, y, r in obstacles) - 24, max(y + r for x, y, r in obstacles) + 24))
        candidates = [horizontal(bend) for bend in xs] + [vertical(bend) for bend in ys]
        points = min([points, *candidates], key=lambda candidate: (collisions(candidate), len(candidate),
                     sum(math.dist(a, b) for a, b in zip(candidate, candidate[1:]))))
    return _rounded_path(points), points


class FlowMapRenderer:
    """Cache geometry independently of changing counts; safe for Waitress threads."""

    def __init__(self):
        self._lock = threading.Lock()
        self._signature = None
        self._geometry = None
        self._positions = {}
        self._known_agents = {}
        self._next_node_slot = 0

    def render(self, snapshot: dict, service: str = "", protocol: str = "", *, topology: dict | None = None) -> dict:
        routes = [route for route in snapshot["routes"]
                  if (not service or route["service"] == service) and (not protocol or route["protocol"] == protocol)]
        members = {node["node_id"]: node for node in (topology or {}).get("nodes", [])}
        node_ids = sorted(set(members) | {node for route in routes for node in route["path"]})
        routes_by_node = defaultdict(list)
        for route in routes:
            for node in route["path"]:
                routes_by_node[node].append(route)
        stations = []
        for node in node_ids:
            station_routes = routes_by_node[node]
            stations.append({"node_id": node, "available": (members.get(node, {}).get("available")
                                                            if not (topology or {}).get("stale") else None),
                             "connection_count": (sum(route["count"] for route in station_routes)
                                                  if snapshot["state"] in {"ready", "partial"} else None),
                             "partial": snapshot["partial"], "route_ids": [route["id"] for route in station_routes]})
        signature = (tuple(sorted(route["id"] for route in routes)), tuple(node_ids))
        with self._lock:
            if signature != self._signature:
                self._signature, self._geometry = signature, self._layout(routes, node_ids)
            geometry = self._geometry
        return {**snapshot, "routes": routes, "visible_route_count": len(routes),
                "visible_flow_count": sum(route["count"] for route in routes),
                "services": sorted({route["service"] for route in snapshot["routes"]}),
                "nodes": stations, "svg": self._svg(routes, geometry, stations)}

    def _layout(self, routes: list[dict], node_ids=()) -> tuple:
        agents = {}
        for route in routes:
            for role in ("consumer", "producer"):
                key = _agent_key(route[role])
                agent = agents.setdefault(key, {**route[role], "roles": set()})
                agent["roles"].add(role)
        # Keep Node reservations even after removal, so a returning Node gets its place back.
        self._positions = {key: point for key, point in self._positions.items()
                           if key.startswith("node:") or key in agents}
        reserved_agents = {**self._known_agents, **agents}
        boxes = [box for key, point in self._positions.items()
                 for box in _station_boxes(key, point, reserved_agents)]

        def vacant(key, point):
            return all(not (left < other_right and other_left < right and top < other_bottom and other_top < bottom)
                       for left, top, right, bottom in _station_boxes(key, point, reserved_agents)
                       for other_left, other_top, other_right, other_bottom in boxes)

        def reserve(key, point):
            self._positions[key] = point
            boxes.extend(_station_boxes(key, point, reserved_agents))

        for node in sorted(node_ids):
            key = "node:" + node
            if key in self._positions:
                continue
            while True:
                index = self._next_node_slot
                self._next_node_slot += 1
                ring = math.isqrt(index)
                offset = index - ring * ring
                column, row = (ring, offset) if offset < ring else (offset - ring, ring)
                point = (240 + column * 240, 160 + row * 160)
                if vacant(key, point):
                    reserve(key, point)
                    break
        pending = defaultdict(deque)
        for key, agent in sorted(agents.items()):
            if key not in self._positions:
                pending[agent["node"]].append(key)
        while pending:
            for node in sorted(list(pending)):
                key = pending[node].popleft()
                if not pending[node]:
                    del pending[node]
                x, y = self._positions["node:" + node]
                side = -1 if agents[key]["roles"] == {"consumer"} else 1
                radius = 0
                while True:
                    candidates = [(x + side * 100 + column * 200, y + 96 + row * 56)
                                  for row in range(radius + 1) for column in range(-radius, radius + 1)
                                  if max(abs(column), row) == radius]
                    candidates.sort(key=lambda point: ((point[0] - x)**2 + (point[1] - y)**2, point))
                    point = next((point for point in candidates if point[0] >= 120 and vacant(key, point)), None)
                    if point is not None:
                        reserve(key, point)
                        break
                    radius += 1
        self._known_agents = agents
        positions = {key: self._positions[key] for key in [*("node:" + node for node in node_ids), *agents]}
        lanes = defaultdict(list)
        for route in sorted(routes, key=lambda route: route["id"]):
            stations = _stations(route)
            for index, (left, right) in enumerate(zip(stations, stations[1:])):
                lanes[tuple(sorted((left, right)))].append((route["id"], index))
        segments = defaultdict(dict)
        all_points = []
        for (left, right), tracks in sorted(lanes.items()):
            obstacles = [(x, y, 36 if key.startswith("node:") else 18)
                         for key, (x, y) in positions.items() if key not in {left, right}]
            for lane, (route_id, index) in enumerate(tracks):
                path, points = _metro_path(positions[left], positions[right], (lane - (len(tracks) - 1) / 2) * 14,
                                           obstacles)
                segments[route_id][index] = path
                all_points.extend(points)
        bounds = [box for key, point in positions.items() for box in _station_boxes(key, point, agents)]
        width = max([box[2] for box in bounds] + [x for x, y in all_points], default=480) + 28
        height = max([box[3] for box in bounds] + [y for x, y in all_points], default=100) + 28
        paths = {key: " ".join(path for _, path in sorted(parts.items())) for key, parts in segments.items()}
        return positions, agents, paths, width, height

    @staticmethod
    def _svg(routes: list[dict], geometry: tuple, stations: list[dict]) -> str:
        positions, agents, paths, width, height = geometry
        nodes = {station["node_id"]: station for station in stations}
        svg = ET.Element("svg", {"xmlns": "http://www.w3.org/2000/svg", "class": "flow-svg",
                                 "viewBox": f"0 0 {width} {height}", "width": str(width), "height": str(height),
                                 "preserveAspectRatio": "xMidYMid meet", "role": "group",
                                 "aria-label": "Current relay routes and Node bandwidth headroom"})
        layer = ET.SubElement(svg, "g", {"class": "flow-scene"})
        for route in routes:
            line = ET.SubElement(layer, "g", {"class": "flow-line", "data-route": route["id"],
                                               "tabindex": "0", "role": "button",
                                               "aria-label": f'{route["service"]} {route["protocol"]}: {route["count"]} connections'})
            ET.SubElement(line, "title").text = (
                f'{route["service"]} / {route["protocol"].upper()} · {route["count"]} connections\n'
                + route["consumer"]["peer"] + " → " + " → ".join(route["path"]) + " → " + route["producer"]["peer"])
            ET.SubElement(line, "path", {"d": paths[route["id"]], "class": "flow-hit"})
            stroke_width = 3 + .8 * math.sqrt(min(route["count"], 50))
            ET.SubElement(line, "path", {"d": paths[route["id"]], "stroke": route["color"],
                                        "stroke-width": f"{stroke_width:.2f}", "class": "flow-track"})
        for key, (x, y) in sorted(positions.items()):
            station = ET.SubElement(layer, "g", {"class": "flow-station", "transform": f"translate({x} {y})"})
            if key.startswith("node:"):
                name = key[5:]
                node = nodes[name]
                available = node["available"]
                headroom = "—" if available is None else f"{available:.0%}"
                state = ("unknown" if available is None else "critical" if available < .1 else
                         "warning" if available < .3 else "good")
                station.set("data-node", name)
                station.set("aria-label", f"Node {name}: {headroom} bandwidth headroom")
                station.set("class", f"flow-station flow-node-station {state}")
                ET.SubElement(station, "circle", {"r": "24", "class": "flow-node"})
                if available is not None:
                    circumference = 2 * math.pi * 24
                    ET.SubElement(station, "circle", {"r": "24", "class": "flow-capacity",
                                                      "transform": "rotate(-90)",
                                                      "stroke-dasharray": f"{circumference * (1 - available):.3f} {circumference:.3f}"})
                ET.SubElement(station, "text", {"y": "4", "text-anchor": "middle",
                                                "class": "flow-headroom"}).text = headroom
                title = f"Node {name}\nBandwidth headroom: {headroom}"
            else:
                agent = agents[key]
                name = agent["peer"]
                ET.SubElement(station, "rect", {"x": "-7", "y": "-7", "width": "14", "height": "14",
                                                "rx": "4", "class": "flow-agent"})
                title = f'Agent {name} · {agent["node"]} · {" / ".join(sorted(agent["roles"]))}'
            ET.SubElement(station, "title").text = title
            label = ET.SubElement(station, "text", {"x": "0", "y": "46" if key.startswith("node:") else "25",
                                                    "text-anchor": "middle", "class": "flow-label"})
            label.text = _label(name)[0]
        return ET.tostring(svg, encoding="unicode")
