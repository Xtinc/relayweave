"""Current relay collection, route grouping and deterministic metro-map SVG.

All input records describe Ready business instances. NodeLink state and
predicted routes are deliberately not used to infer an active business.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
import hashlib
import heapq
import json
import math
import threading
from bisect import bisect_right
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


class _FlowRouter:
    """Route on shared orthogonal corridors, leaving station labels clear."""

    def __init__(self, positions: dict, agents: dict, parallel: int):
        boxes = [box for key, point in positions.items() for box in _station_boxes(key, point, agents)]
        xs = {value for box in boxes for value in (box[0], box[2])}
        ys = {value for box in boxes for value in (box[1], box[3])}
        # A corridor on either side of every station gives parallel routes room.
        for key, (x, y) in positions.items():
            radius = 34 if key.startswith("node:") else 17
            xs.update((x - radius, x + radius))
            ys.add(y)
            for lane in range(1, parallel + 1):
                ys.update((y - radius - lane * 12, y + 66 + lane * 12))
        xs.update((min(xs) - 24, max(xs) + 24))
        ys.update((min(ys) - 24, max(ys) + 24))
        self.xs, self.ys = sorted(xs), sorted(ys)
        self.xindex = {value: index for index, value in enumerate(self.xs)}
        self.yindex = {value: index for index, value in enumerate(self.ys)}
        self.rows = [self._merge((left, right) for left, top, right, bottom in boxes if top < y < bottom)
                     for y in self.ys]
        self.columns = [self._merge((top, bottom) for left, top, right, bottom in boxes if left < x < right)
                        for x in self.xs]
        self.used = defaultdict(int)

    @staticmethod
    def _merge(intervals):
        merged = []
        for low, high in sorted(intervals):
            if merged and low < merged[-1][1]:
                merged[-1] = (merged[-1][0], max(high, merged[-1][1]))
            else:
                merged.append((low, high))
        return merged

    @staticmethod
    def _blocked(intervals, value):
        index = bisect_right(intervals, (value, math.inf)) - 1
        return index >= 0 and intervals[index][0] < value < intervals[index][1]

    def path(self, left: str, right: str, positions: dict) -> list[tuple[float, float]]:
        # Always use the same ports for reverse traffic between a station pair.
        if left > right:
            return list(reversed(self.path(right, left, positions)))
        x1, y1 = positions[left]
        x2, y2 = positions[right]
        side = 1 if x2 >= x1 else -1
        start = (x1 + side * (34 if left.startswith("node:") else 17), y1)
        end = (x2 - side * (34 if right.startswith("node:") else 17), y2)
        source = (self.xindex[start[0]], self.yindex[start[1]], 0)
        target = (self.xindex[end[0]], self.yindex[end[1]])
        queue = [(abs(start[0] - end[0]) + abs(start[1] - end[1]), 0, source)]
        costs, previous = {source: 0}, {}
        while queue:
            _, cost, current = heapq.heappop(queue)
            if cost != costs[current]:
                continue
            ix, iy, direction = current
            if (ix, iy) == target:
                break
            for nx, ny, axis in ((ix - 1, iy, 0), (ix + 1, iy, 0), (ix, iy - 1, 1), (ix, iy + 1, 1)):
                if not (0 <= nx < len(self.xs) and 0 <= ny < len(self.ys)):
                    continue
                x, y = self.xs[nx], self.ys[ny]
                midpoint = (self.ys[iy] + y) / 2 if axis else (self.xs[ix] + x) / 2
                if self._blocked(self.columns[ix] if axis else self.rows[iy], midpoint):
                    continue
                edge = tuple(sorted(((ix, iy), (nx, ny))))
                length = abs(x - self.xs[ix]) + abs(y - self.ys[iy])
                candidate = cost + length * (1 + .8 * self.used.get(edge, 0)) + (18 if axis != direction else 0)
                neighbor = (nx, ny, axis)
                if candidate < costs.get(neighbor, math.inf):
                    costs[neighbor], previous[neighbor] = candidate, current
                    heuristic = abs(x - end[0]) + abs(y - end[1])
                    heapq.heappush(queue, (candidate + heuristic, candidate, neighbor))
        else:
            raise ValueError("Flow station ports must have an unobstructed route")
        points = []
        while current != source:
            ix, iy, _ = current
            points.append((self.xs[ix], self.ys[iy]))
            parent = previous[current]
            self.used[tuple(sorted((current[:2], parent[:2])))] += 1
            current = parent
        points.append(start)
        points.reverse()
        # Remove grid subdivisions so rounded corners only appear at real turns.
        compact = [points[0]]
        for point, following in zip(points[1:-1], points[2:]):
            if not (compact[-1][0] == point[0] == following[0] or compact[-1][1] == point[1] == following[1]):
                compact.append(point)
        compact.append(end)
        return [(x1, y1), *compact, (x2, y2)]


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

    @staticmethod
    def _layout(routes: list[dict], node_ids=()) -> tuple:
        graph = defaultdict(set)
        for node in node_ids:
            graph[node]
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
        blocks = []
        remaining = set(graph)
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
            ordered = sorted(levels, key=lambda node: (levels[node], node))
            column_count = max(1, math.ceil(math.sqrt(len(ordered))))
            slots = {}
            column_left, column_right = [100] * column_count, [100] * column_count
            for index, node in enumerate(ordered):
                sides = defaultdict(list)
                for key in by_node[node]:
                    sides[-1 if agents[key]["roles"] == {"consumer"} else 1].append(key)
                count = max(map(len, sides.values()), default=0)
                agent_columns = max(1, round(math.sqrt(count * .5)))
                column = index % column_count
                if index // column_count % 2:
                    column = column_count - column - 1
                column_left[column] = max(column_left[column], min(len(sides[-1]), agent_columns) * 200)
                column_right[column] = max(column_right[column], min(len(sides[1]), agent_columns) * 200)
                slots[node] = (column, sides, agent_columns)
            column_x = [sum(column_left[:index]) + sum(column_right[:index]) + index * 32 + column_left[index]
                        for index in range(column_count)]
            local = {}
            base = 34
            for start in range(0, len(ordered), column_count):
                row_nodes = ordered[start:start + column_count]
                row_height = 112
                for node in row_nodes:
                    column, sides, agent_columns = slots[node]
                    x, y = column_x[column], base
                    local["node:" + node] = (x, y)
                    for side, keys in sides.items():
                        for index, key in enumerate(keys):
                            local[key] = (x + side * (100 + index % agent_columns * 200),
                                          y + 96 + index // agent_columns * 56)
                        if keys:
                            row_height = max(row_height, 156 + (math.ceil(len(keys) / agent_columns) - 1) * 56)
                base += row_height
            bounds = [box for key, point in local.items() for box in _station_boxes(key, point, agents)]
            left, top = min(box[0] for box in bounds), min(box[1] for box in bounds)
            width = max(box[2] for box in bounds) - left + 32
            height = max(box[3] for box in bounds) - top + 32
            blocks.append((width, height, root, {key: (x - left + 16, y - top + 16)
                                                 for key, (x, y) in local.items()}))
        # Pack disconnected components together instead of stacking them in one tall column.
        total_area = sum((width + 32) * (height + 32) for width, height, _, _ in blocks)
        target_width = max(max((block[0] for block in blocks), default=480), math.sqrt(total_area * 2.2))
        shelves = []
        positions = {}
        for width, height, _, local in sorted(blocks, key=lambda block: (-block[1], -block[0], block[2])):
            shelf = next((item for item in shelves if item[0] + width <= target_width and height <= item[2]), None)
            if shelf is None:
                shelf = [0, sum(item[2] + 32 for item in shelves), height]
                shelves.append(shelf)
            for key, (x, y) in local.items():
                positions[key] = (x + shelf[0], y + shelf[1])
            shelf[0] += width + 32
        lanes = defaultdict(list)
        for route in sorted(routes, key=lambda r: r["id"]):
            for left, right in zip(_stations(route), _stations(route)[1:]):
                lanes[tuple(sorted((left, right)))].append(route["id"])
        router = _FlowRouter(positions, agents, min(4, max(map(len, lanes.values()), default=1))) if routes else None
        polylines = {}
        for route in sorted(routes, key=lambda route: route["id"]):
            stations = _stations(route)
            polylines[route["id"]] = [router.path(left, right, positions)
                                     for left, right in zip(stations, stations[1:])]
        all_points = [point for segments in polylines.values() for points in segments for point in points]
        bounds = [box for key, point in positions.items() for box in _station_boxes(key, point, agents)]
        min_x = min([box[0] for box in bounds] + [x for x, y in all_points], default=0)
        min_y = min([box[1] for box in bounds] + [y for x, y in all_points], default=0)
        shift_x, shift_y = 24 - min_x, 24 - min_y
        positions = {key: (x + shift_x, y + shift_y) for key, (x, y) in positions.items()}
        width = max([box[2] for box in bounds] + [x for x, y in all_points], default=480) + shift_x + 24
        height = max([box[3] for box in bounds] + [y for x, y in all_points], default=100) + shift_y + 24
        paths = {key: " ".join(_rounded_path([(x + shift_x, y + shift_y) for x, y in points])
                              for points in segments) for key, segments in polylines.items()}
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
