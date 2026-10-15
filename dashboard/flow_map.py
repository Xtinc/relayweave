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
import xml.etree.ElementTree as ET


ROUTE_COLORS = ("#4389bd", "#7764b7", "#369b91", "#c68b42", "#bd6884", "#607eaa",
                "#b87552", "#719b70", "#a56ca6", "#4f9caf", "#c47563", "#939044")


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
                route.update(id=digest, color=ROUTE_COLORS[int(digest[:8], 16) % len(ROUTE_COLORS)], businesses=[])
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


def _rounded_path(points: list[tuple[float, float]], offset: float = 0) -> str:
    """Round every actual turn after removing redundant points and tiny spurs."""
    points = _compact_path(points)
    normals = [(-(b[1] - a[1]) / math.dist(a, b), (b[0] - a[0]) / math.dist(a, b))
               for a, b in zip(points, points[1:])]
    start = tuple(points[0][i] + normals[0][i] * offset for i in (0, 1))
    result = [f"M{start[0]},{start[1]}"]
    for index, (previous, corner, following) in enumerate(zip(points, points[1:], points[2:])):
        before = math.dist(previous, corner)
        after = math.dist(corner, following)
        radius = min(24, before / 2, after / 2)
        entry = tuple(corner[i] + (previous[i] - corner[i]) * radius / before for i in (0, 1))
        leave = tuple(corner[i] + (following[i] - corner[i]) * radius / after for i in (0, 1))
        entry = tuple(entry[i] + normals[index][i] * offset for i in (0, 1))
        leave = tuple(leave[i] + normals[index + 1][i] * offset for i in (0, 1))
        bend = tuple(corner[i] + (normals[index][i] + normals[index + 1][i]) * offset for i in (0, 1))
        result.append(f"L{entry[0]},{entry[1]} Q{bend[0]},{bend[1]} {leave[0]},{leave[1]}")
    end = tuple(points[-1][i] + normals[-1][i] * offset for i in (0, 1))
    result.append(f"L{end[0]},{end[1]}")
    return " ".join(result)


def _metro_path(start: tuple[float, float], end: tuple[float, float],
                obstacles: list[tuple[float, float, float]], *, occupied=(), nodes=(),
                start_node=False, end_node=False) -> tuple[str, list[tuple[float, float]]]:
    """Prefer one simple metro track; avoid symbols without detouring around text."""
    x1, y1 = start
    x2, y2 = end

    def horizontal(bend):
        return _compact_path([start, (bend, y1), (bend, y2), end])

    def vertical(bend):
        return _compact_path([start, (x1, bend), (x2, bend), end])

    if y1 == y2:
        points = vertical(y1)
    elif x1 == x2:
        points = horizontal(x1)
    elif abs(x2 - x1) >= abs(y2 - y1):
        points = horizontal((x1 + x2) / 2)
    else:
        points = vertical((y1 + y2) / 2)

    def collisions(candidate):
        return sum(any(math.hypot(x - max(min(x, max(a[0], b[0])), min(a[0], b[0])),
                                  y - max(min(y, max(a[1], b[1])), min(a[1], b[1]))) < radius
                       for a, b in zip(candidate, candidate[1:])) for x, y, radius in obstacles)

    hits = collisions(points)
    if hits:
        xs = [min(x1, x2) - 48, (x1 + x2) / 2, max(x1, x2) + 48]
        ys = [min(y1, y2) - 48, (y1 + y2) / 2, max(y1, y2) + 48]
        if obstacles:
            xs.extend((min(x - r for x, y, r in obstacles) - 24, max(x + r for x, y, r in obstacles) + 24))
            ys.extend((min(y - r for x, y, r in obstacles) - 24, max(y + r for x, y, r in obstacles) + 24))
        candidates = [horizontal(bend) for bend in xs] + [vertical(bend) for bend in ys]
        points = min([points, *candidates], key=lambda candidate: (collisions(candidate), len(candidate),
                     sum(math.dist(a, b) for a, b in zip(candidate, candidate[1:]))))
    if occupied and _track_overlap(_visible_segments(points, nodes), occupied)[1]:
        def ports(point, other, is_node):
            x, y = point
            options = []
            for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                for lane in ((0, -12, 12, -6, 6, -18, 18, -21, 21) if is_node else (0,)):
                    bend = (x - dy * lane, y + dx * lane)
                    port = (bend[0] + dx * (32 if is_node else 14), bend[1] + dy * (32 if is_node else 14))
                    prefix = _compact_path([point, bend, port])
                    if not _track_overlap(_visible_segments(prefix, nodes), occupied)[0]:
                        options.append((math.dist(port, other), abs(lane), prefix))
            return [item[2] for item in sorted(options)]

        all_starts, all_ends = ports(start, end, start_node), ports(end, start, end_node)
        starts, ends = all_starts[:6], all_ends[:6]
        candidates = {tuple(points)}
        for prefix in starts:
            for suffix in ends:
                a, b = prefix[-1], suffix[-1]
                xs = {(a[0] + b[0]) / 2, a[0], b[0], min(a[0], b[0]) - 48, max(a[0], b[0]) + 48}
                ys = {(a[1] + b[1]) / 2, a[1], b[1], min(a[1], b[1]) - 48, max(a[1], b[1]) + 48}
                for first, second in occupied:
                    if first[0] == second[0]:
                        xs.update((first[0] - 12, first[0] + 12))
                    else:
                        ys.update((first[1] - 12, first[1] + 12))
                xs = sorted(xs, key=lambda x: (abs(x - (a[0] + b[0]) / 2), x))[:10]
                ys = sorted(ys, key=lambda y: (abs(y - (a[1] + b[1]) / 2), y))[:10]
                for x in xs:
                    candidates.add(tuple(_compact_path([*prefix, (x, a[1]), (x, b[1]), *reversed(suffix)])))
                for y in ys:
                    candidates.add(tuple(_compact_path([*prefix, (a[0], y), (b[0], y), *reversed(suffix)])))
        best_score = None
        for candidate in sorted(candidates, key=lambda p: (len(p), sum(math.dist(a, b) for a, b in zip(p, p[1:])), p)):
            exact, nearby = _track_overlap(_visible_segments(candidate, nodes), occupied)
            symbols = collisions(candidate)
            score = (bool(exact) + bool(symbols), exact + symbols * 1000, nearby, len(candidate),
                     sum(math.dist(a, b) for a, b in zip(candidate, candidate[1:])))
            if best_score is None or score < best_score:
                points, best_score = list(candidate), score
            if not exact and not nearby and not symbols:
                break
        if _track_overlap(_visible_segments(points, nodes), occupied)[0]:
            routed = _corridor_path(all_starts, all_ends, occupied,
                                    [*obstacles, (*start, 24 if start_node else 8),
                                     (*end, 24 if end_node else 8)])
            if routed is not None:
                points = routed
    return _rounded_path(points), points


def _corridor_path(starts, ends, occupied, obstacles):
    """Find a clear corridor when a simple two-turn track is insufficient."""
    if not starts or not ends:
        return None
    boxes = [(x - r, y - r, x + r, y + r) for x, y, r in obstacles]
    xs = {x for path in [*starts, *ends] for x, y in path}
    ys = {y for path in [*starts, *ends] for x, y in path}
    for left, top, right, bottom in boxes:
        xs.update((left, right)); ys.update((top, bottom))
    for a, b in occupied:
        xs.update((a[0], b[0], a[0] - 12, a[0] + 12))
        ys.update((a[1], b[1], a[1] - 12, a[1] + 12))
    xs.update((min(xs) - 48, max(xs) + 48))
    ys.update((min(ys) - 48, max(ys) + 48))
    xs, ys = sorted(xs), sorted(ys)
    xi, yi = {x: i for i, x in enumerate(xs)}, {y: i for i, y in enumerate(ys)}
    rows = [[(left, right) for left, top, right, bottom in boxes if top < y < bottom] +
            [(a[0], b[0]) for a, b in occupied if a[1] == b[1] == y] for y in ys]
    columns = [[(top, bottom) for left, top, right, bottom in boxes if left < x < right] +
               [(a[1], b[1]) for a, b in occupied if a[0] == b[0] == x] for x in xs]
    goals = {(xi[path[-1][0]], yi[path[-1][1]]): path for path in ends}
    min_x, max_x = min(path[-1][0] for path in ends), max(path[-1][0] for path in ends)
    min_y, max_y = min(path[-1][1] for path in ends), max(path[-1][1] for path in ends)
    queue, costs, previous, roots = [], {}, {}, {}
    for path in starts:
        x, y = path[-1]
        axis = int(path[-2][0] == x)
        state = (xi[x], yi[y], axis)
        cost = sum(math.dist(a, b) for a, b in zip(path, path[1:])) + 64 * (len(path) - 2)
        costs[state], roots[state], previous[state] = cost, path, None
        heapq.heappush(queue, (cost, cost, state))
    while queue:
        _, cost, current = heapq.heappop(queue)
        if cost != costs[current]:
            continue
        ix, iy, direction = current
        if (ix, iy) in goals:
            suffix = goals[ix, iy]
            points = []
            while previous[current] is not None:
                points.append((xs[current[0]], ys[current[1]]))
                current = previous[current]
            return _compact_path([*roots[current], *reversed(points), *reversed(suffix)])
        for nx, ny, axis in ((ix - 1, iy, 0), (ix + 1, iy, 0), (ix, iy - 1, 1), (ix, iy + 1, 1)):
            if not (0 <= nx < len(xs) and 0 <= ny < len(ys)):
                continue
            midpoint = (ys[iy] + ys[ny]) / 2 if axis else (xs[ix] + xs[nx]) / 2
            if any(low < midpoint < high for low, high in (columns[ix] if axis else rows[iy])):
                continue
            neighbor = (nx, ny, axis)
            candidate = cost + abs(xs[nx] - xs[ix]) + abs(ys[ny] - ys[iy]) + (64 if axis != direction else 0)
            if candidate < costs.get(neighbor, math.inf):
                costs[neighbor], previous[neighbor] = candidate, current
                heuristic = max(min_x - xs[nx], 0, xs[nx] - max_x) + max(min_y - ys[ny], 0, ys[ny] - max_y)
                heapq.heappush(queue, (candidate + heuristic, candidate, neighbor))
    return None


def _visible_segments(points, nodes):
    """Only station interiors may hide the join between independent tracks."""
    segments = []
    for a, b in zip(points, points[1:]):
        horizontal = a[1] == b[1]
        fixed = a[1] if horizontal else a[0]
        intervals = [(min(a[0], b[0]), max(a[0], b[0])) if horizontal else (min(a[1], b[1]), max(a[1], b[1]))]
        for x, y in nodes:
            distance = abs(fixed - (y if horizontal else x))
            if distance >= 24:
                continue
            extent = math.sqrt(24**2 - distance**2)
            low, high = (x if horizontal else y) - extent, (x if horizontal else y) + extent
            intervals = [(left, right) for start, end in intervals
                         for left, right in ((start, min(end, low)), (max(start, high), end)) if right > left]
        segments.extend((((low, fixed), (high, fixed)) if horizontal else ((fixed, low), (fixed, high)))
                        for low, high in intervals)
    return segments


def _track_overlap(segments, occupied):
    exact = nearby = 0
    for a, b in segments:
        horizontal = a[1] == b[1]
        axis = 0 if horizontal else 1
        for c, d in occupied:
            if horizontal != (c[1] == d[1]):
                continue
            gap = abs(a[1 - axis] - c[1 - axis])
            if gap >= 10:
                continue
            overlap = max(0, min(b[axis], d[axis]) - max(a[axis], c[axis]))
            if gap < 1e-6:
                exact += overlap
            nearby += overlap * (10 - gap) / 10
    return exact, nearby


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
        shared = []
        for (left, right), (path, points) in sorted(geometry[3].items()):
            contributions = [route for route in sorted(routes, key=lambda route: route["id"]) if (left, right) in {
                tuple(sorted(pair)) for pair in zip(_stations(route), _stations(route)[1:])}]
            count = sum(route["count"] for route in contributions)
            width = 3 + .8 * math.sqrt(min(count, 50))
            shared.append({"id": hashlib.sha256(json.dumps([left, right]).encode()).hexdigest()[:16],
                           "count": count, "width": width, "path": path, "points": points,
                           "routes": [{"id": route["id"], "color": route["color"], "count": route["count"],
                                       "share": route["count"] / count} for route in contributions]})
        return {**snapshot, "routes": routes, "visible_route_count": len(routes),
                "visible_flow_count": sum(route["count"] for route in routes),
                "services": sorted({route["service"] for route in snapshot["routes"]}),
                "nodes": stations,
                "shared_tracks": [{key: value for key, value in track.items() if key not in {"points", "path"}}
                                  for track in shared],
                "svg": self._svg(routes, geometry, stations, shared)}

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
        pairs = {tuple(sorted((left, right))) for route in routes
                 for left, right in zip(_stations(route), _stations(route)[1:])}
        hops = {}
        all_points = []
        occupied = []
        node_centers = [point for key, point in positions.items() if key.startswith("node:")]
        for left, right in sorted(pairs):
            obstacles = [(x, y, 36 if key.startswith("node:") else 18)
                         for key, (x, y) in positions.items() if key not in {left, right}]
            path, points = _metro_path(positions[left], positions[right], obstacles, occupied=occupied,
                                       nodes=node_centers, start_node=left.startswith("node:"),
                                       end_node=right.startswith("node:"))
            hops[left, right] = (path, points)
            occupied.extend(_visible_segments(points, node_centers))
            all_points.extend(points)
        bounds = [box for key, point in positions.items() for box in _station_boxes(key, point, agents)]
        width = max([box[2] for box in bounds] + [x for x, y in all_points], default=480) + 28
        height = max([box[3] for box in bounds] + [y for x, y in all_points], default=100) + 28
        paths = {route["id"]: " ".join(hops[tuple(sorted((left, right)))][0]
                                      for left, right in zip(_stations(route), _stations(route)[1:]))
                 for route in routes}
        return positions, agents, paths, hops, width, height

    @staticmethod
    def _svg(routes: list[dict], geometry: tuple, stations: list[dict], shared: list[dict]) -> str:
        positions, agents, paths, hops, width, height = geometry
        nodes = {station["node_id"]: station for station in stations}
        svg = ET.Element("svg", {"xmlns": "http://www.w3.org/2000/svg", "class": "flow-svg",
                                 "viewBox": f"0 0 {width} {height}", "width": str(width), "height": str(height),
                                 "preserveAspectRatio": "xMidYMid meet", "role": "group",
                                 "aria-label": "Current relay routes and Node bandwidth headroom"})
        layer = ET.SubElement(svg, "g", {"class": "flow-scene"})
        tracks = ET.SubElement(layer, "g", {"class": "flow-routes"})
        for track in shared:
            group = ET.SubElement(tracks, "g", {"class": "flow-shared-track", "data-track": track["id"],
                                                "data-routes": json.dumps([route["id"] for route in track["routes"]])})
            ET.SubElement(group, "title").text = f'{track["count"]} connections across {len(track["routes"])} routes'
            ET.SubElement(group, "path", {"d": track["path"], "class": "flow-track",
                                           "stroke": "var(--flow-ring)", "stroke-width": f'{track["width"]:.6f}'})
            cursor = -track["width"] / 2
            for contribution in track["routes"]:
                band_width = track["width"] * contribution["share"]
                ET.SubElement(group, "path", {"d": _rounded_path(track["points"], cursor + band_width / 2),
                                               "class": "flow-band", "data-route": contribution["id"],
                                               "stroke": contribution["color"], "stroke-width": f"{band_width:.6f}"})
                cursor += band_width
        for route in routes:
            line = ET.SubElement(tracks, "g", {"class": "flow-line", "data-route": route["id"],
                                               "tabindex": "0", "role": "button",
                                               "aria-label": f'{route["service"]} {route["protocol"]}: {route["count"]} connections'})
            ET.SubElement(line, "title").text = (
                f'{route["service"]} / {route["protocol"].upper()} · {route["count"]} connections\n'
                + route["consumer"]["peer"] + " → " + " → ".join(route["path"]) + " → " + route["producer"]["peer"])
            ET.SubElement(line, "path", {"d": paths[route["id"]], "class": "flow-hit"})
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
                ET.SubElement(station, "circle", {"r": "24", "class": "flow-node-halo"})
                ET.SubElement(station, "circle", {"r": "18", "class": "flow-node"})
                ET.SubElement(station, "circle", {"r": "20", "class": "flow-capacity-track"})
                if available is not None:
                    circumference = 2 * math.pi * 20
                    ET.SubElement(station, "circle", {"r": "20", "class": "flow-capacity",
                                                      "transform": "rotate(-90)",
                                                      "stroke-dasharray": f"{circumference * (1 - available):.3f} {circumference:.3f}"})
                ET.SubElement(station, "text", {"y": "4", "text-anchor": "middle",
                                                "class": "flow-headroom"}).text = headroom
                title = f"Node {name}\nBandwidth headroom: {headroom}"
            else:
                station.set("class", "flow-station flow-agent-station")
                agent = agents[key]
                name = agent["peer"]
                ET.SubElement(station, "rect", {"x": "-7", "y": "-7", "width": "14", "height": "14",
                                                "rx": "3", "class": "flow-agent"})
                title = f'Agent {name} · {agent["node"]} · {" / ".join(sorted(agent["roles"]))}'
            ET.SubElement(station, "title").text = title
            label = ET.SubElement(station, "text", {"x": "0", "y": "38" if key.startswith("node:") else "25",
                                                    "text-anchor": "middle", "class": "flow-label"})
            label.text = _label(name)[0]
        return ET.tostring(svg, encoding="unicode")
