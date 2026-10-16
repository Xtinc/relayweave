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
from typing import NamedTuple
import xml.etree.ElementTree as ET


ROUTE_COLORS = ("#4389bd", "#7764b7", "#369b91", "#c68b42", "#bd6884", "#607eaa",
                "#b87552", "#719b70", "#a56ca6", "#4f9caf", "#c47563", "#939044")

# Geometry is expressed in SVG units; zoom scales stations and tracks together.
TRACK_BASE_WIDTH = 3
TRACK_WIDTH_SCALE = .8
TRACK_CONNECTION_CAP = 100
TRACK_MAX_WIDTH = TRACK_BASE_WIDTH + TRACK_WIDTH_SCALE * math.sqrt(TRACK_CONNECTION_CAP)
TURN_RADIUS = 24
NODE_RING_RADIUS = 18
NODE_RING_WIDTH = 5
NODE_HALO_RADIUS = NODE_RING_RADIUS + 4
NODE_CENTER_RADIUS = NODE_RING_RADIUS - 1.5
NODE_PORT_LENGTH = 64
AGENT_VERTICAL_BIAS = .6
AGENT_SIZE = 14
AGENT_OUTLINE_WIDTH = 1.6
CORRIDOR_GAP = TRACK_MAX_WIDTH + 4


def _symbol_radius(key):
    body = (NODE_RING_RADIUS + NODE_RING_WIDTH / 2 if key.startswith("node:")
            else (AGENT_SIZE + AGENT_OUTLINE_WIDTH) / 2)
    clearance = body + TRACK_MAX_WIDTH / 2
    # Reserve clearance for both straight strokes and the inward cut of a
    # quadratic corner fillet; count changes cannot invalidate the corridor.
    return math.ceil(max(clearance + 1, TURN_RADIUS / 4 + clearance / math.sqrt(2) + 1))


class MetroGeometry(NamedTuple):
    positions: dict
    agents: dict
    hops: dict
    width: float
    height: float


class MetroRoutingError(ValueError):
    """A requested pair of ports has no valid corridor."""


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


def _route_hops(route):
    stations = _stations(route)
    return [tuple(sorted((left, right))) for left, right in zip(stations, stations[1:])]


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
    return _primitive_path(_path_primitives(points), offset)


def _path_primitives(points):
    """Round first, so branches share only the straight track before a fillet."""
    points = _compact_path(points)
    cursor = points[0]
    pieces = []

    def point(value):
        return tuple(round(coordinate, 6) for coordinate in value)

    def line(end):
        nonlocal cursor
        end = point(end)
        if cursor != end:
            pieces.append(("L", cursor, end))
        cursor = end

    cursor = point(cursor)
    for previous, corner, following in zip(points, points[1:], points[2:]):
        before, after = math.dist(previous, corner), math.dist(corner, following)
        radius = min(TURN_RADIUS, before / 2, after / 2)
        entry = tuple(corner[i] + (previous[i] - corner[i]) * radius / before for i in (0, 1))
        leave = point(tuple(corner[i] + (following[i] - corner[i]) * radius / after for i in (0, 1)))
        line(entry)
        pieces.append(("Q", cursor, point(corner), leave))
        cursor = leave
    line(points[-1])
    return pieces


def _reverse_piece(piece):
    return (piece[0], *reversed(piece[1:]))


def _merge_tracks(hops, routes):
    """Union coincident geometry, counting a business once on each visible piece."""
    memberships = defaultdict(set)
    for route in routes:
        for pair in _route_hops(route):
            memberships[pair].add(route["id"])
    lines, merged = defaultdict(list), defaultdict(set)
    for pair, (_, points) in sorted(hops.items()):
        for piece in _path_primitives(points):
            piece = min(piece, _reverse_piece(piece))
            if piece[0] == "Q":
                merged[piece].update(memberships[pair])
            else:
                a, b = piece[1:]
                axis = int(a[0] == b[0])
                lines[axis, a[1 - axis]].append((a[axis], b[axis], memberships[pair]))
    for (axis, fixed), intervals in sorted(lines.items()):
        breaks = sorted({coordinate for low, high, _ in intervals for coordinate in (low, high)})
        for low, high in zip(breaks, breaks[1:]):
            ids = set().union(*(ids for start, end, ids in intervals if start <= low and high <= end))
            if not ids:
                continue
            a, b = ((low, fixed), (high, fixed)) if axis == 0 else ((fixed, low), (fixed, high))
            merged["L", a, b].update(ids)
    # Keep a trunk continuous through side branches when its contributions stay
    # the same. Otherwise independent butt caps leave a hairline across it.
    adjacent = defaultdict(list)
    for piece in merged:
        adjacent[piece[1]].append(piece)
        adjacent[piece[-1]].append(piece)
    remaining, tracks = set(merged), []
    for first in sorted(merged):
        if first not in remaining:
            continue
        chain = deque([first])
        remaining.remove(first)
        ids = merged[first]
        for front in (False, True):
            while True:
                joint = chain[0][1] if front else chain[-1][-1]
                current = chain[0] if front else chain[-1]
                current_key = min(current, _reverse_piece(current))
                options = [piece for piece in adjacent[joint] if piece != current_key and merged[piece] == ids]
                if len(options) != 1 or options[0] not in remaining:
                    break
                following = options[0]
                if len(adjacent[joint]) > 2:
                    def tangent(piece):
                        neighbor = piece[2] if piece[1] == joint else piece[-2]
                        return neighbor[0] - joint[0], neighbor[1] - joint[1]
                    a, b = tangent(current), tangent(following)
                    if a[0] * b[1] != a[1] * b[0] or a[0] * b[0] + a[1] * b[1] >= 0:
                        break
                remaining.remove(following)
                if front:
                    chain.appendleft(following if following[-1] == joint else _reverse_piece(following))
                else:
                    chain.append(following if following[1] == joint else _reverse_piece(following))
        chain = min(tuple(chain), tuple(_reverse_piece(piece) for piece in reversed(chain)))
        tracks.append((chain, tuple(sorted(ids))))
    return sorted(tracks)


def _primitive_path(pieces, offset=0):
    commands = []
    for piece in pieces:
        start, end = piece[1], piece[-1]
        before = piece[2] if piece[0] == "Q" else end
        after = piece[2] if piece[0] == "Q" else start
        def normal(a, b):
            length = math.dist(a, b)
            return (-(b[1] - a[1]) / length, (b[0] - a[0]) / length)
        n1, n2 = normal(start, before), normal(after, end)
        a = tuple(start[i] + n1[i] * offset for i in (0, 1))
        b = tuple(end[i] + n2[i] * offset for i in (0, 1))
        if not commands:
            commands.append(f"M{a[0]},{a[1]}")
        if piece[0] == "Q":
            control = tuple(piece[2][i] + (n1[i] + n2[i]) * offset for i in (0, 1))
            commands.append(f"Q{control[0]},{control[1]} {b[0]},{b[1]}")
        else:
            commands.append(f"L{b[0]},{b[1]}")
    return " ".join(commands)


def _station_port(key, point, other, other_key):
    if not key.startswith("node:"):
        return point
    dx, dy = other[0] - point[0], other[1] - point[1]
    # Nearby Agents below a Node join its south trunk instead of adding side lanes.
    vertical = abs(dy) >= abs(dx) * (1 if other_key.startswith("node:") else AGENT_VERTICAL_BIAS)
    direction = ((0, 1 if dy >= 0 else -1) if vertical else (1 if dx >= 0 else -1, 0))
    return tuple(point[i] + NODE_PORT_LENGTH * direction[i] for i in (0, 1))


def _track_width(count):
    return TRACK_BASE_WIDTH + TRACK_WIDTH_SCALE * math.sqrt(min(count, TRACK_CONNECTION_CAP))


def _symbol_collisions(points, obstacles):
    return sum(any(math.hypot(x - max(min(x, max(a[0], b[0])), min(a[0], b[0])),
                              y - max(min(y, max(a[1], b[1])), min(a[1], b[1]))) < radius
                   for a, b in zip(points, points[1:])) for x, y, radius in obstacles)


def _corridor_overlap(points, occupied):
    overlap = 0
    for a, b in zip(points, points[1:]):
        axis = int(a[0] == b[0])
        low, high = sorted((a[axis], b[axis]))
        for c, d in occupied:
            if axis != int(c[0] == d[0]) or a[1 - axis] != c[1 - axis]:
                continue
            left, right = sorted((c[axis], d[axis]))
            overlap += max(0, min(high, right) - max(low, left))
    return overlap


def _clear_corridor(start, end, obstacles, occupied):
    """Find a valid orthogonal path, minimizing turns and then distance.

    Parallel occupied segments are forbidden; perpendicular crossings are allowed.
    Search coordinates come from symbol boundaries and clear lanes beside tracks,
    so this fallback is independent of connection counts and screen dimensions.
    """
    boxes = [(x - radius, y - radius, x + radius, y + radius) for x, y, radius in obstacles]
    xs, ys = {start[0], end[0]}, {start[1], end[1]}
    for left, top, right, bottom in boxes:
        xs.update((left, right)); ys.update((top, bottom))
    for a, b in occupied:
        xs.update((a[0], b[0], a[0] - CORRIDOR_GAP, a[0] + CORRIDOR_GAP))
        ys.update((a[1], b[1], a[1] - CORRIDOR_GAP, a[1] + CORRIDOR_GAP))
    xs.update((min(xs) - TURN_RADIUS * 2, max(xs) + TURN_RADIUS * 2))
    ys.update((min(ys) - TURN_RADIUS * 2, max(ys) + TURN_RADIUS * 2))
    xs, ys = sorted(xs), sorted(ys)
    xi, yi = {x: i for i, x in enumerate(xs)}, {y: i for i, y in enumerate(ys)}
    rows = [[(left, right) for left, top, right, bottom in boxes if top < y < bottom] +
            [tuple(sorted((a[0], b[0]))) for a, b in occupied if a[1] == b[1] == y] for y in ys]
    columns = [[(top, bottom) for left, top, right, bottom in boxes if left < x < right] +
               [tuple(sorted((a[1], b[1]))) for a, b in occupied if a[0] == b[0] == x] for x in xs]
    costs, previous, queue = {}, {}, []
    for axis in (0, 1):
        state = (xi[start[0]], yi[start[1]], axis)
        costs[state], previous[state] = (0, 0), None
        heapq.heappush(queue, (0, 0, state))
    while queue:
        turns, distance, state = heapq.heappop(queue)
        if costs[state] != (turns, distance):
            continue
        ix, iy, direction = state
        if (xs[ix], ys[iy]) == end:
            points = []
            while state is not None:
                points.append((xs[state[0]], ys[state[1]]))
                state = previous[state]
            return _compact_path(list(reversed(points)))
        for nx, ny, axis in ((ix - 1, iy, 0), (ix + 1, iy, 0), (ix, iy - 1, 1), (ix, iy + 1, 1)):
            if not (0 <= nx < len(xs) and 0 <= ny < len(ys)):
                continue
            midpoint = (ys[iy] + ys[ny]) / 2 if axis else (xs[ix] + xs[nx]) / 2
            if any(low < midpoint < high for low, high in (columns[ix] if axis else rows[iy])):
                continue
            neighbor = (nx, ny, axis)
            cost = (turns + (direction != axis), distance + abs(xs[nx] - xs[ix]) + abs(ys[ny] - ys[iy]))
            if cost < costs.get(neighbor, (math.inf, math.inf)):
                costs[neighbor], previous[neighbor] = cost, state
                heapq.heappush(queue, (*cost, neighbor))
    raise MetroRoutingError("No clear metro corridor between station ports")


def _metro_path(start: tuple[float, float], end: tuple[float, float],
                obstacles: list[tuple[float, float, float]], *, occupied=()) -> tuple[str, list[tuple[float, float]]]:
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

    candidates = [_compact_path([start, (x1, y2), end]),
                  _compact_path([start, (x2, y1), end]), points]
    if all(_symbol_collisions(candidate, obstacles) or _corridor_overlap(candidate, occupied)
           for candidate in candidates):
        xs = [min(x1, x2) - TURN_RADIUS * 2, (x1 + x2) / 2, max(x1, x2) + TURN_RADIUS * 2]
        ys = [min(y1, y2) - TURN_RADIUS * 2, (y1 + y2) / 2, max(y1, y2) + TURN_RADIUS * 2]
        if obstacles:
            xs.extend((min(x - r for x, y, r in obstacles) - TURN_RADIUS,
                       max(x + r for x, y, r in obstacles) + TURN_RADIUS))
            ys.extend((min(y - r for x, y, r in obstacles) - TURN_RADIUS,
                       max(y + r for x, y, r in obstacles) + TURN_RADIUS))
        for a, b in occupied:
            if a[0] == b[0]:
                xs.extend((a[0] - CORRIDOR_GAP, a[0] + CORRIDOR_GAP))
            else:
                ys.extend((a[1] - CORRIDOR_GAP, a[1] + CORRIDOR_GAP))
        candidates.extend([horizontal(bend) for bend in xs] + [vertical(bend) for bend in ys])
    points = min(candidates, key=lambda candidate: (_symbol_collisions(candidate, obstacles),
                 _corridor_overlap(candidate, occupied), len(candidate),
                 sum(math.dist(a, b) for a, b in zip(candidate, candidate[1:]))))
    if _symbol_collisions(points, obstacles) or _corridor_overlap(points, occupied):
        points = _clear_corridor(start, end, obstacles, occupied)
    return _rounded_path(points), points


def _route_hop(left, right, positions, hops):
    """Route station geometry first; counts and colors never choose a corridor."""
    start, end = positions[left], positions[right]
    entry = _station_port(left, start, end, right)
    exit_port = _station_port(right, end, start, left)
    # Sharing requires a common station, not merely a coincident screen position.
    occupied = [segment for pair, (_, previous) in hops.items() if not set(pair) & {left, right}
                for segment in zip(previous, previous[1:])]
    unrelated = [(x, y, _symbol_radius(key)) for key, (x, y) in positions.items() if key not in {left, right}]

    # All clear Node–Agent branches on a side use the same station axis, with
    # one rounded bend at the target's row/column; never add an intermediate spine.
    if left.startswith("node:") != right.startswith("node:"):
        node, agent, port = ((start, end, entry) if left.startswith("node:") else (end, start, exit_port))
        bend = (node[0], agent[1]) if port[0] == node[0] else (agent[0], node[1])
        direct = _compact_path([node, bend, agent])
        if not _symbol_collisions(direct, unrelated) and not _corridor_overlap(direct, occupied):
            points = direct if left.startswith("node:") else list(reversed(direct))
            return _rounded_path(points), points

    obstacles = [*unrelated, *((*positions[key], _symbol_radius(key))
                               for key in (left, right) if key.startswith("node:"))]
    try:
        _, middle = _metro_path(entry, exit_port, obstacles, occupied=occupied)
        points = _compact_path([start, *middle, end])
    except MetroRoutingError:
        # A long approach arm can be blocked while the station itself remains
        # reachable. Search from its center rather than emit an invalid track.
        points = _clear_corridor(start, end, unrelated, occupied)
    # Validate the whole path, including approach arms outside the port search.
    if _symbol_collisions(points, unrelated) or _corridor_overlap(points, occupied):
        points = _clear_corridor(start, end, unrelated, occupied)
    return _rounded_path(points), points


def _weighted_tracks(tracks, routes):
    """Apply current counts once to cached geometry and deduplicated route IDs."""
    result = []
    by_id = {route["id"]: route for route in routes}
    for pieces, route_ids in tracks:
        contributions = [by_id[route_id] for route_id in route_ids]
        count = sum(route["count"] for route in contributions)
        result.append({"id": hashlib.sha256(json.dumps(pieces).encode()).hexdigest()[:16],
                       "count": count, "width": _track_width(count),
                       "path": _primitive_path(pieces), "pieces": pieces,
                       "routes": [{"id": route["id"], "color": route["color"], "count": route["count"],
                                   "share": route["count"] / count} for route in contributions]})
    return result


class FlowMapRenderer:
    """Cache geometry independently of changing counts; safe for Waitress threads."""

    def __init__(self):
        self._lock = threading.Lock()
        self._signature = None
        self._geometry = None
        self._tracks = []
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
                self._geometry = self._layout(routes, node_ids)
                self._tracks = _merge_tracks(self._geometry.hops, routes)
                self._signature = signature
            geometry, tracks = self._geometry, self._tracks
        shared = _weighted_tracks(tracks, routes)
        return {**snapshot, "routes": routes, "visible_route_count": len(routes),
                "visible_flow_count": sum(route["count"] for route in routes),
                "services": sorted({route["service"] for route in snapshot["routes"]}),
                "nodes": stations,
                "shared_tracks": [{key: value for key, value in track.items() if key not in {"pieces", "path"}}
                                  for track in shared],
                "svg": self._svg(geometry, stations, shared)}

    def _layout(self, routes: list[dict], node_ids=()) -> MetroGeometry:
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
        pairs = {pair for route in routes for pair in _route_hops(route)}
        hops = {}
        for left, right in sorted(pairs):
            hops[left, right] = _route_hop(left, right, positions, hops)
        all_points = [point for _, points in hops.values() for point in points]
        bounds = [box for key, point in positions.items() for box in _station_boxes(key, point, agents)]
        width = max([box[2] for box in bounds] + [x for x, y in all_points], default=480) + 28
        height = max([box[3] for box in bounds] + [y for x, y in all_points], default=100) + 28
        return MetroGeometry(positions, agents, hops, width, height)

    @staticmethod
    def _svg(geometry: MetroGeometry, stations: list[dict], shared: list[dict]) -> str:
        positions, agents, hops, width, height = geometry
        nodes = {station["node_id"]: station for station in stations}
        svg = ET.Element("svg", {"xmlns": "http://www.w3.org/2000/svg", "class": "flow-svg",
                                 "viewBox": f"0 0 {width} {height}", "width": str(width), "height": str(height),
                                 "preserveAspectRatio": "xMidYMid meet", "role": "img", "focusable": "false",
                                 "aria-label": "Current relay routes and Node bandwidth headroom"})
        layer = ET.SubElement(svg, "g", {"class": "flow-scene"})
        tracks = ET.SubElement(layer, "g", {"class": "flow-routes"})
        # Lay branches down first, then let the wider trunk cover their inner
        # ends. The branch becomes visible at the trunk's edge rather than
        # painting a short colored stub across its center. Counts break ties
        # when both strokes have reached the width cap.
        for track in sorted(shared, key=lambda track: (track["width"], track["count"], track["id"])):
            group = ET.SubElement(tracks, "g", {"class": "flow-shared-track", "data-track": track["id"],
                                                "data-routes": json.dumps([route["id"] for route in track["routes"]])})
            ET.SubElement(group, "title").text = f'{track["count"]} connections across {len(track["routes"])} routes'
            ET.SubElement(group, "path", {"d": track["path"], "class": "flow-track",
                                           "stroke": "var(--flow-ring)", "stroke-width": f'{track["width"]:.6f}'})
            cursor = -track["width"] / 2
            for contribution in track["routes"]:
                band_width = track["width"] * contribution["share"]
                ET.SubElement(group, "path", {"d": _primitive_path(track["pieces"], cursor + band_width / 2),
                                               "class": "flow-band", "data-route": contribution["id"],
                                               "stroke": contribution["color"], "stroke-width": f"{band_width:.6f}"})
                cursor += band_width
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
                ET.SubElement(station, "circle", {"r": str(NODE_HALO_RADIUS), "class": "flow-node-halo"})
                ET.SubElement(station, "circle", {"r": str(NODE_CENTER_RADIUS), "class": "flow-node"})
                ET.SubElement(station, "circle", {"r": str(NODE_RING_RADIUS), "stroke-width": str(NODE_RING_WIDTH),
                                                      "class": "flow-capacity-track"})
                if available is not None and available < 1:
                    circumference = 2 * math.pi * NODE_RING_RADIUS
                    ET.SubElement(station, "circle", {"r": str(NODE_RING_RADIUS), "stroke-width": str(NODE_RING_WIDTH),
                                                      "class": "flow-capacity",
                                                      "transform": "rotate(-90)",
                                                      "stroke-dasharray": f"{circumference * (1 - available):.3f} {circumference:.3f}"})
                ET.SubElement(station, "text", {"y": "3.5", "text-anchor": "middle",
                                                "class": "flow-headroom"}).text = headroom
                title = f"Node {name}\nBandwidth headroom: {headroom}"
            else:
                station.set("class", "flow-station flow-agent-station")
                agent = agents[key]
                name = agent["peer"]
                ET.SubElement(station, "rect", {"x": str(-AGENT_SIZE / 2), "y": str(-AGENT_SIZE / 2),
                                                "width": str(AGENT_SIZE), "height": str(AGENT_SIZE),
                                                "stroke-width": str(AGENT_OUTLINE_WIDTH),
                                                "rx": "3", "class": "flow-agent"})
                title = f'Agent {name} · {agent["node"]} · {" / ".join(sorted(agent["roles"]))}'
            ET.SubElement(station, "title").text = title
            label = ET.SubElement(station, "text", {"x": "0", "y": str(NODE_HALO_RADIUS + 13) if key.startswith("node:") else "25",
                                                    "text-anchor": "middle", "class": "flow-label"})
            label.text = _label(name)[0]
        return ET.tostring(svg, encoding="unicode")
