"""Business snapshot and metro geometry regressions, without live sockets."""
import unittest
import random
import xml.etree.ElementTree as ET

from flow_map import (FlowCollection, FlowMapRenderer, _metro_path, _rounded_path,
                      _station_boxes, _corridor_overlap, _symbol_collisions, validate_relays)


def single(uuid=1, service="ssh", protocol="tcp", consumer="192.0.2.10:123", producer="[2001:db8::1]:456"):
    return dict(mode="single", service=service, protocol=protocol, uuid=uuid,
                consumer_peer=consumer, producer_peer=producer)


def half(role, flow=2, epoch=11, path=None, service="ssh", protocol="tcp"):
    report = dict(mode="multi", role=role, service=service, protocol=protocol,
                  epoch=epoch, flow_id=flow, agent_peer="192.0.2.10:123" if role == "ingress" else "[2001:db8::1]:456")
    if role == "ingress":
        report["path"] = path or ["a", "b", "c"]
    return report


def collect(reports, epoch=11):
    collector = FlowCollection()
    collector.topology(set(reports), epoch)
    collector.begin(1, 1)
    for node, records in reports.items():
        collector.record(1, node, validate_relays(node, records))
    return collector


class FlowCollectionTest(unittest.TestCase):
    def test_single_grouping_and_large_id(self):
        view = collect({"a": [single(2**64-1), single(2)]}).snapshot(True)
        self.assertEqual((view["flow_count"], view["route_count"]), (2, 1))
        self.assertEqual(view["routes"][0]["count"], 2)
        self.assertEqual({b["uuid"] for b in view["routes"][0]["businesses"]}, {str(2**64-1), "2"})

    def test_different_services_protocols_endpoints_and_paths(self):
        reports = {"a": [single(), single(2, service="rdp"), single(3, protocol="tls"),
                          single(4, consumer="192.0.2.11:123"), half("ingress", 5),
                          half("ingress", 6, path=["a", "c"])], "b": [],
                   "c": [half("egress", 5), half("egress", 6)]}
        view = collect(reports).snapshot(True)
        self.assertEqual((view["flow_count"], view["route_count"]), (6, 6))

    def test_tail_first_and_before_topology(self):
        collector = FlowCollection()
        collector.begin(1, 1)
        collector.record(1, "c", validate_relays("c", [half("egress", 2**64-1)]))
        collector.record(1, "a", validate_relays("a", [half("ingress", 2**64-1)]))
        collector.record(1, "b", ())
        self.assertEqual(collector.snapshot(True)["state"], "pending")
        collector.topology({"a", "b", "c"}, 11)
        route = collector.snapshot(True)["routes"][0]
        self.assertEqual(route["path"], ["a", "b", "c"])
        self.assertEqual(route["businesses"][0]["flow_id"], str(2**64-1))

    def test_initial_membership_excludes_previously_reported_departed_node(self):
        collector = FlowCollection()
        collector.begin(1, 1)
        collector.record(1, "departed", validate_relays("departed", [single()]))
        collector.record(1, "a", ())
        collector.topology({"a"}, 11)
        self.assertEqual(collector.snapshot(True)["state"], "ready")
        self.assertEqual(collector.snapshot(True)["flow_count"], 0)

    def test_never_pairs_different_rounds(self):
        collector = FlowCollection()
        collector.topology({"a", "b", "c"}, 11)
        collector.begin(1, 1)
        collector.record(1, "a", validate_relays("a", [half("ingress")]))
        collector.begin(2, 2)
        collector.record(2, "c", validate_relays("c", [half("egress")]))
        collector.begin(3, 3)
        self.assertIsNone(collector.snapshot(True)["round"])
        collector.begin(4, 4)
        view = collector.snapshot(True)
        self.assertEqual(view["state"], "partial")
        self.assertEqual(view["missing_nodes"], ["b", "c"])
        self.assertEqual(view["unmatched_count"], 1)
        self.assertEqual(view["flow_count"], 0)

    def test_duplicate_replies_empty_replacement_and_disconnect(self):
        collector = collect({"a": [single()]})
        collector.record(1, "a", validate_relays("a", [single(2)]))
        self.assertEqual(collector.snapshot(True)["flow_count"], 1)
        self.assertEqual(collector.snapshot(False)["routes"], [])
        collector.begin(2, 2)
        collector.record(2, "a", ())
        self.assertEqual(collector.snapshot(True)["flow_count"], 0)
        collector.record(1, "a", validate_relays("a", [single(3)]))
        self.assertEqual(collector.snapshot(True)["round"], "2")

    def test_newer_complete_round_cannot_be_replaced_by_old_timeout(self):
        collector = FlowCollection()
        collector.topology({"a", "c"}, 11)
        collector.begin(1, 1)
        collector.record(1, "a", validate_relays("a", [single()]))
        collector.begin(2, 2)
        collector.record(2, "a", ())
        collector.record(2, "c", ())
        collector.begin(4, 4)
        self.assertEqual(collector.snapshot(True)["round"], "2")

    def test_epoch_and_membership_changes_clear_old_businesses(self):
        collector = collect({"a": [single()]})
        collector.topology({"a", "b"}, 12)
        self.assertEqual(collector.snapshot(True)["state"], "pending")
        collector.begin(2, 2)
        collector.record(2, "a", validate_relays("a", [half("ingress", epoch=11, path=["a", "b"])]))
        collector.record(2, "b", validate_relays("b", [half("egress", epoch=11)]))
        self.assertEqual(collector.snapshot(True)["flow_count"], 0)

    def test_membership_without_epoch_change_invalidates_pending_records(self):
        collector = collect({"a": [single()], "b": []})
        collector.begin(2, 2)
        collector.record(2, "a", validate_relays("a", [single()]))
        collector.topology({"a"}, 11)
        self.assertEqual(collector.snapshot(True)["state"], "pending")
        collector.record(2, "b", ())
        self.assertEqual(collector.snapshot(True)["flow_count"], 0)
        collector.begin(3, 3)
        collector.record(3, "a", ())
        self.assertEqual(collector.snapshot(True)["state"], "ready")

    def test_mismatched_endpoints_and_metadata_are_not_inferred(self):
        for tail_node, service, protocol in [("b", "ssh", "tcp"), ("c", "rdp", "tcp"), ("c", "ssh", "udp")]:
            with self.subTest(tail_node=tail_node, service=service, protocol=protocol):
                reports = {"a": [half("ingress")], "b": [], "c": []}
                reports[tail_node] = [half("egress", service=service, protocol=protocol)]
                view = collect(reports).snapshot(True)
                self.assertEqual((view["flow_count"], view["unmatched_count"]), (0, 1))

    def test_mandatory_protocol_and_invalid_records(self):
        bad = [None, {}, [single(True)], [single(0)], [single(2**64)],
               [single(), single()], [single(protocol="http")],
               [half("ingress", path=["a", "a"])], [half("ingress", path=["b", "c"])]]
        for raw in bad:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                validate_relays("a", raw)


class FlowMapTest(unittest.TestCase):
    def test_shared_nodes_branch_reverse_ipv6_and_same_agent(self):
        reports = {"a": [half("ingress", 1, path=["a", "b"]), half("egress", 2),
                          single(3, consumer="[2001:db8::1]:99", producer="[2001:db8::1]:99")],
                   "b": [half("egress", 1), half("ingress", 2, path=["b", "a"])]}
        view = collect(reports).snapshot(True)
        renderer = FlowMapRenderer()
        rendered = renderer.render(view)
        svg = ET.fromstring(rendered["svg"])
        ns = {"s": "http://www.w3.org/2000/svg"}
        self.assertEqual(len(svg.findall(".//s:circle[@class='flow-node']", ns)), 2)
        self.assertEqual(rendered["visible_route_count"], 3)
        self.assertIn("consumer / producer", rendered["svg"])
        self.assertEqual(renderer.render(view, protocol="udp")["visible_route_count"], 0)

    def test_count_refresh_reuses_geometry_and_stable_route_id(self):
        renderer = FlowMapRenderer()
        first = collect({"a": [single()]}).snapshot(True)
        before = renderer.render(first)
        geometry = renderer._geometry
        second = collect({"a": [single(index) for index in range(1, 11)]}).snapshot(True)
        after = renderer.render(second)
        self.assertIs(renderer._geometry, geometry)
        self.assertEqual(before["routes"][0]["id"], after["routes"][0]["id"])
        self.assertIn("10 connections", after["svg"])
        ns = {"s": "http://www.w3.org/2000/svg"}
        width = lambda result: float(ET.fromstring(result["svg"]).find(".//s:path[@class='flow-track']", ns).get("stroke-width"))
        self.assertGreater(width(after), width(before))

    def test_station_counts_each_business_once_and_includes_idle_members(self):
        view = collect({"a": [half("ingress", 1), half("ingress", 2), single(3)], "b": [],
                        "c": [half("egress", 1), half("egress", 2)]}).snapshot(True)
        topology = {"nodes": [{"node_id": node, "available": available}
                              for node, available in (("a", .64), ("b", .18), ("c", .79), ("idle", 1))]}
        result = FlowMapRenderer().render(view, topology=topology)
        nodes = {node["node_id"]: node for node in result["nodes"]}
        self.assertEqual({node: data["connection_count"] for node, data in nodes.items()},
                         {"a": 3, "b": 2, "c": 2, "idle": 0})
        self.assertEqual(len(nodes["b"]["route_ids"]), 1)
        self.assertEqual(nodes["b"]["available"], .18)
        svg = ET.fromstring(result["svg"])
        ns = {"s": "http://www.w3.org/2000/svg"}
        self.assertEqual(len(svg.findall(".//s:circle[@class='flow-capacity']", ns)), 3)
        self.assertIn('flow-node-station warning', result["svg"])
        self.assertIn('18%', result["svg"])

    def test_capacity_only_changes_reuse_layout_and_stale_values_are_unknown(self):
        view = collect({"a": [single()]}).snapshot(True)
        topology = {"nodes": [{"node_id": "a", "available": .8}]}
        renderer = FlowMapRenderer()
        first = renderer.render(view, topology=topology)
        geometry = renderer._geometry
        topology["nodes"][0]["available"] = .05
        second = renderer.render(view, topology=topology)
        self.assertIs(renderer._geometry, geometry)
        self.assertIn('flow-node-station critical', second["svg"])
        self.assertNotEqual(first["svg"], second["svg"])
        topology["stale"] = True
        stale = renderer.render(view, topology=topology)
        self.assertIsNone(stale["nodes"][0]["available"])
        self.assertNotIn('class="flow-capacity"', stale["svg"])
        self.assertIn('flow-node-station unknown', stale["svg"])

    def test_pending_disconnected_and_partial_counts_are_not_reported_as_complete(self):
        topology = {"nodes": [{"node_id": "idle", "available": None}]}
        renderer = FlowMapRenderer()
        for connected in (True, False):
            result = renderer.render(FlowCollection().snapshot(connected), topology=topology)
            self.assertIsNone(result["nodes"][0]["connection_count"])
            self.assertNotIn('Connections pending', result["svg"])
        view = collect({"a": [single()]}).snapshot(True)
        view.update(state="partial", partial=True)
        result = renderer.render(view)
        self.assertEqual(result["nodes"][0]["connection_count"], 1)
        self.assertTrue(result["nodes"][0]["partial"])

    def test_line_width_uses_square_root_scale_and_caps_at_100(self):
        view = collect({"a": [single()]}).snapshot(True)
        renderer = FlowMapRenderer()
        geometry = None
        for count, expected in ((1, 3.8), (4, 4.6), (9, 5.4), (10, 5.529822128),
                                (49, 8.6), (50, 8.656854249), (51, 8.713142743),
                                (99, 10.959899497), (100, 11), (101, 11), (10000, 11)):
            view["routes"][0]["count"] = count
            result = renderer.render(view)
            widths = {track["width"] for track in result["shared_tracks"]}
            self.assertEqual(len(widths), 1)
            self.assertAlmostEqual(next(iter(widths)), expected, places=6, msg=f"{count} connections")
            if geometry is not None:
                self.assertIs(renderer._geometry, geometry)
            geometry = renderer._geometry
            svg = ET.fromstring(result["svg"])
            ns = {"s": "http://www.w3.org/2000/svg"}
            for track in svg.findall(".//s:path[@class='flow-track']", ns):
                self.assertAlmostEqual(float(track.get("stroke-width")), expected, places=5)

    def test_nodes_show_name_and_capacity_without_interactive_counts(self):
        result = FlowMapRenderer().render(collect({"a": [single()]}).snapshot(True),
                                          topology={"nodes": [{"node_id": "a", "available": .64}]})
        svg = ET.fromstring(result["svg"])
        ns = {"s": "http://www.w3.org/2000/svg"}
        station = svg.find(".//s:g[@data-node='a']", ns)
        self.assertNotIn("tabindex", station.attrib)
        self.assertNotIn("role", station.attrib)
        self.assertNotIn("connections", "".join(station.itertext()))
        self.assertEqual(station.find("s:text[@class='flow-label']", ns).text, "a")
        self.assertEqual(station.find("s:text[@class='flow-headroom']", ns).text, "64%")
        self.assertNotIn("min-width", svg.get("style", ""))
        self.assertEqual(svg.get("preserveAspectRatio"), "xMidYMid meet")
        self.assertEqual(svg.get("role"), "img")
        self.assertEqual(svg.get("focusable"), "false")
        self.assertFalse(svg.findall(".//*[@tabindex]"))
        self.assertFalse(svg.findall(".//*[@role='button']"))
        self.assertFalse(svg.findall(".//s:path[@class='flow-hit']", ns))


    def test_shared_hops_use_the_same_track_without_parallel_offsets(self):
        view = collect({"a": [half("ingress", 1, path=["a", "c"]),
                              half("ingress", 2, path=["a", "c"], protocol="udp")],
                        "c": [half("egress", 1), half("egress", 2, protocol="udp")]}).snapshot(True)
        renderer = FlowMapRenderer()
        renderer.render(view)
        geometry = renderer._geometry
        self.assertEqual(len(geometry.hops), 3)
        self.assertTrue(all(len(route_ids) == 2 for _, route_ids in renderer._tracks))
        tracks = [path for path, _ in geometry.hops.values()]
        self.assertTrue(any(" Q" in path for path in tracks))
        self.assertTrue(all(" C" not in path for path in tracks))
        self.assertEqual(geometry.positions["node:a"][1], geometry.positions["node:c"][1])
        self.assertTrue(all(0 < x < geometry.width and 0 < y < geometry.height
                            for x, y in geometry.positions.values()))

    def test_shared_track_width_and_color_bands_use_summed_connections(self):
        view = collect({"a": [single(1), single(2), single(3), single(4, protocol="udp")]}).snapshot(True)
        renderer = FlowMapRenderer()
        rendered = renderer.render(view)
        geometry = renderer._geometry
        for track in rendered["shared_tracks"]:
            self.assertEqual(track["count"], 4)
            self.assertAlmostEqual(track["width"], 4.6)
            self.assertEqual(sorted(route["share"] for route in track["routes"]), [.25, .75])
        svg = ET.fromstring(rendered["svg"])
        ns = {"s": "http://www.w3.org/2000/svg"}
        for track in svg.findall(".//s:g[@class='flow-shared-track']", ns):
            width = float(track.find("s:path[@class='flow-track']", ns).get("stroke-width"))
            bands = sorted(float(band.get("stroke-width")) for band in track.findall("s:path[@class='flow-band']", ns))
            self.assertAlmostEqual(sum(bands), width, places=5)
            self.assertAlmostEqual(bands[1] / bands[0], 3, places=5)
        for route in view["routes"]:
            route["count"] = 6
        merged = renderer.render(view)
        for track in merged["shared_tracks"]:
            self.assertEqual(track["count"], 12)
            self.assertAlmostEqual(track["width"], 5.771281292, places=6)
        view["routes"][0]["count"] = 100
        changed = renderer.render(view)
        self.assertIs(renderer._geometry, geometry)
        self.assertTrue(all(track["width"] <= 11 for track in changed["shared_tracks"]))

    def test_overlapping_tracks_are_merged_instead_of_drawn_on_top_of_each_other(self):
        ingress_a = half("ingress", 1, path=["aliyun-1", "awsyun-1"])
        ingress_a["agent_peer"] = "219.140.92.118:6347"
        ingress_b = half("ingress", 2, path=["llyun-1", "awsyun-1"])
        ingress_b["agent_peer"] = "219.140.92.118:40076"
        reports = {"aliyun-1": [ingress_a, single(3)],
                   "awsyun-1": [half("egress", 1), half("egress", 2)],
                   "llyun-1": [ingress_b, single(4), single(5, producer="219.140.92.118:14070")],
                   "txyun-1": [single(6)]}
        renderer = FlowMapRenderer()
        rendered = renderer.render(collect(reports).snapshot(True))
        self.assert_tracks_are_merged(renderer)
        hops = list(renderer._geometry.hops.items())
        for index, (pair, (_, points)) in enumerate(hops):
            for other_pair, (_, other) in hops[index + 1:]:
                if not set(pair) & set(other_pair):
                    self.assertEqual(_corridor_overlap(points, list(zip(other, other[1:]))), 0,
                                     msg=str((pair, other_pair)))
        self.assertTrue(any(len(track["routes"]) >= 2 for track in rendered["shared_tracks"]))

    def test_text_is_escaped_and_labels_are_truncated(self):
        peer = '<script>alert("x")</script>' * 3
        view = collect({"<node>": [single(service="<ssh>", consumer=peer)]}).snapshot(True)
        svg = FlowMapRenderer().render(view)["svg"]
        ET.fromstring(svg)
        self.assertNotIn("<script>", svg)
        self.assertIn("&lt;script&gt;", svg)
        self.assertIn("…", svg)

    def test_disconnected_nodes_pack_into_a_balanced_map(self):
        for count in (4, 12, 60):
            renderer = FlowMapRenderer()
            renderer.render(FlowCollection().snapshot(True), topology={"nodes": [
                {"node_id": f"Node-{index:02}", "available": .5} for index in range(count)]})
            width, height = renderer._geometry[-2:]
            self.assertGreater(width / height, .9)
            self.assertLess(width / height, 3)

    def test_many_agents_use_multiple_columns_without_label_overlap(self):
        reports = {"a": [single(index, consumer=f"192.0.2.{index}:123", producer=f"198.51.100.{index}:456")
                         for index in range(1, 17)]}
        renderer = FlowMapRenderer()
        renderer.render(collect(reports).snapshot(True))
        positions, agents, _, width, height = renderer._geometry
        self.assertGreater(width / height, 1)
        self.assertLess(width / height, 3)
        labels = [_station_boxes(key, point, agents)[1] for key, point in positions.items()]
        for index, (left, top, right, bottom) in enumerate(labels):
            for other_left, other_top, other_right, other_bottom in labels[index + 1:]:
                self.assertFalse(left < other_right and other_left < right
                                 and top < other_bottom and other_top < bottom)
        self.assert_tracks_are_merged(renderer)
        # Even with 32 Agents, every approach remains on one of the Node's four axes.
        center = positions["node:a"]
        for (_, points) in renderer._geometry.hops.values():
            neighbor = points[-2] if points[-1] == center else points[1]
            self.assertTrue(neighbor[0] == center[0] or neighbor[1] == center[1])

    def assert_tracks_are_merged(self, renderer):
        straight = []
        curved = set()
        for pieces, _ in renderer._tracks:
            for piece in pieces:
                if piece[0] == "Q":
                    key = min(piece, (piece[0], *reversed(piece[1:])))
                    self.assertNotIn(key, curved)
                    curved.add(key)
                    continue
                a, b = piece[1:]
                horizontal = a[1] == b[1]
                axis = 0 if horizontal else 1
                low, high = sorted((a[axis], b[axis]))
                for c, d in straight:
                    if horizontal != (c[1] == d[1]) or a[1-axis] != c[1-axis]:
                        continue
                    other_low, other_high = sorted((c[axis], d[axis]))
                    self.assertLessEqual(min(high, other_high) - max(low, other_low), 0)
                straight.append((a, b))

    def test_south_agents_share_one_trunk_with_weighted_connections(self):
        reports = {"llyun-1": [single(index, consumer="source", producer=f"target-{index}")
                                for index in range(1, 4)]}
        view = collect(reports).snapshot(True)
        for route in view["routes"]:
            route["count"] = 3 if route["producer"]["peer"] == "target-1" else 1
        renderer = FlowMapRenderer()
        renderer._positions = {"node:llyun-1": (240, 320),
                               'agent:["llyun-1", "source"]': (140, 528),
                               'agent:["llyun-1", "target-1"]': (340, 416),
                               'agent:["llyun-1", "target-2"]': (340, 472),
                               'agent:["llyun-1", "target-3"]': (540, 416)}
        rendered = renderer.render(view)
        # At y=344, all four lower Agents use exactly one rendered southbound track.
        trunks = [(pieces, ids) for pieces, ids in renderer._tracks if any(
            piece[0] == "L" and piece[1][0] == piece[-1][0] == 240
            and min(piece[1][1], piece[-1][1]) <= 344 < max(piece[1][1], piece[-1][1])
            for piece in pieces)]
        self.assertEqual(len(trunks), 1)
        self.assertEqual(set(trunks[0][1]), {route["id"] for route in view["routes"]})
        shared = next(track for track in rendered["shared_tracks"] if len(track["routes"]) == 3)
        self.assertEqual(shared["count"], 5)  # Never count the consumer and producer hop twice.
        self.assertEqual(sorted(route["share"] for route in shared["routes"]), [.2, .2, .6])
        self.assertAlmostEqual(shared["width"], 4.788854382, places=6)
        self.assert_tracks_are_merged(renderer)

    def test_lower_agents_branch_from_one_axis_without_a_second_shared_spine(self):
        view = collect({"llyun-1": [single(1, consumer="219.140.92.118:40076", producer="192.229.85.177:41132"),
                                      single(2, consumer="219.140.92.118:40076", producer="219.140.92.118:14070")]}).snapshot(True)
        renderer = FlowMapRenderer()
        renderer._positions = {"node:llyun-1": (240, 320),
                               'agent:["llyun-1", "219.140.92.118:40076"]': (140, 528),
                               'agent:["llyun-1", "192.229.85.177:41132"]': (340, 416),
                               'agent:["llyun-1", "219.140.92.118:14070"]': (340, 472)}
        renderer.render(view)
        center = (240, 320)
        for pair, (path, points) in renderer._geometry.hops.items():
            agent = renderer._positions[pair[0]]
            self.assertEqual(points, [agent, (240, agent[1]), center])
            self.assertEqual(path.count(" Q"), 1)
        route_ids = {route["id"] for route in view["routes"]}
        self.assertEqual(sum(set(ids) == route_ids for _, ids in renderer._tracks), 1)
        self.assert_tracks_are_merged(renderer)

    def test_txyun_agent_connection_uses_one_meaningful_bend(self):
        source = half("ingress", 1, path=["other", "txyun-1"])
        target = half("egress", 1)
        target["agent_peer"] = "219.140.92.118:4699"
        renderer = FlowMapRenderer()
        renderer._positions = {"node:other": (240, 320), "node:txyun-1": (480, 320),
                               'agent:["txyun-1", "219.140.92.118:4699"]': (580, 416)}
        renderer.render(collect({"other": [source], "txyun-1": [target]}).snapshot(True))
        path, points = renderer._geometry.hops['agent:["txyun-1", "219.140.92.118:4699"]', 'node:txyun-1']
        self.assertEqual(points, [(580, 416), (480, 416), (480, 320)])
        self.assertEqual(path.count(" Q"), 1)

    def test_dense_maps_keep_unrelated_tracks_and_symbols_clear(self):
        for node_count, route_count, seed, trials in ((4, 8, 731, 10), (8, 12, 947, 5)):
            rng = random.Random(seed)
            for trial in range(trials):
                with self.subTest(nodes=node_count, trial=trial):
                    reports = {f"node-{index}": [] for index in range(node_count)}
                    for index in range(route_count):
                        first, last = rng.sample(list(reports), 2)
                        ingress = half("ingress", index + 1, path=[first, last])
                        ingress["agent_peer"] = f"source-{index}"
                        egress = half("egress", index + 1)
                        egress["agent_peer"] = f"target-{index}"
                        reports[first].append(ingress)
                        reports[last].append(egress)
                    renderer = FlowMapRenderer()
                    renderer.render(collect(reports).snapshot(True))
                    geometry = renderer._geometry
                    hops = list(geometry.hops.items())
                    for index, (pair, (_, points)) in enumerate(hops):
                        self.assertEqual(points[0], geometry.positions[pair[0]])
                        self.assertEqual(points[-1], geometry.positions[pair[1]])
                        self.assertTrue(all(a[0] == b[0] or a[1] == b[1]
                                            for a, b in zip(points, points[1:])))
                        obstacles = [(x, y, 22 if key.startswith("node:") else 12)
                                     for key, (x, y) in geometry.positions.items() if key not in pair]
                        self.assertEqual(_symbol_collisions(points, obstacles), 0, msg=str(pair))
                        for other_pair, (_, other) in hops[index + 1:]:
                            if not set(pair) & set(other_pair):
                                self.assertEqual(_corridor_overlap(points, list(zip(other, other[1:]))), 0,
                                                 msg=str((pair, other_pair)))
                    self.assert_tracks_are_merged(renderer)

    def test_perpendicular_crossings_do_not_merge_route_counts(self):
        a = half("ingress", 1, path=["a", "b"])
        c = half("ingress", 2, path=["c", "d"])
        renderer = FlowMapRenderer()
        renderer._positions = {"node:a": (200, 200), "node:b": (600, 200),
                               "node:c": (400, 80), "node:d": (400, 360)}
        renderer.render(collect({"a": [a], "b": [half("egress", 1)],
                                 "c": [c], "d": [half("egress", 2)]}).snapshot(True))
        crossing = []
        for pieces, ids in renderer._tracks:
            if any(piece[0] == "L" and
                   min(piece[1][0], piece[-1][0]) <= 400 <= max(piece[1][0], piece[-1][0]) and
                   min(piece[1][1], piece[-1][1]) <= 200 <= max(piece[1][1], piece[-1][1])
                   for piece in pieces):
                crossing.append(ids)
        self.assertEqual(len(crossing), 2)
        self.assertTrue(all(len(ids) == 1 for ids in crossing))
        self.assertNotEqual(crossing[0], crossing[1])

    def test_blocked_approach_port_does_not_force_a_track_through_an_agent(self):
        renderer = FlowMapRenderer()
        renderer._positions = {"node:a": (240, 320), "node:b": (480, 480),
                               'agent:["a", "source-a"]': (140, 416),
                               'agent:["a", "target-a"]': (540, 480),
                               'agent:["b", "source-b"]': (380, 576),
                               'agent:["b", "target-b"]': (780, 480)}
        renderer.render(collect({"a": [single(1, consumer="source-a", producer="target-a")],
                                 "b": [single(2, consumer="source-b", producer="target-b")]}).snapshot(True))
        _, points = renderer._geometry.hops['agent:["b", "target-b"]', 'node:b']
        self.assertEqual(_symbol_collisions(points, [(540, 480, 12)]), 0)
        self.assertEqual(points[0], (780, 480))
        self.assertEqual(points[-1], (480, 480))
        self.assert_tracks_are_merged(renderer)

    def test_capacity_ring_is_thick_inside_a_track_connected_station_outline(self):
        rendered = FlowMapRenderer().render(collect({"a": [single()]}).snapshot(True),
                                            topology={"nodes": [{"node_id": "a", "available": .64}]})
        ns = {"s": "http://www.w3.org/2000/svg"}
        station = ET.fromstring(rendered["svg"]).find(".//s:g[@data-node='a']", ns)
        for class_name in ("flow-capacity", "flow-capacity-track"):
            ring = station.find(f"s:circle[@class='{class_name}']", ns)
            self.assertEqual(float(ring.get("r")), 18)
            self.assertEqual(float(ring.get("stroke-width")), 7)
        outline = station.find("s:circle[@class='flow-node']", ns)
        self.assertEqual(float(outline.get("r")), 23)
        self.assertEqual(float(outline.get("stroke-width")), 2)
        self.assertEqual(float(outline.get("r")) - float(outline.get("stroke-width")) / 2 - (18 + 7 / 2), .5)
        self.assertIsNone(station.find("s:circle[@class='flow-node-halo']", ns))
        arc = station.find("s:circle[@class='flow-capacity']", ns)
        used, circumference = map(float, arc.get("stroke-dasharray").split())
        self.assertAlmostEqual(circumference, 2 * 3.141592653589793 * 18, places=3)
        self.assertAlmostEqual(used / circumference, .36, places=4)

    def test_metro_tracks_are_orthogonal_with_every_turn_rounded(self):
        for start, end in (((0, 0), (300, 0)), ((100, 100), (100, 400)), ((50, 60), (400, 350))):
            path, points = _metro_path(start, end, [])
            self.assertNotIn(" C", path)
            self.assertEqual(path.count(" Q"), len(points) - 2)
            self.assertTrue(all(a[0] == b[0] or a[1] == b[1] for a, b in zip(points, points[1:])))
            self.assertEqual(points[0], start)
            self.assertEqual(points[-1], end)
            dx, dy = end[0] - start[0], end[1] - start[1]
            projected = [(x - start[0]) * dx + (y - start[1]) * dy for x, y in points]
            self.assertEqual(projected, sorted(projected))

    def test_redundant_points_and_short_spurs_do_not_create_corners(self):
        clean = [(0, 0), (100, 0), (100, 100)]
        redundant = [(0, 0), (10, 0), (20, 0), (10, 0), (100, 0), (100, 25), (100, 100)]
        self.assertEqual(_rounded_path(clean), _rounded_path(redundant))
        self.assertEqual(_rounded_path(clean).count(" Q"), 1)

    def test_metro_tracks_pass_around_unrelated_node_symbols(self):
        _, points = _metro_path((0, 0), (300, 0), [(150, 0, 36)])
        for a, b in zip(points, points[1:]):
            x = max(min(150, max(a[0], b[0])), min(a[0], b[0]))
            y = max(min(0, max(a[1], b[1])), min(a[1], b[1]))
            self.assertGreater((x - 150)**2 + y**2, 36**2)

    def test_node_positions_survive_route_agent_and_membership_changes(self):
        renderer = FlowMapRenderer()
        idle = collect({"a": [], "b": [], "c": []}).snapshot(True)
        topology = {"nodes": [{"node_id": name, "available": .5} for name in ("a", "b", "c")]}
        renderer.render(idle, topology=topology)
        original = dict(renderer._geometry.positions)
        active = collect({"a": [half("ingress", 1, path=["a", "c"])], "b": [], "c": [half("egress", 1)]}).snapshot(True)
        renderer.render(active, topology=topology)
        self.assertTrue(all(renderer._geometry.positions[key] == point for key, point in original.items()))
        agent_positions = {key: point for key, point in renderer._geometry.positions.items() if key.startswith("agent:")}
        active["routes"][0]["id"] = "another-service-route"
        renderer.render(active, topology=topology)
        self.assertTrue(all(renderer._geometry.positions[key] == point for key, point in {**original, **agent_positions}.items()))
        renderer.render(idle, topology={"nodes": [{"node_id": "a", "available": .5}]})
        topology["nodes"].append({"node_id": "d", "available": .5})
        renderer.render(idle, topology=topology)
        self.assertTrue(all(renderer._geometry.positions[key] == point for key, point in original.items()))

    def test_route_order_does_not_change_geometry(self):
        view = collect({"a": [single(1), single(2, protocol="udp"), single(3, consumer="192.0.2.11:123")]}).snapshot(True)
        before = FlowMapRenderer().render(view)["svg"]
        view["routes"].reverse()
        after = FlowMapRenderer().render(view)["svg"]
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
