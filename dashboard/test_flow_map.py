"""Business snapshot and metro geometry regressions, without live sockets."""
import unittest
import xml.etree.ElementTree as ET

from flow_map import FlowCollection, FlowMapRenderer, _curve_path, _station_boxes, validate_relays


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
        self.assertEqual(len(svg.findall(".//s:circle", ns)), 2)
        self.assertEqual(len(svg.findall(".//s:g[@class='flow-line']", ns)), 3)
        self.assertIn("consumer / producer", rendered["svg"])
        self.assertEqual(renderer.render(view, protocol="udp")["visible_route_count"], 0)

    def test_count_refresh_reuses_geometry_and_stable_route_id(self):
        renderer = FlowMapRenderer()
        first = collect({"a": [single()]}).snapshot(True)
        before = renderer.render(first)
        geometry = renderer._geometry
        second = collect({"a": [single(), single(2)]}).snapshot(True)
        after = renderer.render(second)
        self.assertIs(renderer._geometry, geometry)
        self.assertEqual(before["routes"][0]["id"], after["routes"][0]["id"])
        self.assertIn("2 connections", after["svg"])
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
        self.assertEqual(len(svg.findall(".//s:circle[@class='flow-capacity']", ns)), 4)
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

    def test_line_width_uses_square_root_scale_capped_at_50_connections(self):
        view = collect({"a": [single()]}).snapshot(True)
        renderer = FlowMapRenderer()
        widths = []
        for count in (1, 10, 49, 50, 51, 100, 10000):
            view["routes"][0]["count"] = count
            result = renderer.render(view)
            ns = {"s": "http://www.w3.org/2000/svg"}
            widths.append(float(ET.fromstring(result["svg"]).find(".//s:path[@class='flow-track']", ns).get("stroke-width")))
            self.assertAlmostEqual(widths[-1], 3 + .8 * min(count, 50)**.5, places=2)
        self.assertEqual(widths, sorted(widths))
        self.assertGreater(widths[3], widths[2])
        self.assertEqual(widths[3:], [widths[3]] * 4)
        self.assertLess(widths[-1], 14)  # Keep tracks narrower than Agent stations.

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

    def test_shared_horizontal_edges_use_distinct_rounded_tracks(self):
        view = collect({"a": [half("ingress", 1, path=["a", "c"]),
                              half("ingress", 2, path=["a", "c"], protocol="udp")],
                        "c": [half("egress", 1), half("egress", 2, protocol="udp")]}).snapshot(True)
        renderer = FlowMapRenderer()
        renderer.render(view)
        positions, agents, paths, width, height = renderer._geometry
        tracks = list(paths.values())
        self.assertNotEqual(tracks[0], tracks[1])
        self.assertTrue(all(" C" in track and " Q" not in track and " L" not in track for track in tracks))
        self.assertEqual(positions["node:a"][1], positions["node:c"][1])
        self.assertTrue(all(0 < x < width and 0 < y < height for x, y in positions.values()))

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

    def test_hops_are_single_smooth_curves_without_backtracking(self):
        for start, end in (((0, 0), (300, 0)), ((100, 100), (100, 400)), ((50, 60), (400, 350))):
            path, points = _curve_path(start, end, 14, [])
            self.assertEqual(path.count(" C"), 1)
            self.assertNotIn(" L", path)
            self.assertNotIn(" Q", path)
            dx, dy = end[0] - start[0], end[1] - start[1]
            projected = [(x - start[0]) * dx + (y - start[1]) * dy for x, y in points]
            self.assertEqual(projected, sorted(projected))
            self.assertEqual(points[0], start)
            self.assertEqual(points[-1], end)

    def test_curves_pass_around_unrelated_node_symbols(self):
        _, points = _curve_path((0, 0), (300, 0), 0, [(150, 0, 32)])
        for step in range(41):
            t = step / 40
            weights = ((1 - t)**3, 3 * (1 - t)**2 * t, 3 * (1 - t) * t**2, t**3)
            x = sum(weight * point[0] for weight, point in zip(weights, points))
            y = sum(weight * point[1] for weight, point in zip(weights, points))
            self.assertGreater((x - 150)**2 + y**2, 32**2)

    def test_node_positions_survive_route_agent_and_membership_changes(self):
        renderer = FlowMapRenderer()
        idle = collect({"a": [], "b": [], "c": []}).snapshot(True)
        topology = {"nodes": [{"node_id": name, "available": .5} for name in ("a", "b", "c")]}
        renderer.render(idle, topology=topology)
        original = dict(renderer._geometry[0])
        active = collect({"a": [half("ingress", 1, path=["a", "c"])], "b": [], "c": [half("egress", 1)]}).snapshot(True)
        renderer.render(active, topology=topology)
        self.assertTrue(all(renderer._geometry[0][key] == point for key, point in original.items()))
        agent_positions = {key: point for key, point in renderer._geometry[0].items() if key.startswith("agent:")}
        active["routes"][0]["id"] = "another-service-route"
        renderer.render(active, topology=topology)
        self.assertTrue(all(renderer._geometry[0][key] == point for key, point in {**original, **agent_positions}.items()))
        renderer.render(idle, topology={"nodes": [{"node_id": "a", "available": .5}]})
        topology["nodes"].append({"node_id": "d", "available": .5})
        renderer.render(idle, topology=topology)
        self.assertTrue(all(renderer._geometry[0][key] == point for key, point in original.items()))

    def test_route_order_does_not_change_geometry(self):
        view = collect({"a": [single(1), single(2, protocol="udp"), single(3, consumer="192.0.2.11:123")]}).snapshot(True)
        before = FlowMapRenderer().render(view)["svg"]
        view["routes"].reverse()
        after = FlowMapRenderer().render(view)["svg"]
        ns = {"s": "http://www.w3.org/2000/svg"}
        paths = lambda svg: {line.get("data-route"): line.find("s:path", ns).get("d")
                             for line in ET.fromstring(svg).findall(".//s:g[@class='flow-line']", ns)}
        self.assertEqual(paths(before), paths(after))


if __name__ == "__main__":
    unittest.main()
