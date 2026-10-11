"""Business snapshot and metro geometry regressions, without live sockets."""
import unittest
import xml.etree.ElementTree as ET

from flow_map import FlowCollection, FlowMapRenderer, validate_relays


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

    def test_shared_horizontal_edges_use_distinct_rounded_tracks(self):
        view = collect({"a": [half("ingress", 1, path=["a", "c"]),
                              half("ingress", 2, path=["a", "c"], protocol="udp")],
                        "c": [half("egress", 1), half("egress", 2, protocol="udp")]}).snapshot(True)
        renderer = FlowMapRenderer()
        renderer.render(view)
        positions, agents, paths, width, height = renderer._geometry
        tracks = list(paths.values())
        self.assertNotEqual(tracks[0], tracks[1])
        self.assertTrue(all(" Q" in track for track in tracks))
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


if __name__ == "__main__":
    unittest.main()
