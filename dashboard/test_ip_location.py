"""Tests for accessor IP location formatting and caching."""

from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from history_store import HistoryStore
from ip_location import (
    CachedLocation,
    GeoLocation,
    LOCAL_LOCATION,
    PENDING_LOCATION,
    IpLocationCache,
    UnLocodeIndex,
    _country_flag,
    _endpoint_ip,
)


class IpLocationTest(unittest.TestCase):
    def test_endpoint_and_location_formatting(self) -> None:
        self.assertEqual(_endpoint_ip("8.8.8.8:53"), "8.8.8.8")
        self.assertEqual(_endpoint_ip("[2001:4860:4860::8888]:53"), "2001:4860:4860::8888")
        self.assertIsNone(_endpoint_ip("[2001:4860:4860::8888]"))
        self.assertIsNone(_endpoint_ip("not-an-endpoint"))
        self.assertEqual(_country_flag("CN"), "🇨🇳")
        self.assertEqual(
            CachedLocation("CN", "HIS", "Huangshi", "Hubei").api_value(),
            {"label": "🇨🇳 HIS", "title": "Huangshi，Hubei"},
        )

    def test_unlocode_country_is_downloaded_once_and_matched(self) -> None:
        csv_data = (
            ",CN,HIS,Huangshi,Huangshi,13,-23-----,AS,2107,,,\n"
            ",CN,HSI,Huangshi Pt,Huangshi Pt,HB,1-------,AS,1407,,,\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            store = HistoryStore(Path(directory) / "history.sqlite3")
            downloads: list[str] = []
            index = UnLocodeIndex(store, lambda country: downloads.append(country) or csv_data)
            geo = GeoLocation("CN", "Huangshi", "Hubei", "HB")
            try:
                self.assertEqual(index.location_code(geo), "HIS")
                self.assertEqual(index.location_code(geo), "HIS")
                self.assertEqual(downloads, ["CN"])
                self.assertEqual(store.load_unlocode_country("CN"), csv_data)
            finally:
                store.close()

    def test_private_address_never_uses_network(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = HistoryStore(Path(directory) / "history.sqlite3")
            calls: list[str] = []
            cache = IpLocationCache(
                store,
                lambda ip: calls.append(ip) or GeoLocation("US", "Nowhere", "", ""),
            )
            try:
                self.assertEqual(cache.location_for_endpoint("192.168.1.2:5000"), LOCAL_LOCATION)
                self.assertEqual(calls, [])
            finally:
                cache.close()
                store.close()

    def test_miss_is_resolved_once_and_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "history.sqlite3"
            store = HistoryStore(database)
            calls: list[str] = []
            lookup = lambda ip: calls.append(ip) or GeoLocation(
                "US", "Mountain View", "California", "CA"
            )
            unlocode = UnLocodeIndex(
                store,
                lambda country: ",US,MTV,Mountain View,Mountain View,CA,---4----,AS,2107,,,\n",
            )
            cache = IpLocationCache(store, lookup, unlocode)
            try:
                self.assertEqual(cache.location_for_endpoint("8.8.8.8:53"), PENDING_LOCATION)
                deadline = time.monotonic() + 1.0
                while time.monotonic() < deadline:
                    location = cache.location_for_endpoint("8.8.8.8:53")
                    if location != PENDING_LOCATION:
                        break
                    time.sleep(0.01)
                self.assertEqual(
                    location,
                    {"label": "🇺🇸 MTV", "title": "Mountain View，California"},
                )
                self.assertEqual(calls, ["8.8.8.8"])
            finally:
                cache.close()
                store.close()

            reopened_store = HistoryStore(database)
            reopened_calls: list[str] = []
            reopened_cache = IpLocationCache(
                reopened_store,
                lambda ip: reopened_calls.append(ip)
                or GeoLocation("US", "Unexpected", "", ""),
            )
            try:
                self.assertEqual(
                    reopened_cache.location_for_endpoint("8.8.8.8:53"),
                    {"label": "🇺🇸 MTV", "title": "Mountain View，California"},
                )
                self.assertEqual(reopened_calls, [])
            finally:
                reopened_cache.close()
                reopened_store.close()


if __name__ == "__main__":
    unittest.main()
