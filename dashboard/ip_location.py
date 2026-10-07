"""Background IP geolocation with cached UNECE UN/LOCODE matching."""

from __future__ import annotations

import csv
import io
import ipaddress
import json
import queue
import re
import sys
import threading
import time
import unicodedata
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass

from history_store import HistoryStore

PENDING_LOCATION = {"label": "🌐 ---", "title": "Location lookup pending"}
LOCAL_LOCATION = {"label": "🏠 LAN", "title": "Local or non-public address"}
LOOKUP_RETRY_SECONDS = 60 * 60
UNLOCODE_COUNTRY_URL = (
    "https://opensource.unicc.org/un/unece/uncefact/"
    "vocab-locode/-/raw/main/locodes/{country}.csv"
)
MAX_UNLOCODE_COUNTRY_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class GeoLocation:
    country_code: str
    city: str
    region: str
    region_code: str


@dataclass(frozen=True)
class CachedLocation:
    country_code: str
    location_code: str
    city: str
    region: str

    def api_value(self) -> dict[str, str]:
        title = "，".join(part for part in (self.city, self.region) if part)
        return {
            "label": f"{_country_flag(self.country_code)} {self.location_code}",
            "title": title,
        }


def _endpoint_ip(endpoint: str) -> str | None:
    if endpoint.startswith("["):
        closing = endpoint.find("]")
        port = endpoint[closing + 2 :] if endpoint[closing + 1 : closing + 2] == ":" else ""
        if closing <= 1 or not port.isdigit():
            return None
        candidate = endpoint[1:closing]
    else:
        candidate, separator, port = endpoint.rpartition(":")
        if not separator or not port.isdigit():
            return None
    try:
        return str(ipaddress.ip_address(candidate))
    except ValueError:
        return None


def _country_flag(country_code: str) -> str:
    code = country_code.upper()
    if len(code) != 2 or not code.isalpha() or not code.isascii():
        return "🌐"
    return "".join(chr(0x1F1E6 + ord(letter) - ord("A")) for letter in code)


def _normalize_name(name: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", ascii_name.casefold())


def query_ip_location(ip: str) -> GeoLocation:
    encoded_ip = urllib.parse.quote(ip, safe=":")
    request = urllib.request.Request(
        f"https://ipapi.co/{encoded_ip}/json/",
        headers={"User-Agent": "RelayWeave-Dashboard/1.0"},
    )
    with urllib.request.urlopen(request, timeout=3.0) as response:
        data = json.load(response)
    if data.get("error"):
        raise RuntimeError(str(data.get("reason") or "IP location lookup failed"))

    country_code = str(data.get("country_code") or "").upper()
    city = str(data.get("city") or "")
    if len(country_code) != 2 or not country_code.isalpha() or not city:
        raise RuntimeError("IP location response has no country code or city")
    return GeoLocation(
        country_code=country_code,
        city=city,
        region=str(data.get("region") or ""),
        region_code=str(data.get("region_code") or "").upper(),
    )


def download_unlocode_country(country_code: str) -> str:
    if (
        len(country_code) != 2
        or not country_code.isalpha()
        or not country_code.isascii()
    ):
        raise ValueError("invalid UN/LOCODE country code")
    url = UNLOCODE_COUNTRY_URL.format(country=country_code.upper())
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "RelayWeave-Dashboard/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30.0) as response:
        payload = response.read(MAX_UNLOCODE_COUNTRY_BYTES + 1)
    if len(payload) > MAX_UNLOCODE_COUNTRY_BYTES:
        raise RuntimeError("UN/LOCODE country file is too large")
    return payload.decode("utf-8-sig")


class UnLocodeIndex:
    """Load and cache the official UNECE CSV for each country on demand."""

    def __init__(
        self,
        store: HistoryStore,
        download: Callable[[str], str] = download_unlocode_country,
    ) -> None:
        self._store = store
        self._download = download
        self._countries: dict[str, dict[str, list[tuple[str, str]]]] = {}

    def location_code(self, geo: GeoLocation) -> str | None:
        country = geo.country_code.upper()
        entries = self._countries.get(country)
        if entries is None:
            csv_data = self._store.load_unlocode_country(country)
            if csv_data is None:
                csv_data = self._download(country)
                self._store.save_unlocode_country(country, csv_data)
            entries = self._parse_country(country, csv_data)
            self._countries[country] = entries

        candidates = entries.get(_normalize_name(geo.city), [])
        if not candidates:
            return None
        region = geo.region_code.upper()
        for subdivision, location_code in candidates:
            if region and subdivision.upper() == region:
                return location_code
        return candidates[0][1]

    @staticmethod
    def _parse_country(
        country: str,
        csv_data: str,
    ) -> dict[str, list[tuple[str, str]]]:
        result: dict[str, list[tuple[str, str]]] = {}
        for row in csv.reader(io.StringIO(csv_data)):
            if (
                len(row) < 6
                or row[0].strip() == "X"
                or row[1].strip().upper() != country
            ):
                continue
            location_code = row[2].strip().upper()
            if len(location_code) != 3:
                continue
            subdivision = row[5].strip().upper()
            for name in {row[3].strip(), row[4].strip()}:
                normalized = _normalize_name(name)
                if normalized:
                    candidate = (subdivision, location_code)
                    if candidate not in result.setdefault(normalized, []):
                        result[normalized].append(candidate)
        return result


class IpLocationCache:
    """Return cached locations immediately and resolve cache misses in one worker."""

    def __init__(
        self,
        store: HistoryStore,
        lookup: Callable[[str], GeoLocation] = query_ip_location,
        unlocode: UnLocodeIndex | None = None,
    ) -> None:
        self._store = store
        self._lookup = lookup
        self._unlocode = unlocode or UnLocodeIndex(store)
        self._locations = {
            ip: CachedLocation(*location)
            for ip, location in store.load_ip_locations().items()
        }
        self._pending: set[str] = set()
        self._retry_after: dict[str, float] = {}
        self._lock = threading.Lock()
        self._queue: queue.Queue[str | None] = queue.Queue()
        self._stopping = threading.Event()
        self._worker = threading.Thread(
            target=self._run,
            name="dashboard-ip-location",
            daemon=True,
        )
        self._worker.start()

    def location_for_endpoint(self, endpoint: str) -> dict[str, str]:
        ip = _endpoint_ip(endpoint)
        if ip is None:
            return PENDING_LOCATION
        address = ipaddress.ip_address(ip)
        if not address.is_global:
            return LOCAL_LOCATION

        now = time.monotonic()
        with self._lock:
            cached = self._locations.get(ip)
            if cached:
                return cached.api_value()
            if (
                not self._stopping.is_set()
                and ip not in self._pending
                and now >= self._retry_after.get(ip, 0.0)
            ):
                self._pending.add(ip)
                self._queue.put(ip)
        return PENDING_LOCATION

    def close(self) -> None:
        self._stopping.set()
        self._queue.put(None)
        self._worker.join()

    def _run(self) -> None:
        while not self._stopping.is_set():
            ip = self._queue.get()
            if ip is None or self._stopping.is_set():
                return
            try:
                geo = self._lookup(ip)
                location = CachedLocation(
                    country_code=geo.country_code,
                    location_code=self._unlocode.location_code(geo) or "---",
                    city=geo.city,
                    region=geo.region,
                )
                self._store.save_ip_location(
                    ip,
                    location.country_code,
                    location.location_code,
                    location.city,
                    location.region,
                )
                with self._lock:
                    self._locations[ip] = location
                    self._retry_after.pop(ip, None)
            except Exception as error:
                with self._lock:
                    self._retry_after[ip] = time.monotonic() + LOOKUP_RETRY_SECONDS
                print(f"[dashboard] IP location lookup failed for {ip}: {error}", file=sys.stderr)
            finally:
                with self._lock:
                    self._pending.discard(ip)
