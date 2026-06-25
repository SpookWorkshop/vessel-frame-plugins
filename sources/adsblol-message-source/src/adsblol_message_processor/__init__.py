from __future__ import annotations

import asyncio
import logging
import math
from contextlib import suppress
from typing import Any

import httpx
from vf_core.message_bus import MessageBus
from vf_core.plugin_types import (
    ConfigField,
    ConfigFieldType,
    ConfigSchema,
    Plugin,
    require_plugin_args,
)

_API_BASE_URL = "https://api.adsb.lol"

# adsb.lol caps the search radius at 250 nautical miles.
_MAX_RADIUS_NM = 250

# Nautical miles per kilometre.
_KM_PER_NM = 1.852

_DEFAULT_POLL_INTERVAL = 10

_INITIAL_RETRY_DELAY: float = 5.0
_MAX_RETRY_DELAY: float = 60.0

_REQUEST_TIMEOUT: float = 30.0


class ADSBLOLMessageProcessor(Plugin):
    """
    Combined source+processor plugin for adsb.lol.

    Polls the adsb.lol REST API for aircraft within a radius of the
    bounding box. Normalises the returned data into the commond
    vessel format and publishes to the bus.
    """

    def __init__(
        self,
        *,
        bus: MessageBus,
        bounding_box: dict[str, float] | None = None,
        poll_interval: int = _DEFAULT_POLL_INTERVAL,
        out_topic: str = "vessel.decoded",
        **kwargs: Any,
    ) -> None:
        require_plugin_args(bus=bus)
        self._logger = logging.getLogger(__name__)
        self._bus = bus
        self._bounding_box = bounding_box
        self._poll_interval = max(1, int(poll_interval or _DEFAULT_POLL_INTERVAL))
        self._out_topic = out_topic
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        """Start the polling loop"""
        if self._task and not self._task.done():
            return

        if not self._bounding_box:
            self._logger.error(
                "adsb.lol bounding box is not configured, plugin will not start"
            )
            return

        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Stop the polling loop and wait for shutdown."""
        self._running = False

        if self._task and not self._task.done():
            self._task.cancel()

            with suppress(asyncio.CancelledError):
                await self._task

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _query_geometry(self) -> tuple[float, float, int]:
        """
        Convert the configured bounding box to a centre point and radius.
        """
        bb = self._bounding_box or {}
        min_lat, max_lat = bb["min_lat"], bb["max_lat"]
        min_lon, max_lon = bb["min_lon"], bb["max_lon"]

        centre_lat = (min_lat + max_lat) / 2
        centre_lon = (min_lon + max_lon) / 2

        # Get radius from bbox by finding out distance from centre to a corner
        diagonal_km = _haversine_km(centre_lat, centre_lon, max_lat, max_lon)
        radius_nm = math.ceil(diagonal_km / _KM_PER_NM)
        radius_nm = max(1, min(radius_nm, _MAX_RADIUS_NM))

        return centre_lat, centre_lon, radius_nm

    def _normalise(self, aircraft: dict[str, Any]) -> dict[str, Any] | None:
        """
        Map an adsb.lol aircraft object to our standard format.
        """
        hex_id = aircraft.get("hex")
        if not isinstance(hex_id, str) or not hex_id.strip():
            return None

        msg: dict[str, Any] = {
            "identifier": hex_id.strip().lower(),
            "source_type": "adsb",
        }

        name = _clean_str(aircraft.get("r"))
        if name:
            msg["name"] = name

        category = _clean_str(aircraft.get("category"))
        ext = {
            "route": _clean_str(aircraft.get("flight")),
            "aircraft_type": _clean_str(aircraft.get("t")),
            "category": category,
            "category_name": _get_category_name(category),
        }
        ext = {k: v for k, v in ext.items() if v is not None}
        if ext:
            msg["extension"] = ext

        # Dynamic data mapped to same keys as AIS to make multi-type screens easier
        _copy_if_present(aircraft, msg, "lat", "lat")
        _copy_if_present(aircraft, msg, "lon", "lon")
        _copy_if_present(aircraft, msg, "gs", "speed")
        _copy_if_present(aircraft, msg, "track", "course")

        heading = aircraft.get("true_heading")
        if heading is None:
            heading = aircraft.get("mag_heading")
        if heading is not None:
            msg["heading"] = heading

        altitude = aircraft.get("alt_baro")
        if altitude == "ground":
            altitude = 0
        if altitude is not None:
            msg["altitude"] = altitude

        _copy_if_present(aircraft, msg, "roll", "roll")
        _copy_if_present(aircraft, msg, "baro_rate", "vertical_rate")
        _copy_if_present(aircraft, msg, "squawk", "squawk")
        _copy_if_present(aircraft, msg, "emergency", "emergency")

        return msg

    # ------------------------------------------------------------------
    # Polling loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        """Poll the API on an interval with backoff retries"""
        centre_lat, centre_lon, radius_nm = self._query_geometry()
        url = f"{_API_BASE_URL}/v2/point/{centre_lat}/{centre_lon}/{radius_nm}"
        self._logger.info(
            "Polling adsb.lol every %ss for aircraft within %snm of "
            "(%.4f, %.4f)",
            self._poll_interval,
            radius_nm,
            centre_lat,
            centre_lon,
        )

        delay = _INITIAL_RETRY_DELAY
        async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT) as client:
            while self._running:
                try:
                    await self._poll_once(client, url)
                    delay = _INITIAL_RETRY_DELAY
                    await asyncio.sleep(self._poll_interval)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._logger.exception(
                        "adsb.lol poll failed, retrying in %.0fs", delay
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, _MAX_RETRY_DELAY)

    async def _poll_once(self, client: httpx.AsyncClient, url: str) -> None:
        """Fetch a snapshot and publish."""
        response = await client.get(url, headers={"Accept": "application/json"})
        response.raise_for_status()
        payload = response.json()

        aircraft_list = payload.get("ac") or []
        published = 0
        for aircraft in aircraft_list:
            if not self._running:
                return
            try:
                normalised = self._normalise(aircraft)
                if normalised is not None:
                    await self._bus.publish(self._out_topic, normalised)
                    published += 1
            except Exception:
                self._logger.exception("Failed to process adsb.lol aircraft")

        self._logger.debug(
            "Published %d/%d aircraft from adsb.lol", published, len(aircraft_list)
        )


# Aircraft category table for converting ADSB cats into readable names.
# I didn't put the weights in but noted them in comments in case we decide to add in later.
_CATEGORY_NAMES: dict[str, str] = {
    "A0": "Unknown",
    "A1": "Light", # < 15,500 lb
    "A2": "Small", # 15,500-75,000 lb
    "A3": "Large", # 75,000-300,000 lb
    "A4": "High Vortex Large",
    "A5": "Heavy", # > 300,000 lb
    "A6": "High Performance",
    "A7": "Rotorcraft",
    "B0": "Unknown",
    "B1": "Glider / Sailplane",
    "B2": "Lighter-than-air",
    "B3": "Parachutist / Skydiver",
    "B4": "Ultralight / Hang-glider",
    "B5": "Reserved",
    "B6": "UAV / Drone",
    "B7": "Space / Trans-atmospheric",
    "C0": "Unknown",
    "C1": "Surface - Emergency Vehicle",
    "C2": "Surface - Service Vehicle",
    "C3": "Point Obstacle",
    "C4": "Cluster Obstacle",
    "C5": "Line Obstacle",
    "C6": "Reserved",
    "C7": "Reserved",
}


# ------------------------------------------------------------------
# Module-level helpers (no self needed)
# ------------------------------------------------------------------

def _get_category_name(category: str | None) -> str | None:
    """Return a human-readable emitter category, or None if unrecognised."""
    if category is None:
        return None
    return _CATEGORY_NAMES.get(category.upper())


def _clean_str(raw: Any) -> str | None:
    """Return a stripped string, or None if empty/non-string."""
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    return value if value else None


def _copy_if_present(src: dict, dst: dict, src_key: str, dst_key: str) -> None:
    value = src.get(src_key)
    if value is not None:
        dst[dst_key] = value


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance between two coords in km."""
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return 6371.0 * c


# ------------------------------------------------------------------
# Plugin entry points
# ------------------------------------------------------------------

def get_config_schema() -> ConfigSchema:
    return ConfigSchema(
        plugin_name="adsblol_message_processor",
        plugin_type="processor",
        fields=[
            ConfigField(
                key="bounding_box",
                label="Bounding Box",
                field_type=ConfigFieldType.BBOX,
                default=None,
                description="Area to receive ADSB data for",
            ),
            ConfigField(
                key="poll_interval",
                label="Poll Interval (s)",
                field_type=ConfigFieldType.INTEGER,
                default=_DEFAULT_POLL_INTERVAL,
                required=False,
                description="Seconds between API update calls",
            ),
        ],
    )


def make_plugin(**kwargs: Any) -> Plugin:
    return ADSBLOLMessageProcessor(**kwargs)
