from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from typing import Any

import websockets
from vf_core.message_bus import MessageBus
from vf_core.plugin_types import (
    ConfigField,
    ConfigFieldType,
    ConfigSchema,
    Plugin,
    require_plugin_args,
)

_WSS_URL = "wss://stream.aisstream.io/v0/stream"

_SUBSCRIBE_MESSAGE_TYPES = [
    "PositionReport",
    "ExtendedClassBPositionReport",
    "ShipStaticData",
    "StandardClassBPositionReport",
    "StaticDataReport",
]

_INITIAL_RECONNECT_DELAY: float = 5.0
_MAX_RECONNECT_DELAY: float = 60.0

# Vessel type lookup kept in sync with ais_decoder_processor.
_VESSEL_TYPES: dict[int, str] = {
    -1: "Unknown",
    0: "Unknown",
    20: "Wing in Ground",
    30: "Fishing",
    31: "Towing",
    32: "Towing (Large)",
    33: "Dredge",
    34: "Diving Vessel",
    35: "Military Ops",
    36: "Sailing",
    37: "Pleasure Craft",
    40: "High Speed Craft",
    50: "Pilot Vessel",
    51: "Search & Rescue",
    52: "Tug",
    53: "Port Tender",
    54: "Anti-pollution Equip.",
    55: "Law Enforcement",
    56: "Local",
    57: "Local",
    58: "Medical Transport",
    59: "Non-combatant Ship",
    60: "Passenger Ship",
    70: "Cargo Ship",
    80: "Tanker",
    90: "Other",
}

_VESSEL_SUBCATS: dict[int, str] = {
    1: "Hazardous (High)",
    2: "Hazardous",
    3: "Hazardous (Low)",
    4: "Non-hazardous",
}


def _get_vessel_full_type_name(type_code: int | None) -> str:
    """Return a human-readable vessel type string matching ais_decoder_processor output."""
    if type_code is None:
        return "Unknown"

    if type_code in _VESSEL_TYPES:
        return _VESSEL_TYPES[type_code]

    base_cat = (type_code // 10) * 10
    vessel_type = _VESSEL_TYPES.get(base_cat)

    if vessel_type is None:
        return "Reserved"

    sub_name = _VESSEL_SUBCATS.get(type_code % 10)
    if sub_name:
        return f"{vessel_type} - {sub_name}"

    return vessel_type


class AISStreamIOMessageProcessor(Plugin):
    """
    Combined source+processor plugin for aisstream.io.

    Connects to the aisstream.io WebSocket feed, receives pre-decoded AIS JSON
    messages, normalises them into VesselFrame's vessel.decoded format and
    publishes directly to the bus.

    Reconnects automatically on connection loss using exponential back off.
    """

    def __init__(
        self,
        *,
        bus: MessageBus,
        api_key: str | None = None,
        bounding_box: dict[str, float] | None = None,
        out_topic: str = "vessel.decoded",
        **kwargs: Any,
    ) -> None:
        require_plugin_args(bus=bus)
        self._logger = logging.getLogger(__name__)
        self._bus = bus
        self._api_key = api_key or None
        self._bounding_box = bounding_box
        self._out_topic = out_topic
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        """Start the WebSocket receive loop. No-op if already running."""
        if self._task and not self._task.done():
            return

        if not self._api_key:
            self._logger.error(
                "aisstream.io API key is not configured, plugin will not start"
            )
            return

        if not self._bounding_box:
            self._logger.error(
                "aisstream.io bounding box is not configured, plugin will not start"
            )
            return

        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Stop the receive loop and wait for clean shutdown."""
        self._running = False

        if self._task and not self._task.done():
            self._task.cancel()

            with suppress(asyncio.CancelledError):
                await self._task

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_subscribe_payload(self) -> str:
        bb = self._bounding_box
        bounding_boxes = [[[bb["min_lat"], bb["min_lon"]], [bb["max_lat"], bb["max_lon"]]]]
        return json.dumps({
            "APIKey": self._api_key,
            "BoundingBoxes": bounding_boxes,
            "FilterMessageTypes": _SUBSCRIBE_MESSAGE_TYPES,
        })

    # Probably not required because aisstream should filter this for us already
    def _is_valid_vessel(self, mmsi: str) -> bool:
        if len(mmsi) != 9:
            self._logger.debug(f"MMSI {mmsi!r} is not a trackable vessel, skipping")
            return False
        if mmsi.startswith("111"):
            return False
        return True

    def _normalise(self, message: dict[str, Any]) -> dict[str, Any] | None:
        """
        Map an aisstream message to VesselFrame's vessel.decoded format.

        Returns None for messages that should be discarded (invalid MMSI,
        unknown message type, unrecognised structure)
        """
        message_type = message.get("MessageType")
        payload = message.get("Message", {}).get(message_type)

        if not payload or not isinstance(payload, dict):
            return None

        mmsi = str(payload.get("UserID", ""))
        if not self._is_valid_vessel(mmsi):
            return None

        msg: dict[str, Any] = {
            "identifier": mmsi,
            "source_type": "ais",
        }

        if message_type in ("PositionReport", "StandardClassBPositionReport"):
            msg.update(_normalise_position(payload))

        elif message_type == "ExtendedClassBPositionReport":
            msg.update(_normalise_position(payload))
            name = _clean_name(payload.get("Name"))
            if name:
                msg["name"] = name

        elif message_type == "ShipStaticData":
            name = _clean_name(payload.get("Name"))
            if name:
                msg["name"] = name

            ship_type = payload.get("Type")
            dimension = payload.get("Dimension") or {}
            ext: dict[str, Any] = {
                "imo":            payload.get("ImoNumber"),
                "callsign":       payload.get("CallSign"),
                "ship_type":      ship_type,
                "ship_type_name": _get_vessel_full_type_name(ship_type),
                "bow":            dimension.get("A"),
                "stern":          dimension.get("B"),
                "port":           dimension.get("C"),
                "starboard":      dimension.get("D"),
            }
            msg["extension"] = {k: v for k, v in ext.items() if v is not None}

        elif message_type == "StaticDataReport":
            report_a = payload.get("ReportA") or {}
            report_b = payload.get("ReportB") or {}

            name = _clean_name(report_a.get("Name"))
            if name:
                msg["name"] = name

            if report_b:
                ship_type = report_b.get("Type")
                dimension = report_b.get("Dimension") or {}
                ext = {
                    "callsign":       report_b.get("CallSign"),
                    "ship_type":      ship_type,
                    "ship_type_name": _get_vessel_full_type_name(ship_type),
                    "bow":            dimension.get("A"),
                    "stern":          dimension.get("B"),
                    "port":           dimension.get("C"),
                    "starboard":      dimension.get("D"),
                }
                ext = {k: v for k, v in ext.items() if v is not None}
                if ext:
                    msg["extension"] = ext

        else:
            return None

        return msg

    # ------------------------------------------------------------------
    # Connection loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        """Outer reconnect loop. Connect, receive, back off on failure."""
        subscribe_payload = self._build_subscribe_payload()
        delay = _INITIAL_RECONNECT_DELAY

        while self._running:
            try:
                await self._connect_and_receive(subscribe_payload)
                # Clean close from the server side
                if self._running:
                    self._logger.warning(
                        "aisstream.io connection closed by server, reconnecting "
                        f"in {_INITIAL_RECONNECT_DELAY:.0f}s"
                    )
                delay = _INITIAL_RECONNECT_DELAY
            except asyncio.CancelledError:
                raise
            except Exception:
                self._logger.exception(
                    f"aisstream.io connection error, reconnecting in {delay:.0f}s"
                )

            if self._running:
                await asyncio.sleep(delay)
                delay = min(delay * 2, _MAX_RECONNECT_DELAY)

    async def _connect_and_receive(self, subscribe_payload: str) -> None:
        """
        Open a single WebSocket session and receive messages until it closes.

        Messages are processed inline rather than queued so that the server
        never sees a backlog of unread frames, which can trigger a forced close.
        """
        self._logger.info("Connecting to aisstream.io...")

        async with websockets.connect(_WSS_URL) as ws:
            await ws.send(subscribe_payload)
            self._logger.info("Connected and subscribed to aisstream.io")

            async for raw in ws:
                if not self._running:
                    return

                try:
                    normalised = self._normalise(json.loads(raw))
                    if normalised is not None:
                        await self._bus.publish(self._out_topic, normalised)
                except Exception:
                    self._logger.exception("Failed to process aisstream.io message")


# ------------------------------------------------------------------
# Module-level helpers (no self needed)
# ------------------------------------------------------------------

def _clean_name(raw: Any) -> str | None:
    """Return a stripped name string, or None if empty/non-string."""
    if not isinstance(raw, str):
        return None
    name = raw.strip()
    return name if name else None


def _normalise_position(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Extract position fields from an aisstream payload.

    Field names are mapped to match pyais output so that downstream consumers
    (screens, controllers) work identically regardless of which source produced
    the vessel.decoded message.
    """
    result: dict[str, Any] = {}

    _copy_if_present(payload, result, "Latitude", "lat")
    _copy_if_present(payload, result, "Longitude", "lon")
    _copy_if_present(payload, result, "Sog", "speed")
    _copy_if_present(payload, result, "Cog", "course")
    _copy_if_present(payload, result, "TrueHeading", "heading")
    _copy_if_present(payload, result, "NavigationalStatus", "status")
    _copy_if_present(payload, result, "RateOfTurn", "turn")
    _copy_if_present(payload, result, "PositionAccuracy", "accuracy")

    return result


def _copy_if_present(src: dict, dst: dict, src_key: str, dst_key: str) -> None:
    value = src.get(src_key)
    if value is not None:
        dst[dst_key] = value


# ------------------------------------------------------------------
# Plugin entry points
# ------------------------------------------------------------------

def get_config_schema() -> ConfigSchema:
    return ConfigSchema(
        plugin_name="aisstreamio_message_processor",
        plugin_type="processor",
        fields=[
            ConfigField(
                key="api_key",
                label="API Key",
                field_type=ConfigFieldType.STRING,
                default=None,
                description="Your aisstream.io API key",
            ),
            ConfigField(
                key="bounding_box",
                label="Bounding Box",
                field_type=ConfigFieldType.BBOX,
                default=None,
                description="Geographic area to receive AIS data for",
            ),
        ],
    )


def make_plugin(**kwargs: Any) -> Plugin:
    return AISStreamIOMessageProcessor(**kwargs)
