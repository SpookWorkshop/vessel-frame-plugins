from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import Any

from vf_core.message_bus import MessageBus
from vf_core.plugin_types import (
    ConfigSchema,
    ConfigField,
    ConfigFieldType,
    Plugin,
    require_plugin_args,
)

_RECONNECT_DELAY: float = 5.0


class TCPMessageSource(Plugin):
    """Source plugin that receives AIS sentences over a TCP connection.

    Connects to a remote host and reads NMEA sentences line by line.
    Automatically reconnects if the connection is lost.
    """

    def __init__(
        self,
        *,
        bus: MessageBus,
        host: str,
        port: int = 10110,
        topic: str = "ais.raw",
        **kwargs: Any,
    ) -> None:
        require_plugin_args(bus=bus, host=host)
        self._logger = logging.getLogger(__name__)
        self._bus = bus
        self._host = host
        self._port = port
        self._topic = topic
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the TCP connection loop."""
        if self._task and not self._task.done():
            return

        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Stop the TCP connection loop."""
        if self._task and not self._task.done():
            self._task.cancel()

            with suppress(asyncio.CancelledError):
                await self._task

    async def _loop(self) -> None:
        """Connect to the remote host and read lines, reconnecting on failure."""
        while True:
            try:
                self._logger.info(f"Connecting to {self._host}:{self._port}")
                reader, writer = await asyncio.open_connection(self._host, self._port)
                self._logger.info(f"Connected to {self._host}:{self._port}")

                try:
                    while True:
                        line = await reader.readline()
                        if not line:
                            # Remote closed the connection
                            self._logger.warning("TCP connection closed by remote")
                            break

                        message = line.decode("ascii", errors="ignore").strip()
                        if message:
                            await self._bus.publish(self._topic, message)
                finally:
                    writer.close()
                    with suppress(Exception):
                        await writer.wait_closed()

            except asyncio.CancelledError:
                raise
            except Exception:
                self._logger.exception(
                    f"TCP connection to {self._host}:{self._port} failed"
                )

            self._logger.info(f"Reconnecting in {_RECONNECT_DELAY}s")
            await asyncio.sleep(_RECONNECT_DELAY)


def get_config_schema() -> ConfigSchema:
    return ConfigSchema(
        plugin_name="tcp_message_source",
        plugin_type="source",
        fields=[
            ConfigField(
                key="host",
                label="Host",
                field_type=ConfigFieldType.STRING,
                default="",
                required=True,
                description="Hostname or IP address to connect to",
            ),
            ConfigField(
                key="port",
                label="Port",
                field_type=ConfigFieldType.INTEGER,
                default=10110,
                description="TCP port to connect to",
            ),
        ],
    )


def make_plugin(**kwargs: Any) -> Plugin:
    return TCPMessageSource(**kwargs)
