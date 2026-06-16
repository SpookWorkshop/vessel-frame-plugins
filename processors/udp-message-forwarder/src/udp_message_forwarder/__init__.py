from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import Any

from vf_core.message_bus import MessageBus
from vf_core.plugin_types import Plugin, ConfigSchema, ConfigField, ConfigFieldType, require_plugin_args


class UDPMessageForwarder(Plugin):
    """Processor plugin that forwards messages from the bus to a remote UDP endpoint."""

    def __init__(
        self,
        *,
        bus: MessageBus,
        host: str,
        port: int | str = 10110,
        in_topic: str = "ais.raw",
        **kwargs: Any,
    ) -> None:
        require_plugin_args(bus=bus, host=host)
        self._logger = logging.getLogger(__name__)
        self._bus = bus
        self._host = host
        self._port = int(port) if isinstance(port, str) else port
        self._in_topic = in_topic
        self._transport: asyncio.BaseTransport | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Open the UDP socket and start forwarding messages."""
        if self._task and not self._task.done():
            return

        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Cancel the forwarding task and close the UDP socket."""
        if self._task and not self._task.done():
            self._task.cancel()

            with suppress(asyncio.CancelledError):
                await self._task

        if self._transport is not None:
            self._transport.close()

    async def _loop(self) -> None:
        """Subscribe to the configured topic and forward each message via UDP."""
        try:
            loop = asyncio.get_running_loop()

            self._transport, _ = await loop.create_datagram_endpoint(
                asyncio.DatagramProtocol,
                remote_addr=(self._host, self._port),
            )

            self._logger.info(f"UDP forwarder ready, sending to {self._host}:{self._port}")

            async for message in self._bus.subscribe(self._in_topic):
                data = (message + "\r\n").encode("ascii", errors="ignore")
                self._transport.sendto(data)

        except asyncio.CancelledError:
            raise
        except Exception:
            self._logger.exception(f"UDP forwarder error ({self._host}:{self._port})")
        finally:
            if self._transport is not None:
                self._transport.close()
                self._transport = None


def get_config_schema() -> ConfigSchema:
    return ConfigSchema(
        plugin_name="udp_forwarder_processor",
        plugin_type="processor",
        fields=[
            ConfigField(
                key="host",
                label="Target Host",
                field_type=ConfigFieldType.STRING,
                default="",
                required=True,
                description="IP address or hostname to forward messages to",
            ),
            ConfigField(
                key="port",
                label="Target Port",
                field_type=ConfigFieldType.INTEGER,
                default=10110,
                description="UDP port to forward messages to",
            ),
            ConfigField(
                key="in_topic",
                label="Topic",
                field_type=ConfigFieldType.STRING,
                default="ais.raw",
                description="Message bus topic to forward",
            ),
        ],
    )


def make_plugin(**kwargs: Any) -> Plugin:
    return UDPMessageForwarder(**kwargs)
