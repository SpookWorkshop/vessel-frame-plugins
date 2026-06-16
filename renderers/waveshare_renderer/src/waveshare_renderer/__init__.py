from __future__ import annotations
from typing import Any
import logging
from vf_core.plugin_types import (
    ConfigField,
    ConfigFieldType,
    ConfigSchema,
    RendererPlugin,
)
from PIL import Image, ImageDraw
from pathlib import Path
import epaper

class WaveshareRenderer(RendererPlugin):
    """Renderer plugin that draws a Pillow canvas to a Waveshare ePaper screen."""

    MIN_RENDER_INTERVAL: int = 0

    def __init__(
        self,
        *,
        data_dir: Path,
        display: str = "epd7in5_V2",
        width: int = 480,
        height: int = 800,
        orientation: str = "portrait",
        **kwargs: Any,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        #self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="waveshare-display")

        self._display = self._load_display(display)
        self._display.init()

        # Swap width and height if they weren't passed in a way that expresses the orientation
        if (orientation == "portrait" and width > height) or (
            orientation == "landscape" and height > width
        ):
            self._width = int(height)
            self._height = int(width)
        else:
            self._width = int(width)
            self._height = int(height)

        self._canvas: Image.Image = Image.new("RGB", (self._width, self._height))

    def _load_display(self, module_name):
        available = epaper.modules()
        if module_name not in available:
            raise ValueError(f"Unknown display '{module_name}'")
        
        return epaper.epaper(module_name).EPD()

    async def flush(self) -> None:
        self._display.display(epd.getbuffer(self._canvas))

    def clear(self) -> None:
        """Clear the canvas by filling it with the background colour."""
        draw = ImageDraw.Draw(self._canvas)
        draw.rectangle(
            [(0, 0), (self._width, self._height)], fill=self.palette["background"]
        )

    @property
    def palette(self) -> dict[str, str]:
        """Colour palette for drawing operations.

        Returns:
            dict[str, str]: A mapping of theme colour names to hex values.
        """

        return {
            "background": "#0000FF",
            "foreground": "#FFFFFF",
            "line": "#0000FF",
            "text": "#0000FF",
            "icon": "#0000FF",
            "accent": "#000000",
        }

    @property
    def canvas(self) -> Image.Image:
        """Current Pillow image canvas."""
        return self._canvas

def get_config_schema() -> ConfigSchema:
    """Return the config schema for this plugin.

    Defines editable fields for the admin panel.

    Returns:
        ConfigSchema: Schema describing this plugin's configuration options.
    """
    return ConfigSchema(
        plugin_name="image_renderer",
        plugin_type="renderer",
        fields=[
            ConfigField(
                key="width",
                label="Width",
                field_type=ConfigFieldType.INTEGER,
                default=480,
                required=True
            ),
            ConfigField(
                key="height",
                label="Height",
                field_type=ConfigFieldType.INTEGER,
                default=800,
                required=True
            ),
            ConfigField(
                key="orientation",
                label="Orientation",
                field_type=ConfigFieldType.SELECT,
                default="portrait",
                options=["portrait", "landscape"],
            ),
        ],
    )

def make_plugin(**kwargs: Any) -> RendererPlugin:
    """
    Factory function required by the entry point.
    """
    return WaveshareRenderer(**kwargs)