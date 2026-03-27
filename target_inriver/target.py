"""InRiver Singer target."""

from __future__ import annotations

from typing import Any, Optional, Union

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from target_inriver.sinks import ItemSizeSink, ProductItemSink, ProductSink


class TargetInriver(TargetHotglue):
    """Load Product, ProductItem, and ItemSize streams into InRiver."""

    name = "target-inriver"
    SINK_TYPES = [ProductSink, ProductItemSink, ItemSizeSink]
    MAX_PARALLELISM = 1

    config_jsonschema = th.PropertiesList(
        th.Property("api_key", th.StringType, required=True, description="X-inRiver-APIKey value"),
        th.Property(
            "base_url",
            th.StringType,
            required=False,
            description="API root without trailing slash (e.g. https://...productmarketingcloud.com)",
        ),
        th.Property(
            "api_url_base",
            th.StringType,
            required=False,
            description="Alias for base_url (matches tap-inriver-style config)",
        ),
        th.Property(
            "stream_maps",
            th.ObjectType(),
            required=False,
            description="Hotglue PluginMapper stream_maps",
        ),
        th.Property(
            "stream_map_config",
            th.ObjectType(),
            required=False,
            description="Hotglue PluginMapper stream_map_config",
        ),
    ).to_dict()

    def __init__(
        self,
        config: Optional[Union[dict, Any]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: Optional[str] = None,
    ) -> None:
        self.config_file = None
        if isinstance(config, (list, tuple)) and len(config) > 0:
            self.config_file = config[0]
        super().__init__(config, parse_env_config, validate_config, state)
        if not (self.config.get("base_url") or self.config.get("api_url_base")):
            raise ValueError("Configuration must set base_url or api_url_base.")


if __name__ == "__main__":
    TargetInriver.cli()
