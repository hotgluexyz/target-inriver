"""Base sink with InRiver REST base URL and auth."""

from __future__ import annotations

from hotglue_singer_sdk.target_sdk.client import HotglueSink

from target_inriver.auth import InRiverAuthenticator


class InRiverSink(HotglueSink):
    """Shared REST settings for InRiver targets."""

    auto_validate_unified_schema = False

    @property
    def base_url(self) -> str:
        raw = self._config.get("base_url") or self._config.get("api_url_base") or ""
        return str(raw).rstrip("/")

    @property
    def authenticator(self):
        return InRiverAuthenticator(self._target, {})
