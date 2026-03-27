"""InRiver API key authentication."""

from hotglue_singer_sdk.target_sdk.auth import ApiAuthenticator


class InRiverAuthenticator(ApiAuthenticator):
    """X-inRiver-APIKey header."""

    def __init__(self, target, state=None):
        if state is None:
            state = {}
        super().__init__(
            target,
            state,
            header_name="X-inRiver-APIKey",
            header_value_prefix="",
            config_key="api_key",
        )
