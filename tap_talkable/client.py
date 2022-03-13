"""REST client handling, including TalkableStream base class."""

from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import BearerTokenAuthenticator


class TalkableStream(RESTStream):
    """Talkable stream class."""

    url_base = "https://www.talkable.com/api/v2"

    records_jsonpath = "$.result[*]"
    next_page_token_jsonpath = "$.next_page"

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object."""
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config.get("api_key")
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def from_parent_context(self, context: dict):
        """Default is to return the dict passed in"""
        if self.partitions is None:
            return context
        else:
            for partition in self.partitions:  # pylint: disable=not-an-iterable
                partition.update(context.copy())  # Add copy of context to partition
            return None  # Context now handled at the partition level

    def _sync_children(self, child_context: dict) -> None:
        for child_stream in self.child_streams:
            if child_stream.selected or child_stream.has_selected_descendents:
                child_stream.sync(
                    child_stream.from_parent_context(context=child_context)
                )
