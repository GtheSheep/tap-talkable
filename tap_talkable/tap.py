"""Talkable tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_talkable.streams import (
    CampaignsStream,
    CampaignMetricsStream,
    TrafficSourcesStream,
)

STREAM_TYPES = [
    CampaignsStream,
    CampaignMetricsStream,
    TrafficSourcesStream,
]


class TapTalkable(Tap):
    """Talkable tap class."""
    name = "tap-talkable"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service"
        ),
        th.Property(
            "site_slug",
            th.StringType,
            required=True,
            description="The slug name for your site"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2019-01-01T00:00:00Z",
            description="Date to start collecting metrics from"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
