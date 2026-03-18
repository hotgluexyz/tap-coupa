"""Coupa tap class."""

from typing import List

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th  # JSON schema typing helpers
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_coupa.streams import InvoicesStream, InvoiceScansStream, InvoiceAttachmentsStream

STREAM_TYPES = [
    InvoicesStream,
    InvoiceScansStream,
    InvoiceAttachmentsStream,
]


class TapCoupa(Tap):
    """Coupa tap class."""
    name = "tap-coupa"
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = th.PropertiesList(
        th.Property("instance_name", th.StringType, required=True),
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("scope", th.StringType, default="core.common.read core.invoice.read"),
        th.Property("start_date", th.DateTimeType, default="2000-01-01T00:00:00.000Z"),
        th.Property("limit", th.IntegerType, default=50),
        th.Property(
            "resume_from_offset",
            th.IntegerType,
            description="Optional. Start invoice fetch from this API offset (e.g. 5001) instead of from the beginning.",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapCoupa.cli()
