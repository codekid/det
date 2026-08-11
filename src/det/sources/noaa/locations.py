from __future__ import annotations

from typing import Any

from det.sources.noaa.storm_events import NoaaStormEventsSource

DEFAULT_FILENAME_SUBSTR = "locations-ftp"


class NoaaLocationsSource(NoaaStormEventsSource):
    """
    NOAA Storm Events location CSVs from the same NCEI index as details/fatalities.

    Files match ``StormEvents_locations-ftp_*.csv.gz``; extract/parse reuse the
    shared storm-events CSV downloader.

    Interval mode: ``year_files`` (inherited). Lands gzip CSV bytes wire-faithful.
    """

    name = "noaa.locations"

    def defaults(self) -> dict[str, Any]:
        return {
            **super().defaults(),
            "filename_substr": DEFAULT_FILENAME_SUBSTR,
        }
