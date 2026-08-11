from __future__ import annotations

from typing import Any

from det.sources.noaa.storm_events import NoaaStormEventsSource

DEFAULT_FILENAME_SUBSTR = "fatalities-ftp"


class NoaaFatalitiesSource(NoaaStormEventsSource):
    """
    NOAA Storm Events fatality CSVs from the same NCEI index as details/locations.

    Files match ``StormEvents_fatalities-ftp_*.csv.gz``; extract/parse reuse the
    shared storm-events CSV downloader.

    Interval mode: ``year_files`` (inherited). Lands gzip CSV bytes wire-faithful.
    """

    name = "noaa.fatalities"

    def defaults(self) -> dict[str, Any]:
        return {
            **super().defaults(),
            "filename_substr": DEFAULT_FILENAME_SUBSTR,
        }
