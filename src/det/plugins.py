from __future__ import annotations

_LOADED = False


def load_plugins() -> None:
    """Register built-in sources, ingestion backends, and mappers (idempotent)."""
    global _LOADED
    if _LOADED:
        return

    from det.ingestion.dlt_backend import DltBackend
    from det.ingestion.thin_backend import ThinBackend
    from det.runtime.mappers import identity_mapper
    from det.runtime.registry import register_ingestion, register_mapper, register_source
    from det.sources.example_api.events import ExampleApiSource, example_api_v1_to_v2
    from det.sources.noaa.storm_events import NoaaStormEventsSource, storm_events_identity_mapper

    register_source("noaa.storm_events", NoaaStormEventsSource)
    register_source("example_api.events", ExampleApiSource)
    register_ingestion("dlt", DltBackend)
    register_ingestion("thin", ThinBackend)
    register_mapper("identity", identity_mapper)
    register_mapper("storm_events_identity", storm_events_identity_mapper)
    register_mapper("example_api_v1_to_v2", example_api_v1_to_v2)
    _LOADED = True
