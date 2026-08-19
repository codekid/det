from __future__ import annotations

_LOADED = False


def load_plugins() -> None:
    """Register built-in ingestion backends and the identity mapper (idempotent).

    Source plugins and ``@mapper`` functions are discovered from
    ``det.sources.<provider>.<source>`` (and optional entry points) on demand.
    """
    global _LOADED
    if _LOADED:
        return

    from det.ingestion.det_backend import DetBackend
    from det.ingestion.thin_backend import ThinBackend
    from det.runtime.mappers import identity_mapper
    from det.runtime.registry import register_ingestion, register_mapper

    register_ingestion("det", DetBackend)
    register_ingestion("dlt", DetBackend)  # deprecated alias for library: det
    register_ingestion("thin", ThinBackend)
    register_mapper("identity", identity_mapper)
    _LOADED = True
