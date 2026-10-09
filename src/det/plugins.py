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

    # Keep backends off the base ``import det`` path (iceberg/duckdb writers).
    from det.ingestion.det_backend import DetBackend  # noqa: PLC0415
    from det.ingestion.thin_backend import ThinBackend  # noqa: PLC0415
    from det.runtime.mappers import identity_mapper  # noqa: PLC0415
    from det.runtime.registry import register_ingestion, register_mapper  # noqa: PLC0415

    register_ingestion("det", DetBackend)
    register_ingestion("thin", ThinBackend)
    register_mapper("identity", identity_mapper)
    _LOADED = True
