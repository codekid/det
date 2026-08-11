"""Deprecated module path — import ``DetBackend`` from ``det.ingestion.det_backend``."""

from __future__ import annotations

from det.ingestion.det_backend import DetBackend, DltBackend

__all__ = ["DetBackend", "DltBackend"]
