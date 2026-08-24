"""Public exception hierarchy for embedders.

Catch ``DetError`` for operational failures. Library code does not call
``configure_logging`` on import — configure at the process edge (CLI already
does) or BYO structlog. See ``docs/api.md``.
"""

from __future__ import annotations

from typing import Any


class DetError(Exception):
    """Base for DET operational failures."""


class DetConfigError(DetError):
    """Invalid settings, pipeline YAML, overrides, or secret store config."""


class DetPluginError(DetError):
    """Source / mapper / ingestion plugin failed (discovery or runtime)."""

    def __init__(self, message: str, *, plugin: str | None = None) -> None:
        super().__init__(message)
        self.plugin = plugin


class DetContractError(DetError):
    """Schema / coerce / wire contract failure."""


class DetConflictError(DetError):
    """Concurrent writer, existing raw extract, or other conflict."""


class DetNotFoundError(DetError):
    """Missing pipeline, raw partition, plugin id, or lake object."""


def plugin_error(exc: BaseException, *, plugin: str, action: str) -> DetPluginError:
    """Build ``DetPluginError`` for a plugin failure (use with ``raise … from exc``)."""
    return DetPluginError(
        f"{action} failed for plugin {plugin!r}: {exc}",
        plugin=plugin,
    )


def reraise_as_plugin(
    exc: BaseException,
    *,
    plugin: str,
    action: str,
) -> Any:
    """``raise`` helper: re-raise ``DetError`` unchanged; else wrap as plugin error."""
    if isinstance(exc, DetError):
        raise exc
    raise plugin_error(exc, plugin=plugin, action=action) from exc
