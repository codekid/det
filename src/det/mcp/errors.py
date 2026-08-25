"""Utilities for sanitizing exception messages before they reach the agent.

Raw exception strings can expose absolute filesystem paths, connection strings,
or large SQL fragments.  Use `sanitize_detail` wherever an exception is
converted to a string in an MCP tool response.
"""

from __future__ import annotations

import re

# Match absolute POSIX paths with at least two components, e.g.
# /Users/alice/dev/project/data/file.db or /home/runner/work/...
_ABS_PATH = re.compile(r"(?<!\w)((?:/[A-Za-z0-9_.~@-]+){2,}/?)")

# DuckDB error messages often include the full user SQL after "in SQL: ...".
# Truncate the message before that fragment reaches the agent.
_SQL_TRAILER = re.compile(r"\s+(?:in SQL|LINE \d+).*", re.DOTALL | re.IGNORECASE)

_MAX_DETAIL_LEN = 400


def sanitize_detail(exc: Exception) -> str:
    """Return a sanitized one-line summary of *exc* safe to include in an MCP response.

    - Absolute filesystem paths are replaced with ``<path>``.
    - SQL trailers (``in SQL: ...``) that embed the original query are stripped.
    - The result is capped at ``_MAX_DETAIL_LEN`` characters.
    """
    msg = str(exc)
    # Strip SQL trailers first (they may contain paths themselves).
    msg = _SQL_TRAILER.sub("", msg)
    # Replace absolute paths.
    msg = _ABS_PATH.sub("<path>", msg)
    msg = msg.strip()
    if len(msg) > _MAX_DETAIL_LEN:
        msg = msg[: _MAX_DETAIL_LEN - 3] + "..."
    return msg
