from __future__ import annotations

import csv
import gzip
import re
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import IO, Any

import pendulum

from det.logging import get_logger
from det.optional_deps import require_beautifulsoup
from det.runtime.lake import LakeRef
from det.runtime.manifest import sha256_file
from det.sources.base import Interval, SourceRow
from det.sources.http import http_get, http_get_file

logger = get_logger(__name__)

DEFAULT_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
DEFAULT_FILENAME_SUBSTR = "details-ftp"
HTTP_TIMEOUT = (15, 120)
HTTP_HEADERS = {
    "User-Agent": "det/0.1 (+https://github.com/local/det; storm-events extract)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
_DATA_YEAR = re.compile(r"_d(\d{4})_")


def _is_repeated_header_row(row: dict[str, Any], fieldnames: list[str] | None) -> bool:
    if not fieldnames:
        return False
    keys = [f for f in fieldnames if f]
    if not keys:
        return False
    for f in keys:
        val = row.get(f)
        if val is None or str(val).strip() != str(f).strip():
            return False
    return True


def _open_text(path: Path | LakeRef, *, content_encoding: str) -> IO[str]:
    if content_encoding == "gzip":
        return gzip.open(
            path.open("rb"),
            mode="rt",
            encoding="utf-8",
            errors="replace",
            newline="",
        )
    return path.open(encoding="utf-8", errors="replace", newline="")


def _format_check_csv(path: Path | LakeRef, *, content_encoding: str) -> None:
    logger.info("Format-checking CSV", path=path.name, content_encoding=content_encoding)
    with _open_text(path, content_encoding=content_encoding) as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"CSV format check failed: no header in {path.name}")
        # Ensure the stream is readable past the header.
        next(reader, None)
    logger.info("CSV format check ok", path=path.name)


def _artifact(
    *,
    data_dir: Path | LakeRef,
    dest: Path | LakeRef,
    origin: str,
    content_encoding: str,
) -> dict[str, Any]:
    _format_check_csv(dest, content_encoding=content_encoding)
    logger.info("Hashing data artifact", path=dest.name)
    digest = sha256_file(dest)
    # Paths are relative to the raw partition root (sibling of data/).
    rel = dest.relative_to(data_dir.parent).as_posix()
    return {
        "path": rel,
        "origin": origin,
        "sha256": digest,
        "bytes": dest.stat().st_size,
        "format": "csv",
        "content_encoding": content_encoding,
        "format_check": "ok",
    }


class NoaaStormEventsSource:
    """
    NOAA Storm Events details CSVs from the NCEI public index.

    Interval mode: ``year_files`` — select ``*_dYYYY_*`` archives whose year
    overlaps the extract window. Lands gzip CSV bytes wire-faithful.
    """

    name = "noaa.storm_events"

    def defaults(self) -> dict[str, Any]:
        return {
            "url": DEFAULT_URL,
            # Public bulk files: declared so no credential is looked up or required.
            "auth_env": None,
            "filename_substr": DEFAULT_FILENAME_SUBSTR,
            "filenames": None,
            "local_csv_dir": None,
        }

    def extract_to_raw(
        self,
        *,
        config: dict[str, Any],
        interval: Interval,
        data_dir: Path | LakeRef,
    ) -> list[dict[str, Any]]:
        data_dir.mkdir(parents=True, exist_ok=True)
        local_dir = config.get("local_csv_dir")
        if local_dir:
            logger.info("NOAA extract using local CSV dir", local_csv_dir=local_dir)
            return self._extract_local(Path(local_dir), config=config, data_dir=data_dir)

        filenames = config.get("filenames")
        if filenames:
            names = list(filenames)
            logger.info("NOAA extract using explicit filenames", files=len(names))
        else:
            logger.info(
                "NOAA extract: resolving yearly files for interval",
                interval_start=interval.start,
                interval_end=interval.end,
                note="a day window still downloads the full year file",
            )
            soup = self._get_soup(config["url"])
            names = self._filenames_in_interval(
                soup,
                interval_start=interval.start,
                interval_end=interval.end,
                substr=config.get("filename_substr", DEFAULT_FILENAME_SUBSTR),
            )
        if not names:
            logger.warning("No NOAA files matched interval", interval_start=interval.start)
        else:
            logger.info("NOAA files selected", files=names)
        artifacts: list[dict[str, Any]] = []
        for i, name in enumerate(names, start=1):
            logger.info("NOAA download starting", file=name, index=i, of=len(names))
            artifacts.append(self._download(config["url"], name, data_dir=data_dir))
        return artifacts

    def records_from_raw(
        self,
        *,
        config: dict[str, Any],
        raw_dir: Path | LakeRef,
        manifest: dict[str, Any],
    ) -> Iterator[SourceRow]:
        del config  # NOAA data layout is fully described by the manifest
        artifacts = list(manifest.get("artifacts") or [])
        logger.info("Parsing NOAA data artifacts", artifacts=len(artifacts), raw_dir=str(raw_dir))
        for art in artifacts:
            path = raw_dir / art["path"]
            encoding = art.get("content_encoding") or "identity"
            yield from self._iter_csv(
                path,
                content_encoding=encoding,
                filename=Path(art["path"]).name,
            )

    def _extract_local(
        self, directory: Path, *, config: dict[str, Any], data_dir: Path | LakeRef
    ) -> list[dict[str, Any]]:
        substr = config.get("filename_substr", DEFAULT_FILENAME_SUBSTR)
        artifacts: list[dict[str, Any]] = []
        for src in sorted(directory.glob("*.csv")):
            if substr and substr not in src.name:
                continue
            dest = data_dir / src.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            with src.open("rb") as inf, dest.open("wb") as out:
                shutil.copyfileobj(inf, out)
            artifacts.append(
                _artifact(
                    data_dir=data_dir,
                    dest=dest,
                    origin=str(src.resolve()),
                    content_encoding="identity",
                )
            )
        if not artifacts:
            raise ValueError(f"No local CSV matched substr={substr!r} in {directory}")
        return artifacts

    def _download(
        self, base_url: str, gz_filename: str, *, data_dir: Path | LakeRef
    ) -> dict[str, Any]:
        full_url = base_url + gz_filename
        dest = data_dir / gz_filename
        logger.info("Downloading NOAA file", url=full_url)
        encoding = "gzip" if gz_filename.endswith(".gz") else "identity"
        downloaded = http_get_file(
            full_url,
            dest,
            timeout=(15, 600),
            headers=HTTP_HEADERS,
            encoding=encoding if encoding == "gzip" else None,
        )
        logger.info("NOAA download finished", file=gz_filename, bytes=downloaded)
        return _artifact(
            data_dir=data_dir,
            dest=dest,
            origin=full_url,
            content_encoding=encoding,
        )

    def _iter_csv(
        self, path: Path | LakeRef, *, content_encoding: str, filename: str
    ) -> Iterator[SourceRow]:
        logger.info("Reading CSV rows", path=path.name, content_encoding=content_encoding)
        emitted = 0
        with _open_text(path, content_encoding=content_encoding) as f:
            reader = csv.DictReader(f)
            fields = reader.fieldnames
            for row in reader:
                if _is_repeated_header_row(row, fields):
                    continue
                data = {
                    str(k).replace('"', ""): v
                    for k, v in row.items()
                    if k is not None
                }
                emitted += 1
                if emitted == 1 or emitted % 50_000 == 0:
                    logger.info("CSV parse progress", path=path.name, rows=emitted)
                yield SourceRow(data=data, filename=filename)
        logger.info("CSV parse finished", path=path.name, rows=emitted)

    def _get_soup(self, page_url: str) -> Any:
        BeautifulSoup = require_beautifulsoup()
        logger.info(
            "Fetching storm events index (HTTP GET; may take a bit)",
            url=page_url,
            timeout_connect=HTTP_TIMEOUT[0],
            timeout_read=HTTP_TIMEOUT[1],
        )
        response = http_get(page_url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT)
        logger.info(
            "Storm events index HTTP response",
            status=response.status_code,
            bytes=len(response.content),
        )
        return BeautifulSoup(response.text, "html.parser")

    def _filenames_in_interval(
        self,
        soup: Any,
        *,
        interval_start: str,
        interval_end: str | None,
        substr: str,
    ) -> list[str]:
        start = pendulum.parse(interval_start).in_timezone("UTC")
        end = (
            pendulum.parse(interval_end).in_timezone("UTC")
            if interval_end
            else start.add(days=1)
        )
        assert start is not None and end is not None
        years = set(range(start.year, end.subtract(microseconds=1).year + 1))
        logger.info("Selecting NOAA files for years", years=sorted(years), substr=substr)
        best: dict[int, str] = {}
        table = soup.find("table")
        if table is None:
            logger.warning("No table in NOAA index HTML")
            return []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if not tds:
                continue
            a = tds[0].find("a", href=True)
            if not a:
                continue
            name = a.get_text(strip=True)
            if not name.endswith(".csv.gz") or substr not in name:
                continue
            match = _DATA_YEAR.search(name)
            if not match:
                continue
            year = int(match.group(1))
            if year not in years:
                continue
            prev = best.get(year)
            if prev is None or name > prev:
                best[year] = name
        selected = [best[y] for y in sorted(best)]
        logger.info("NOAA year→file map", mapping=best)
        return selected


