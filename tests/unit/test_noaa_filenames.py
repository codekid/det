from __future__ import annotations

from pathlib import Path

from bs4 import BeautifulSoup

from det.sources.noaa.storm_events import NoaaStormEventsSource


def _row(name: str, modified: str) -> str:
    return (
        f'<tr><td><a href="{name}">{name}</a></td>'
        f"<td>{modified}</td></tr>"
    )


INDEX = "<html><body><table>" + "".join(
    [
        "<tr><th>Name</th><th>Last modified</th></tr>",
        _row("StormEvents_details-ftp_v1.0_d2023_c20240101.csv.gz", "2024-01-01 00:00"),
        _row("StormEvents_details-ftp_v1.0_d2024_c20250101.csv.gz", "2025-01-01 00:00"),
        _row("StormEvents_details-ftp_v1.0_d2024_c20260728.csv.gz", "2026-07-27 21:11"),
        _row("StormEvents_details-ftp_v1.0_d2025_c20260728.csv.gz", "2026-07-27 21:11"),
    ]
) + "</table></body></html>"


def test_filenames_selected_by_data_year_not_mtime():
    soup = BeautifulSoup(INDEX, "html.parser")
    names = NoaaStormEventsSource()._filenames_in_interval(
        soup,
        interval_start="2024-01-01",
        interval_end="2024-01-02",
        substr="details-ftp",
    )
    # Latest _c* republish for 2024, even though mtime is outside the window.
    assert names == ["StormEvents_details-ftp_v1.0_d2024_c20260728.csv.gz"]


def test_filenames_span_years_in_window():
    soup = BeautifulSoup(INDEX, "html.parser")
    names = NoaaStormEventsSource()._filenames_in_interval(
        soup,
        interval_start="2024-01-01",
        interval_end="2026-01-01",
        substr="details-ftp",
    )
    assert names == [
        "StormEvents_details-ftp_v1.0_d2024_c20260728.csv.gz",
        "StormEvents_details-ftp_v1.0_d2025_c20260728.csv.gz",
    ]


def test_get_soup_uses_http_get(monkeypatch):
    class Resp:
        status_code = 200
        content = INDEX.encode()
        text = INDEX

    monkeypatch.setattr(
        "det.sources.noaa.storm_events.http_get", lambda *args, **kwargs: Resp()
    )
    soup = NoaaStormEventsSource()._get_soup("https://ncei.example/")
    assert soup.find("table") is not None


def test_download_uses_http_get_file(monkeypatch, tmp_path: Path):
    import gzip

    captured: dict[str, object] = {}

    def fake_file(url, dest, **kwargs):
        captured["url"] = url
        dest.write_bytes(gzip.compress(b"EVENT_ID\n1\n"))
        return dest.stat().st_size

    monkeypatch.setattr("det.sources.noaa.storm_events.http_get_file", fake_file)
    art = NoaaStormEventsSource()._download(
        "https://ncei.example/",
        "StormEvents_details-ftp.csv.gz",
        data_dir=tmp_path,
    )
    assert captured["url"] == "https://ncei.example/StormEvents_details-ftp.csv.gz"
    assert art["content_encoding"] == "gzip"
    assert art["format_check"] == "ok"
