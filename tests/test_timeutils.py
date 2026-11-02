"""Tests for gateway.core.timeutils.parse_timestamp."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from gateway.core.timeutils import parse_timestamp


def test_none_returns_none() -> None:
    assert parse_timestamp(None) is None


def test_z_suffix_is_parsed_as_utc() -> None:
    assert parse_timestamp("2026-01-02T03:04:05Z") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


def test_naive_string_is_promoted_to_utc() -> None:
    parsed = parse_timestamp("2026-01-02T03:04:05")
    assert parsed == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


def test_explicit_offset_is_preserved_not_shifted() -> None:
    parsed = parse_timestamp("2026-01-02T03:04:05-05:00")
    assert parsed is not None
    assert parsed.utcoffset() == timedelta(hours=-5)
    assert parsed == datetime(2026, 1, 2, 8, 4, 5, tzinfo=UTC)


def test_naive_datetime_is_promoted_to_utc() -> None:
    assert parse_timestamp(datetime(2026, 1, 2, 3, 4, 5)) == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


def test_aware_datetime_passes_through_unchanged() -> None:
    aware = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=2)))
    assert parse_timestamp(aware) is aware


@pytest.mark.parametrize("bad", ["", "not-a-date", "2026-13-45T00:00:00Z"])
def test_unparseable_string_returns_none(bad: str) -> None:
    assert parse_timestamp(bad) is None
