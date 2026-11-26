from datetime import datetime, timezone

from bot_utils import now_utc, parse_timestamp


def test_now_utc_is_timezone_aware():
    assert now_utc().tzinfo == timezone.utc


def test_parse_timestamp_supports_legacy_naive_value():
    parsed = parse_timestamp("2025-01-02T03:04:05")
    assert parsed.tzinfo == timezone.utc
    assert parsed == datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


def test_parse_timestamp_preserves_explicit_offset():
    parsed = parse_timestamp("2025-01-02T03:04:05+08:00")
    assert parsed.utcoffset().total_seconds() == 8 * 3600
