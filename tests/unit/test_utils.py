from __future__ import annotations

import pytest

from sandboxpilot.errors import ValidationError
from sandboxpilot.utils.ids import new_id, short_id, uuid7
from sandboxpilot.utils.sizes import (
    cpus_to_millis,
    format_bytes,
    parse_bytes,
    parse_duration,
)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1GB", 1000**3),
        ("1GiB", 1024**3),
        ("512MB", 512 * 1000**2),
        ("512Mi", 512 * 1024**2),
        (1024, 1024),
        ("100", 100),
    ],
)
def test_parse_bytes(text: str | int, expected: int) -> None:
    assert parse_bytes(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"), [("30s", 30), ("5m", 300), ("2h", 7200), ("1h30m", 5400), (45, 45)]
)
def test_parse_duration(text: str | int, expected: float) -> None:
    assert parse_duration(text) == expected


def test_parse_errors() -> None:
    with pytest.raises(ValidationError):
        parse_bytes("lots")
    with pytest.raises(ValidationError):
        parse_duration("soon")


def test_cpus_to_millis() -> None:
    assert cpus_to_millis(0.5) == 500
    assert cpus_to_millis("2") == 2000
    with pytest.raises(ValidationError):
        cpus_to_millis(0)


def test_format_bytes_round_trip() -> None:
    assert format_bytes(2 * 1024**3).startswith("2")


def test_ids_are_time_ordered_and_prefixed() -> None:
    a, b = uuid7(), uuid7()
    assert a.version == 7
    assert str(a) < str(b)
    sid = new_id("sbx")
    assert sid.startswith("sbx_")
    assert len(short_id(sid)) == 8


def test_short_ids_differ_for_ids_minted_in_the_same_millisecond() -> None:
    # The UUIDv7 prefix is a timestamp; short ids must come from the random tail.
    ids = [new_id("sbx") for _ in range(50)]
    assert len({short_id(i) for i in ids}) == 50
    assert len({short_id(i, 6) for i in ids}) == 50
    assert all(i.endswith(short_id(i)) for i in ids)


def test_container_names_differ_for_ids_minted_in_the_same_millisecond() -> None:
    # Same-millisecond ids differ only in their last bits: names must use the whole id.
    from sandboxpilot.worker.runtime.docker_gvisor import container_name

    names = {container_name(new_id("sbx")) for _ in range(200)}
    assert len(names) == 200
    assert all(n.startswith("sp-sbx-") and len(n) == len("sp-sbx-") + 32 for n in names)
