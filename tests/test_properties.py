"""Properties of the sync's pure functions, checked on generated inputs with hypothesis.

The example-based tests pin the cases someone thought of; these state the invariant and let hypothesis hunt for a case
that breaks it. A failure prints the smallest falsifying input; add it to the matching example-based test as a
regression. hypothesis keeps its example database in .hypothesis/ (git-ignored).

Run from the repo root:  .venv/bin/python -m pytest tests/test_properties.py
"""

import datetime
import re

from hypothesis import given, settings
from hypothesis import strategies as st

from orchestration.assets import reverse_etl_cdf_setup as cdf
from orchestration.assets import reverse_etl_delta_reader as reader

PROPERTY = settings(deadline=None, max_examples=200)


# --- chunking ----------------------------------------------------------------------------------------------------------


@PROPERTY
@given(start=st.integers(0, 500), end=st.integers(-5, 600), size=st.integers(1, 60))
def test_version_chunks_cover_the_range_exactly_in_order(start, end, size):
    chunks = cdf._version_chunks(start, end, size)
    if end < start:
        assert chunks == []
        return
    assert chunks[0][0] == start and chunks[-1][1] == end
    assert all(nxt[0] == prev[1] + 1 for prev, nxt in zip(chunks, chunks[1:]))  # contiguous, no overlap, no gap
    assert all(1 <= last - first + 1 <= size for first, last in chunks)
    assert all(last - first + 1 == size for first, last in chunks[:-1])  # only the final chunk may be short


@st.composite
def timelines(draw):
    """A range of versions, a chunk size, and the versions where a new schema begins (a schema is never reused, so two
    versions share one exactly when no change lies between them)."""
    start = draw(st.integers(0, 60))
    end = draw(st.integers(start, start + 80))
    size = draw(st.integers(1, 30))
    changes = draw(st.sets(st.integers(start + 1, end), max_size=8)) if end > start else set()
    return start, end, size, changes


@PROPERTY
@given(timelines())
def test_splitting_at_schema_changes_leaves_one_schema_per_chunk_and_covers_the_same_versions(timeline):
    start, end, size, changes = timeline

    def schema_at(version):
        return sum(1 for change in changes if change <= version)

    original = cdf._version_chunks(start, end, size)
    result = cdf._split_at_schema_changes(original, schema_at)

    assert result[0][0] == start and result[-1][1] == end
    assert all(nxt[0] == prev[1] + 1 for prev, nxt in zip(result, result[1:]))
    for first, last in result:
        assert len({schema_at(v) for v in range(first, last + 1)}) == 1  # no change inside a chunk
        assert any(a <= first and last <= b for a, b in original)  # never straddles an original boundary


@PROPERTY
@given(timelines())
def test_a_chunk_without_a_schema_change_is_never_split(timeline):
    start, end, size, changes = timeline
    original = cdf._version_chunks(start, end, size)
    result = cdf._split_at_schema_changes(original, lambda version: sum(1 for change in changes if change <= version))
    for first, last in original:
        if not any(first < change <= last for change in changes):
            assert (first, last) in result


# --- watermark ---------------------------------------------------------------------------------------------------------

version_values = st.one_of(st.integers(0, 10**6), st.integers(0, 10**6).map(str))


@PROPERTY
@given(
    versions=st.lists(st.one_of(version_values, st.none()), max_size=12),
    current=st.one_of(st.none(), st.integers(0, 10**6)),
)
def test_the_watermark_is_the_numerically_highest_version_seen_or_the_current_one(versions, current):
    rows = [{cdf.COMMIT_VERSION_COLUMN: v} if v is not None else {} for v in versions]
    seen = [int(v) for v in versions if v is not None]
    assert cdf._next_watermark(rows, current) == (max(seen) if seen else current)


# --- value coercion ----------------------------------------------------------------------------------------------------

CONNECT_TYPES = ["int8", "int16", "int32", "int64", "float", "double", "boolean", "string", cdf.DATE]


@PROPERTY
@given(st.sampled_from(CONNECT_TYPES))
def test_a_null_stays_null_for_every_type(connect_type):
    assert cdf._coerce(None, connect_type) is None


@PROPERTY
@given(n=st.integers(-(2**63), 2**63 - 1), connect_type=st.sampled_from(["int8", "int16", "int32", "int64"]))
def test_integers_arrive_from_the_api_as_strings_and_come_out_as_ints(n, connect_type):
    assert cdf._coerce(str(n), connect_type) == n and cdf._coerce(n, connect_type) == n


@PROPERTY
@given(x=st.floats(allow_nan=False, allow_infinity=False), connect_type=st.sampled_from(["float", "double"]))
def test_floats_survive_the_trip_through_a_string(x, connect_type):
    assert cdf._coerce(repr(x), connect_type) == x


@PROPERTY
@given(flag=st.booleans(), cased=st.sampled_from([str.lower, str.upper, str.title]))
def test_booleans_survive_as_bools_or_any_casing_of_their_text(flag, cased):
    assert cdf._coerce(flag, "boolean") is flag
    assert cdf._coerce(cased(str(flag)), "boolean") is flag


@PROPERTY
@given(day=st.dates(), suffix=st.sampled_from(["", "T10:00:00Z", " 23:59:59", "T00:00:00.123456+02:00"]))
def test_a_date_becomes_its_days_since_the_epoch_whatever_follows_the_date_in_the_text(day, suffix):
    assert cdf._coerce(day.isoformat() + suffix, cdf.DATE) == (day - datetime.date(1970, 1, 1)).days


@PROPERTY
@given(text=st.text())
def test_strings_are_left_alone(text):
    assert cdf._coerce(text, "string") == text


# --- change events -----------------------------------------------------------------------------------------------------

CHANGE_TYPES = ["insert", "delete", "update_preimage", "update_postimage"]
change_rows = st.lists(
    st.fixed_dictionaries(
        {
            "id": st.integers(1, 3),
            cdf.COMMIT_VERSION_COLUMN: st.integers(1, 3),
            cdf.CHANGE_TYPE_COLUMN: st.sampled_from(CHANGE_TYPES),
            cdf.COMMIT_TIMESTAMP_COLUMN: st.just("2026-10-18T17:26:36.779212Z"),
            "payload": st.integers(0, 9),
        }
    ),
    max_size=30,
)


@PROPERTY
@given(change_rows)
def test_events_account_for_every_change_row_and_never_carry_the_cdf_columns(rows):
    events = cdf._to_debezium_events(rows, ["id"])

    groups: dict = {}
    for row in rows:
        counts = groups.setdefault((row["id"], row[cdf.COMMIT_VERSION_COLUMN]), dict.fromkeys(CHANGE_TYPES, 0))
        counts[row[cdf.CHANGE_TYPE_COLUMN]] += 1
    expected = {
        "c": sum(c["insert"] for c in groups.values()),
        "d": sum(c["delete"] for c in groups.values()),
        "u": sum(max(c["update_preimage"], c["update_postimage"]) for c in groups.values()),  # paired positionally
    }
    assert {op: n for op, n in cdf._summarize_ops(events).items()} == {op: n for op, n in expected.items() if n}

    for event in events:
        for side in ("before", "after"):
            assert not (event[side] or {}).keys() & set(cdf.CDF_METADATA_COLUMNS)
        if event["op"] == "c":
            assert event["before"] is None and event["after"] is not None
        if event["op"] == "d":
            assert event["after"] is None and event["before"] is not None
        assert event["source"]["commit_version"] in {1, 2, 3}


# --- timestamps --------------------------------------------------------------------------------------------------------

TIMESTAMP = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z")
FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


@PROPERTY
@given(
    value=st.datetimes(
        min_value=datetime.datetime(1971, 1, 1),
        max_value=datetime.datetime(2100, 1, 1),
        timezones=st.one_of(st.none(), st.timezones()),
    )
)
def test_a_timestamp_is_rendered_as_the_same_utc_instant_in_microseconds(value):
    text = reader._format_timestamp(value)
    assert TIMESTAMP.fullmatch(text)
    parsed = datetime.datetime.strptime(text[:-1], FORMAT)
    expected = value.astimezone(datetime.timezone.utc).replace(tzinfo=None) if value.tzinfo else value
    assert parsed == expected


@PROPERTY
@given(milliseconds=st.integers(0, 4_000_000_000_000))
def test_a_commit_timestamp_in_milliseconds_is_rendered_exactly(milliseconds):
    text = reader._format_commit_timestamp(milliseconds)
    assert TIMESTAMP.fullmatch(text)
    parsed = datetime.datetime.strptime(text[:-1], FORMAT)
    assert parsed == datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=milliseconds)


# --- row comparison ----------------------------------------------------------------------------------------------------

rows_of_values = st.dictionaries(st.sampled_from(list("abcdef")), st.one_of(st.none(), st.integers(0, 3)), max_size=6)


@PROPERTY
@given(old=rows_of_values, new=rows_of_values, skip=st.sets(st.sampled_from(list("abcdef"))))
def test_row_comparison_is_symmetric_and_reads_a_missing_column_as_null(old, new, skip):
    assert reader._changed_payload_columns(old, new, skip) == reader._changed_payload_columns(new, old, skip)
    padded_old = {name: old.get(name) for name in set(old) | set(new)}
    padded_new = {name: new.get(name) for name in set(old) | set(new)}
    changed = any(padded_old[name] != padded_new[name] for name in padded_old if name not in skip)
    assert reader._changed_payload_columns(old, new, skip) == changed
