# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""TIME and VARIANT columns reflect to real types rather than NullType.

Neither was in ``ischema_names``, so reflecting either column raised inside
``parse_data_type``; ``_parse_column_type`` swallowed that and returned
``NullType``, whose ``compile()`` raises ``CompileError``. Any caller that
compiles a reflected column — comparing a table against its declared shape,
for instance — therefore failed on a table that had itself created happily.
"""

from datetime import timedelta

import pytest
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.sql import sqltypes

from starrocks.datatype import TIME, VARIANT
from starrocks.dialect import StarRocksDialect
from starrocks.drivers.parsers import parse_data_type


@pytest.fixture(name="dialect")
def dialect_fixture():
    return StarRocksDialect()


@pytest.mark.parametrize(
    "type_string, expected_type, expected_ddl",
    [
        # information_schema.columns reports TIME upper case, unlike every
        # other type; the parser lowercases before the lookup.
        ("TIME", TIME, "TIME"),
        ("time", TIME, "TIME"),
        ("variant", VARIANT, "VARIANT"),
        ("VARIANT", VARIANT, "VARIANT"),
    ],
)
def test_reflected_type_compiles_back(type_string, expected_type, expected_ddl, dialect):
    parsed = parse_data_type(type_string)
    instance = parsed() if isinstance(parsed, type) else parsed

    assert isinstance(instance, expected_type)
    assert instance.compile(dialect) == expected_ddl


@pytest.mark.parametrize("type_string", ["TIME", "variant"])
def test_reflected_type_is_not_nulltype(type_string):
    """NullType is what made this fail: it cannot generate DDL."""
    parsed = parse_data_type(type_string)
    instance = parsed() if isinstance(parsed, type) else parsed

    assert not isinstance(instance, sqltypes.NullType)


@pytest.mark.parametrize(
    "wire_value, expected",
    [
        ("12:34:56", timedelta(hours=12, minutes=34, seconds=56)),
        # timediff('1000-01-02 01:01:01', '1000-01-01 01:01:01'). A time-of-day
        # processor drops timedelta.days here and reports 00:00:00.
        ("24:00:00", timedelta(hours=24)),
        ("-24:00:00", timedelta(hours=-24)),
        # The sign is what gets lost on this one -- it reads back as 23:59:59.
        ("-00:00:01", timedelta(seconds=-1)),
        # Past 999 hours PyMySQL stops parsing and hands the string through, so
        # anything expecting a timedelta from the driver raises AttributeError.
        ("78883632:00:00", timedelta(hours=78883632)),
        ("-78883632:00:01", -timedelta(hours=78883632, seconds=1)),
        ("00:00:00.123456", timedelta(microseconds=123456)),
        ("-00:00:00.5", timedelta(microseconds=-500000)),
    ],
)
def test_time_preserves_signed_and_multi_day_durations(wire_value, expected, dialect):
    """StarRocks TIME is a signed duration, not a time of day.

    It is held as a signed number of seconds and rendered as ``[-]HH:MM:SS``
    with no day part, so ``timediff`` legitimately produces ``24:00:00`` and
    ``-78883632:00:01``. Reflecting such a column as a time-of-day type
    silently truncates every one of these.
    """
    result = TIME().result_processor(dialect, None)

    assert result(wire_value) == expected


def test_time_accepts_the_drivers_timedelta_unchanged(dialect):
    """PyMySQL decodes TIME within its range for us; keep the full duration."""
    result = TIME().result_processor(dialect, None)

    assert result(timedelta(days=1)) == timedelta(hours=24)


def test_time_decodes_bytes_from_the_driver(dialect):
    result = TIME().result_processor(dialect, None)

    assert result(b"24:00:00") == timedelta(hours=24)


def test_time_passes_null_through(dialect):
    assert TIME().result_processor(dialect, None)(None) is None


def test_time_rejects_an_unparseable_value(dialect):
    result = TIME().result_processor(dialect, None)

    with pytest.raises(ValueError):
        result("not a time")


def test_generic_time_column_uses_the_starrocks_type(dialect):
    """A declared sqlalchemy.Time must not fall back to the MySQL processor."""
    column = Column("c", sqltypes.Time())

    assert isinstance(column.type.dialect_impl(dialect), TIME)


def test_variant_insert_parses_json_server_side(dialect):
    """The bound value must reach the column through PARSE_JSON.

    Without this, the JSON text arrives as a VARCHAR bind and StarRocks stores
    the whole document as a VARIANT *string*: indexing into it yields NULL and
    reading it back returns text instead of the original structure. Note this
    is why VARIANT does not subclass sqltypes.JSON, which suppresses
    bind_expression on INSERT -- so asserting on the compiled statement is what
    catches a regression here; processors alone would still look correct.
    """
    table = Table("t", MetaData(), Column("j", VARIANT()))
    compiled = str(table.insert().values(j={"a": 1}).compile(dialect=dialect))

    assert "parse_json(" in compiled.lower()


def test_variant_round_trips_json_values(dialect):
    value = {"a": 1, "b": "x", "nested": [1, 2]}
    bind = VARIANT().bind_processor(dialect)
    result = VARIANT().result_processor(dialect, None)

    assert result(bind(value)) == value


def test_variant_decodes_bytes_from_the_driver(dialect):
    """pymysql hands the column back as bytes."""
    result = VARIANT().result_processor(dialect, None)

    assert result(b'{"a": 1}') == {"a": 1}


def test_variant_passes_null_through(dialect):
    assert VARIANT().bind_processor(dialect)(None) is None
    assert VARIANT().result_processor(dialect, None)(None) is None
