#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""How the scalar types StarRocks sends differently from MySQL are bound and read, without a server."""

from datetime import datetime

from sqlalchemy import testing
from sqlalchemy.testing import eq_, expect_raises_message, fixtures

from starrocks import datatype
from starrocks.dialect import StarRocksDialect


_DIALECT = StarRocksDialect()


class DatetimeTextReadTest(fixtures.TestBase):
    """DATETIME text, as the server writes it inside an ARRAY, MAP or STRUCT, is read as a ``datetime``."""

    @testing.combinations(
        ("whole_seconds", "2026-01-02 03:04:05", datetime(2026, 1, 2, 3, 4, 5)),
        ("six_digit_fraction", "2026-01-02 03:04:05.000007", datetime(2026, 1, 2, 3, 4, 5, 7)),
        ("one_digit_fraction", "2026-01-02 03:04:05.5", datetime(2026, 1, 2, 3, 4, 5, 500000)),
        ("first_day", "0001-01-01 00:00:00", datetime(1, 1, 1)),
        ("last_microsecond", "9999-12-31 23:59:59.999999", datetime(9999, 12, 31, 23, 59, 59, 999999)),
        argnames="text,expected",
        id_="iaa",
    )
    def test_text_is_read(self, text, expected):
        eq_(datatype.DATETIME()._cached_result_processor(_DIALECT, None)(text), expected)

    @testing.combinations(
        ("one_digit", "2026-01-02 03:04:05.5", datetime(2026, 1, 2, 3, 4, 5, 500000)),
        ("four_digits", "2026-01-02 03:04:05.1234", datetime(2026, 1, 2, 3, 4, 5, 123400)),
        argnames="text,expected",
        id_="iaa",
    )
    def test_a_short_fraction_is_scaled_without_fromisoformat(self, text, expected):
        """Python 3.10's ``fromisoformat`` reads only three or six fraction digits; the fallback reads the rest."""
        eq_(datatype._datetime_with_short_fraction(text), expected)

    @testing.combinations(
        ("not_a_datetime", "yesterday"),
        ("fraction_not_digits", "2026-01-02 03:04:05.x"),
        ("seven_digit_fraction", "2026-01-02 03:04:05.1234567"),
        argnames="text",
        id_="ia",
    )
    def test_text_that_is_not_a_datetime_raises(self, text):
        with expect_raises_message(ValueError, "as a DATETIME value"):
            datatype._datetime_with_short_fraction(text)

    @testing.combinations((None,), (datetime(2026, 1, 2),), argnames="value")
    def test_a_driver_value_is_returned_as_it_is(self, value):
        eq_(datatype.DATETIME()._cached_result_processor(_DIALECT, None)(value), value)


class LargeintTest(fixtures.TestBase):
    """LARGEINT reads as ``int``, though the server sends its values as text."""

    @testing.combinations(
        ("text", "-170141183460469231731687303715884105728", -(2**127)),
        ("bytes", b"170141183460469231731687303715884105727", 2**127 - 1),
        ("int", 5, 5),
        ("none", None, None),
        argnames="value,expected",
        id_="iaa",
    )
    def test_value_is_read_as_int(self, value, expected):
        read = datatype.LARGEINT()._cached_result_processor(_DIALECT, None)(value)
        eq_(read, expected)
        assert read is None or type(read) is int

    def test_display_width_is_accepted_and_dropped(self):
        """StarRocks shows a LARGEINT inside a collection as ``largeint(40)``."""
        eq_(datatype.LARGEINT(40).compile(dialect=_DIALECT), "LARGEINT")


class DoubleTest(fixtures.TestBase):
    """DOUBLE reads as ``float`` unless asked for Decimal; MySQL's DOUBLE rounds each value to ten places."""

    @testing.combinations(
        ("default", datatype.DOUBLE(), False),
        ("asdecimal", datatype.DOUBLE(asdecimal=True), True),
        ("adapted", datatype.DOUBLE().adapt(datatype.DOUBLE), False),
        argnames="type_,asdecimal",
        id_="iaa",
    )
    def test_asdecimal(self, type_, asdecimal):
        eq_(type_.asdecimal, asdecimal)

    def test_a_double_is_not_rounded(self):
        read = datatype.DOUBLE()._cached_result_processor(_DIALECT, None)
        value = 1.2345678901234567
        eq_(value if read is None else read(value), value)
