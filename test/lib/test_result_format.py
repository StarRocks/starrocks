# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Tests for format_cell.

The recorded cases reach only positive whole seconds, so everything else here -- the two
signs, sub-second values, an hour past a day -- has no cover from them. The negative
fractional case is the one a previous revision got wrong, by splitting the value before
summing its sign, so it is pinned twice: at the boundary and away from it.

Imported by path rather than as lib.result_format, because importing the package would
pull in lib/__init__.py and with it cup and timeout_decorator. Nothing here needs a driver
or a cluster.
"""

import datetime
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from result_format import format_cell

TD = datetime.timedelta


class TestFormatCell(unittest.TestCase):
    def assert_renders(self, value, expected):
        self.assertEqual(expected, format_cell(value), repr(value))

    def test_zero(self):
        self.assert_renders(TD(0), "00:00:00")

    def test_whole_seconds(self):
        # The hour is padded, which str(timedelta) does not do: it gives "0:01:00".
        self.assert_renders(TD(seconds=1), "00:00:01")
        self.assert_renders(TD(seconds=60), "00:01:00")
        self.assert_renders(TD(seconds=3723), "01:02:03")

    def test_negative_whole_seconds(self):
        # A timedelta holds this as days=-1, seconds=86399, and prints it that way:
        # "-1 day, 23:59:59".
        self.assert_renders(TD(seconds=-1), "-00:00:01")
        self.assert_renders(TD(seconds=-60), "-00:01:00")
        self.assert_renders(TD(seconds=-3723), "-01:02:03")

    def test_sub_second(self):
        self.assert_renders(TD(microseconds=1), "00:00:00.000001")
        self.assert_renders(TD(seconds=1, microseconds=500000), "00:00:01.500000")

    def test_negative_sub_second(self):
        # The sign lives in `days` alone and the other two fields are non-negative
        # remainders, so -1 microsecond is days=-1, seconds=86399, microseconds=999999.
        # Splitting before summing renders this as -00:00:01.999999.
        self.assert_renders(TD(microseconds=-1), "-00:00:00.000001")
        self.assert_renders(TD(seconds=-1, microseconds=-500000), "-00:00:01.500000")
        self.assert_renders(TD(seconds=-3723, microseconds=-1), "-01:02:03.000001")

    def test_hours_past_a_day(self):
        # sec_to_time clamps at MAX_TIME = 3023999 seconds; a cast is not clamped and can
        # answer with more hours than that, so the hour field has no upper width.
        self.assert_renders(TD(seconds=86400), "24:00:00")
        self.assert_renders(TD(seconds=3023999), "839:59:59")
        self.assert_renders(TD(seconds=-3023999), "-839:59:59")
        self.assert_renders(TD(seconds=3240000), "900:00:00")

    def test_everything_else_is_left_to_str(self):
        for value in [0, 1, -1, 1.5, "abc", "", None, True, [1, 2], {"a": 1},
                      datetime.date(2020, 1, 2),
                      datetime.datetime(2020, 1, 2, 3, 4, 5),
                      datetime.datetime(2020, 1, 2, 3, 4, 5, 123456)]:
            self.assertEqual(str(value), format_cell(value), repr(value))


if __name__ == "__main__":
    unittest.main()
