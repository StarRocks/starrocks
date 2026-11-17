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
How a query result cell becomes the text in an R file.

Its own module, and standard library only, so the unit test beside it needs neither a
database driver nor the rest of the framework to run.
"""

import datetime


def format_cell(value):
    """
    Render one cell of a query result the way the server wrote it.

    Everything the driver hands back stringifies to the server's own text, with one
    exception. There is no Python type for SQL TIME: datetime.time is a time of day, so it
    cannot be negative and stops at 24 hours, while a TIME is a signed span with no bound
    the type itself enforces -- cast('900:00:00' as time) answers 900:00:00, and only
    individual functions clamp, sec_to_time at MAX_TIME = 3023999 seconds
    (time_functions.cpp), which is 839:59:59 and not MySQL's 838:59:59.

    So the driver returns a timedelta, which holds the value correctly and then prints
    itself as a duration: the server's -00:00:01 comes back as "-1 day, 23:59:59", because
    a timedelta stores a negative span as a negative day plus a positive remainder, and
    00:01:00 comes back as "0:01:00" because it does not pad the hour. The hour field here
    is therefore written without an upper width, not padded to three.

    Recording that is recording how Python happens to hold the answer rather than what the
    answer is, and it would fail a case the day the driver changes its mind about
    formatting, with nothing about StarRocks having moved.

    TIME cannot be a column type here -- "Type:TIME of column:v does not support" -- so it
    arrives only from an expression, or from an external catalog whose own type system
    allows the column.
    """
    if isinstance(value, datetime.timedelta):
        # A timedelta carries its sign in `days` alone; `seconds` and `microseconds` are
        # non-negative remainders. -1 microsecond is days=-1, seconds=86399,
        # microseconds=999999. So the signed total has to be summed before it is split, or
        # the remainder is subtracted twice and -00:00:00.000001 comes out as
        # -00:00:01.999999.
        total_us = (value.days * 86400 + value.seconds) * 1000000 + value.microseconds
        sign = "-" if total_us < 0 else ""
        seconds, microseconds = divmod(abs(total_us), 1000000)
        text = "%s%02d:%02d:%02d" % (sign, seconds // 3600, seconds % 3600 // 60, seconds % 60)
        return text + (".%06d" % microseconds if microseconds else "")
    return str(value)
