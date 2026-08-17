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

"""Unit tests for materialized view REFRESH clause extraction during reflection."""

import pytest

from starrocks.common.params import TableInfoKeyWithPrefix
from starrocks.reflection import StarRocksTableDefinitionParser


def _reflect_refresh(refresh_clause: str) -> str:
    """Parse a synthetic SHOW CREATE MATERIALIZED VIEW DDL and return the reflected
    refresh string (ReflectedRefreshInfo rendered via str())."""
    parser = StarRocksTableDefinitionParser.__new__(StarRocksTableDefinitionParser)
    ddl = (
        "CREATE MATERIALIZED VIEW `m` (`val`)\n"
        "DISTRIBUTED BY RANDOM\n"
        f"{refresh_clause}\n"
        'PROPERTIES ("replication_num" = "1")\n'
        "AS SELECT val FROM t"
    )
    state = parser._parse_mv_ddl("m", ddl, schema="db")
    return str(state.table_options.get(TableInfoKeyWithPrefix.REFRESH))


def _reflect_partition_by(partition_clause: str) -> str:
    """Parse a synthetic SHOW CREATE MATERIALIZED VIEW DDL and return the reflected
    PARTITION BY expression (rendered via str())."""
    parser = StarRocksTableDefinitionParser.__new__(StarRocksTableDefinitionParser)
    ddl = (
        "CREATE MATERIALIZED VIEW `m` (`dt`, `val`)\n"
        f"{partition_clause}\n"
        "DISTRIBUTED BY HASH(val)\n"
        "REFRESH ASYNC\n"
        'PROPERTIES ("replication_num" = "1")\n'
        "AS SELECT asset_dt AS dt, val FROM t"
    )
    state = parser._parse_mv_ddl("m", ddl, schema="db")
    return str(state.table_options.get(TableInfoKeyWithPrefix.PARTITION_BY))


@pytest.mark.parametrize(
    "partition_clause, expected",
    [
        # Simple column partition.
        ("PARTITION BY province", "province"),
        # Regression: a partition expression referencing an identifier that begins with "as"
        # (e.g. asset_dt) must NOT be truncated. The extraction regex used a bare "AS"
        # terminator that matched the "AS" inside "ASset_dt", cutting the expression down to
        # "date_trunc('day',".
        ("PARTITION BY date_trunc('day', asset_dt)", "date_trunc('day', asset_dt)"),
    ],
)
def test_mv_partition_by_fully_extracted(partition_clause, expected):
    assert _reflect_partition_by(partition_clause) == expected


@pytest.mark.parametrize(
    "refresh_clause, expected",
    [
        # Plain periodic async — the original working case.
        ("REFRESH ASYNC EVERY(INTERVAL 1 HOUR)", "ASYNC EVERY(INTERVAL 1 HOUR)"),
        # Regression: a moment prefix before ASYNC must NOT truncate the clause. The refresh
        # extraction regex used a bare "AS" terminator that matched the "AS" inside "ASYNC",
        # cutting "REFRESH DEFERRED ASYNC EVERY(...)" down to just "REFRESH DEFERRED".
        ("REFRESH DEFERRED ASYNC EVERY(INTERVAL 1 HOUR)", "DEFERRED ASYNC EVERY(INTERVAL 1 HOUR)"),
        ("REFRESH IMMEDIATE ASYNC EVERY(INTERVAL 5 MINUTE)", "IMMEDIATE ASYNC EVERY(INTERVAL 5 MINUTE)"),
        # 4.1 SCHEDULE spelling with a moment prefix, canonicalized back to ASYNC.
        ("REFRESH DEFERRED SCHEDULE EVERY(INTERVAL 1 HOUR)", "DEFERRED ASYNC EVERY(INTERVAL 1 HOUR)"),
        # Bare schemes (no period) are unaffected.
        ("REFRESH DEFERRED ASYNC", "DEFERRED ASYNC"),
        ("REFRESH MANUAL", "MANUAL"),
    ],
)
def test_mv_refresh_clause_fully_extracted(refresh_clause, expected):
    assert _reflect_refresh(refresh_clause) == expected
