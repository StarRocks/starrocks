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

from sqlalchemy import testing
from sqlalchemy.testing import fixtures

from starrocks.common.params import TableInfoKeyWithPrefix
from starrocks.engine.interfaces import ReflectedMVState
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


class ReflectionMVRefreshTest(fixtures.TestBase):
    @testing.combinations(
        # Simple column partition.
        ("PARTITION BY province", "province"),
        # Regression: a partition expression referencing an identifier that begins with "as"
        # (e.g. asset_dt) must NOT be truncated. The extraction regex used a bare "AS"
        # terminator that matched the "AS" inside "ASset_dt", cutting the expression down to
        # "date_trunc('day',".
        ("PARTITION BY date_trunc('day', asset_dt)", "date_trunc('day', asset_dt)"),
        argnames="partition_clause, expected",
    )
    def test_mv_partition_by_fully_extracted(self, partition_clause, expected):
        assert _reflect_partition_by(partition_clause) == expected

    @testing.combinations(
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
        argnames="refresh_clause, expected",
    )
    def test_mv_refresh_clause_fully_extracted(self, refresh_clause, expected):
        assert _reflect_refresh(refresh_clause) == expected

    @testing.combinations(
        ("areas", "areas"),
        ("last_refresh", "last_refresh"),
        ("x_properties", "x_properties"),
        ("lookup_properties (dt)", "dt"),
        ("haswith_x", "haswith_x"),
        argnames="partition_expression, column",
        id_="aa",
    )
    def test_a_bare_name_ending_in_a_clause_keyword(self, partition_expression, column):
        """A name printed without backquotes that ends in AS, REFRESH or PROPERTIES, or reads AS WITH inside."""
        parser = StarRocksTableDefinitionParser.__new__(StarRocksTableDefinitionParser)
        ddl = (
            f"CREATE MATERIALIZED VIEW `m` (`{column}`, `val`)\n"
            f"PARTITION BY {partition_expression}\n"
            "DISTRIBUTED BY RANDOM\n"
            "REFRESH DEFERRED SCHEDULE EVERY(INTERVAL 1 DAY)\n"
            'PROPERTIES ("colocate_with" = "g1")\n'
            f"AS SELECT {column}, val FROM t"
        )
        state = parser._parse_mv_ddl("m", ddl, schema="db")

        assert str(state.table_options.get(TableInfoKeyWithPrefix.PARTITION_BY)) == partition_expression
        assert str(state.table_options.get(TableInfoKeyWithPrefix.REFRESH)) == "DEFERRED ASYNC EVERY(INTERVAL 1 DAY)"
        assert dict(state.table_options[TableInfoKeyWithPrefix.PROPERTIES]) == {"colocate_with": "g1"}
        assert state.definition == f"SELECT {column}, val FROM t"

    def test_the_fallback_reading_keeps_the_async_after_a_moment(self):
        """The regex used when the REFRESH grammar can't read the clause."""
        parser = StarRocksTableDefinitionParser.__new__(StarRocksTableDefinitionParser)
        state = ReflectedMVState(table_name="m", definition="SELECT 1")

        parser._parse_mv_refresh_with_regex('REFRESH DEFERRED ASYNC\nPROPERTIES ("replication_num" = "1")\n', state)

        assert str(state.table_options[TableInfoKeyWithPrefix.REFRESH]) == "DEFERRED ASYNC"

    def test_comments_naming_clauses_are_not_read_as_them(self):
        """A column comment and the MV comment that say PARTITION BY, REFRESH or AS SELECT, as StarRocks prints them."""
        parser = StarRocksTableDefinitionParser.__new__(StarRocksTableDefinitionParser)
        ddl = (
            'CREATE MATERIALIZED VIEW `mv` (`k` COMMENT "no PARTITION BY k, 12\\" wide", `v`)\n'
            'COMMENT "was \\"REFRESH ASYNC EVERY (INTERVAL 1 DAY)\\", AS SELECT 1"\n'
            "DISTRIBUTED BY HASH(`k`) BUCKETS 1 \n"
            "REFRESH MANUAL\n"
            'PROPERTIES (\n"replication_num" = "1"\n)\n'
            "AS SELECT k, v FROM t;"
        )
        state = parser._parse_mv_ddl("mv", ddl, schema="db")

        assert str(state.table_options.get(TableInfoKeyWithPrefix.REFRESH)) == "MANUAL"
        assert TableInfoKeyWithPrefix.PARTITION_BY not in state.table_options
        assert state.definition == "SELECT k, v FROM t"
