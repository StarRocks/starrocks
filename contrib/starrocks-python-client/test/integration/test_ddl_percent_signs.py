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

"""DDL whose SQL text holds a ``%``, as in ``str2date(s, '%Y-%m-%d')``, reaches StarRocks as written."""

from sqlalchemy import Column, Table, inspect, testing
from sqlalchemy.testing import fixtures

from starrocks import DATE, INTEGER, VARCHAR
from starrocks.sql.schema import MaterializedView, View


class PercentSignInDdlTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    def test_a_partition_expression(self, metadata, connection):
        Table(
            "pct_partitioned",
            metadata,
            Column("s", VARCHAR(20)),
            Column("v", INTEGER),
            starrocks_partition_by=(
                "RANGE(str2date(s, '%Y-%m-%d')) (START ('2026-01-01') END ('2026-01-03') EVERY (INTERVAL 1 DAY))"
            ),
            starrocks_distributed_by="HASH(v)",
            starrocks_properties={"replication_num": "1"},
        )
        metadata.create_all(connection)
        partition_by = inspect(connection).get_table_options("pct_partitioned")["starrocks_partition_by"]
        assert "'%Y-%m-%d'" in str(partition_by), partition_by

    @testing.requires.sqlalchemy_2
    def test_view_and_materialized_view_definitions(self, metadata, connection):
        Table("pct_base", metadata, Column("d", DATE), starrocks_properties={"replication_num": "1"})
        View("pct_view", metadata, definition="SELECT date_format(d, '%Y') AS y FROM pct_base")
        MaterializedView(
            "pct_mv",
            metadata,
            definition="SELECT date_format(d, '%Y') AS y FROM pct_base",
            starrocks_refresh="MANUAL",
            starrocks_properties={"replication_num": "1"},
        )
        metadata.create_all(connection)
        assert "%Y" in inspect(connection).get_view_definition("pct_view")
