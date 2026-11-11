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

"""Alembic's offline ``op.bulk_insert`` and ``op.inline_literal`` of ARRAY, MAP and STRUCT values.

Alembic renders those values through its own inline-literal construct rather than a bound parameter, so the
script it writes is run here and the stored values are read back.
"""

from datetime import date
from decimal import Decimal
import io

from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
from sqlalchemy import Column, Integer, Table, select, testing
from sqlalchemy.testing import eq_, fixtures

from starrocks.datatype import ARRAY, DATE, DECIMAL, INTEGER, MAP, STRUCT, VARCHAR


class OfflineInlineLiteralTest(fixtures.TestBase):
    """Each value is rendered by ``op.bulk_insert`` and by ``op.inline_literal``, with and without literal binds."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.combinations(
        ("array_of_integer", ARRAY(INTEGER()), [1, None, 3]),
        ("array_of_varchar", ARRAY(VARCHAR(10)), ["a", "it's", None]),
        ("array_of_decimal", ARRAY(DECIMAL(10, 2)), [Decimal("1.25"), None]),
        ("array_of_date", ARRAY(DATE()), [date(2024, 2, 29)]),
        ("generic_array", sa.ARRAY(sa.Integer), [1, 2]),
        ("map", MAP(VARCHAR(5), INTEGER()), {"k": 1}),
        ("struct", STRUCT(a=INTEGER(), s=VARCHAR(5)), {"a": 1, "s": "x"}),
        ("array_of_struct", ARRAY(STRUCT(a=INTEGER())), [{"a": 1}, None]),
        argnames="type_,value",
        id_="iaa",
    )
    def test_value_is_stored_exactly(self, connection, metadata, type_, value):
        table = Table(
            "test_offline_literal",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("v", type_),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        table.create(connection)

        statements = self._offline_inserts(table, value, literal_binds=True) + self._offline_inserts(
            table, value, literal_binds=False, first_id=3
        )
        for statement in statements:
            connection.exec_driver_sql(statement)

        eq_(connection.execute(select(table.c.v).order_by(table.c.id)).scalars().all(), [value] * 4)

    @staticmethod
    def _offline_inserts(table: Table, value, literal_binds: bool, first_id: int = 1) -> list:
        buffer = io.StringIO()
        context = MigrationContext.configure(
            dialect_name="starrocks",
            opts={"as_sql": True, "output_buffer": buffer, "literal_binds": literal_binds},
            dialect_opts={"paramstyle": "named"},
        )
        operations = Operations(context)
        operations.bulk_insert(table, [{"id": first_id, "v": value}])
        operations.bulk_insert(
            table, [{"id": first_id + 1, "v": operations.inline_literal(value, type_=table.c.v.type)}]
        )
        return [statement.strip() for statement in buffer.getvalue().split(";\n") if statement.strip()]
