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

"""Column comparisons in autogenerate against tables reflected from StarRocks."""

from unittest.mock import Mock

from alembic.autogenerate.api import AutogenContext
from alembic.operations import Operations
from alembic.operations.ops import AlterColumnOp
from alembic.runtime.migration import MigrationContext
from sqlalchemy import BIGINT, Column, Integer, MetaData, Table, exc, inspect, testing
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import expect_raises_message

from starrocks.alembic.compare import compare_starrocks_column_agg_type, compare_starrocks_column_autoincrement
from starrocks.common.types import ColumnAggType
from starrocks.datatype import STRUCT
from test.util import AUTO_INCREMENT_COMMENT_MODIFY_NEEDS_4_1_3


AGGREGATE_TABLE_DDL = (
    "CREATE TABLE {name} (id BIGINT, v BIGINT SUM) AGGREGATE KEY(id) DISTRIBUTED BY HASH(id) "
    'PROPERTIES("replication_num"="1")'
)


class _ReflectedTableFixture:
    """Creates a table with plain DDL, reflects it, and drops only that table afterwards."""

    @testing.fixture
    def reflected_table(self, connection):
        created = []

        def create_and_reflect(name: str, ddl: str) -> Table:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            connection.exec_driver_sql(ddl.format(name=name))
            created.append(name)
            return Table(name, MetaData(), autoload_with=connection)

        yield create_and_reflect
        for name in created:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

    def _autogen_context(self, connection) -> Mock:
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect = connection.dialect
        return autogen_context


class CompareColumnAggTypeTest(_ReflectedTableFixture, fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    def _metadata_table(self, name: str, agg_type: ColumnAggType) -> Table:
        return Table(
            name,
            MetaData(),
            Column("id", BIGINT),
            Column("v", BIGINT, starrocks_agg_type=agg_type),
            starrocks_distributed_by="HASH(id)",
        )

    def test_compare_agg_type_no_changes(self, connection, reflected_table, caplog):
        conn_table = reflected_table("compare_agg_type_no_changes", AGGREGATE_TABLE_DDL)
        meta_table = self._metadata_table("compare_agg_type_no_changes", ColumnAggType.SUM)
        caplog.set_level("INFO")

        compare_starrocks_column_agg_type(
            self._autogen_context(connection), None, None, conn_table.name, "v", conn_table.c.v, meta_table.c.v
        )
        assert "aggregation type" not in caplog.text

    def test_compare_agg_type_changes_warns(self, connection, reflected_table, caplog):
        conn_table = reflected_table("compare_agg_type_changes_warns", AGGREGATE_TABLE_DDL)
        meta_table = self._metadata_table("compare_agg_type_changes_warns", ColumnAggType.REPLACE)

        compare_starrocks_column_agg_type(
            self._autogen_context(connection), None, None, conn_table.name, "v", conn_table.c.v, meta_table.c.v
        )
        assert "does not support changing the aggregation type" in caplog.text


class CompareColumnAutoIncrementTest(_ReflectedTableFixture, fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    def _compare_with_autoincrement_metadata(self, connection, conn_table: Table) -> object:
        meta_table = Table(
            conn_table.name,
            MetaData(),
            Column("id", BIGINT, autoincrement=True),
            starrocks_properties={"replication_num": "1"},
        )
        return compare_starrocks_column_autoincrement(
            self._autogen_context(connection), None, None, conn_table.name, "id", conn_table.c.id, meta_table.c.id
        )

    def test_compare_autoincrement_no_changes(self, connection, reflected_table, caplog):
        conn_table = reflected_table(
            "compare_autoincrement_no_changes",
            "CREATE TABLE {name} (id BIGINT NOT NULL AUTO_INCREMENT) DISTRIBUTED BY RANDOM "
            'PROPERTIES("replication_num"="1")',
        )
        caplog.set_level("INFO")

        assert not self._compare_with_autoincrement_metadata(connection, conn_table)
        assert "cannot add or remove AUTO_INCREMENT" not in caplog.text

    def test_the_alter_of_an_integer_primary_key_carries_no_autoincrement(self, connection, reflected_table):
        conn_table = reflected_table(
            "compare_integer_primary_key",
            "CREATE TABLE {name} (id INT NOT NULL COMMENT 'old', v INT) PRIMARY KEY(id) DISTRIBUTED BY HASH(id) "
            'PROPERTIES("replication_num"="1")',
        )
        meta_table = Table(
            conn_table.name, MetaData(), Column("id", Integer, primary_key=True, comment="new"), Column("v", Integer)
        )
        alter_column_op = AlterColumnOp(conn_table.name, "id", modify_comment="new", autoincrement=True)
        compare_starrocks_column_autoincrement(
            self._autogen_context(connection), alter_column_op, None, conn_table.name, "id",
            conn_table.c.id, meta_table.c.id,
        )
        assert "autoincrement" not in alter_column_op.kw

    @AUTO_INCREMENT_COMMENT_MODIFY_NEEDS_4_1_3
    def test_an_alter_without_existing_autoincrement_reads_it_from_the_database(self, connection, reflected_table):
        conn_table = reflected_table(
            "alter_reads_autoincrement",
            "CREATE TABLE {name} (k INT, id BIGINT NOT NULL AUTO_INCREMENT COMMENT 'old') DUPLICATE KEY(k) "
            'DISTRIBUTED BY RANDOM PROPERTIES("replication_num"="1")',
        )
        Operations(MigrationContext.configure(connection)).alter_column(
            conn_table.name, "id", comment="new", existing_type=BIGINT(), existing_nullable=False, autoincrement=True
        )
        columns = {column["name"]: column for column in inspect(connection).get_columns(conn_table.name)}
        assert (columns["id"]["comment"], columns["id"]["autoincrement"]) == ("new", True)

    def test_an_alter_adding_autoincrement_is_refused(self, connection, reflected_table):
        conn_table = reflected_table(
            "alter_adds_autoincrement",
            "CREATE TABLE {name} (k INT, id BIGINT NOT NULL COMMENT 'old') DUPLICATE KEY(k) "
            'DISTRIBUTED BY RANDOM PROPERTIES("replication_num"="1")',
        )
        with expect_raises_message(exc.CompileError, "cannot add AUTO_INCREMENT"):
            Operations(MigrationContext.configure(connection)).alter_column(
                conn_table.name, "id", comment="new", existing_type=BIGINT(), existing_nullable=False,
                autoincrement=True,
            )

    @testing.combinations(("duplicate_key", "DUPLICATE"), ("primary_key", "PRIMARY"), argnames="model", id_="ia")
    def test_an_alter_reads_autoincrement_past_a_comment_holding_the_next_column_line(
        self, connection, reflected_table, model
    ):
        """The comment of ``a`` holds a line like ``b``'s own, with AUTO_INCREMENT, which ``b`` does not have.

        StarRocks 3.3 leaves the comment's ``"`` unescaped in a PRIMARY KEY table, so the line looks just like one.
        """
        conn_table = reflected_table(
            "alter_past_next_line_comment",
            "CREATE TABLE {name} (a INT NOT NULL COMMENT 'one,\n  `b` bigint(20) NOT NULL AUTO_INCREMENT COMMENT \"', "
            f"b BIGINT NULL COMMENT 'old') {model} KEY(a) DISTRIBUTED BY HASH(a) BUCKETS 1 "
            'PROPERTIES("replication_num"="1")',
        )
        operations = Operations(MigrationContext.configure(connection, opts={"starrocks_wait_for_schema_change": True}))

        operations.alter_column(
            conn_table.name, "b", comment="new", existing_type=BIGINT(), existing_nullable=True, autoincrement=False
        )

        columns = {column["name"]: column for column in inspect(connection).get_columns(conn_table.name)}
        assert (columns["b"]["comment"], columns["b"]["autoincrement"]) == ("new", False)

    @testing.combinations(
        ("past_a_quote_in_a_field_name", "ai", BIGINT(), False, "cannot remove AUTO_INCREMENT"),
        ("in_a_field_named_null_auto_increment", "n", STRUCT(("null auto_increment", Integer())), True,
         "cannot add AUTO_INCREMENT"),
        argnames="column_name, existing_type, autoincrement, message",
        id_="iaaaa",
    )
    def test_an_alter_reads_autoincrement_next_to_struct_field_names(
        self, connection, column_name, existing_type, autoincrement, message
    ):
        """StarRocks 3.3 prints STRUCT field names bare, so the quote in ``s``'s type paired with the partition's."""
        name = "alter_past_struct_field_names"
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
        connection.exec_driver_sql(
            f"CREATE TABLE {name} (k INT, dt DATETIME NOT NULL, s STRUCT<`owner's` INT> NULL, "
            "n STRUCT<`null auto_increment` INT> NULL, ai BIGINT NOT NULL AUTO_INCREMENT) DUPLICATE KEY(k, dt) "
            "PARTITION BY date_trunc('day', dt) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\")"
        )
        try:
            with expect_raises_message(exc.CompileError, message):
                Operations(MigrationContext.configure(connection)).alter_column(
                    name, column_name, comment="new", existing_type=existing_type,
                    existing_nullable=column_name != "ai", autoincrement=autoincrement,
                )
        finally:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

    def test_compare_autoincrement_changes_warns(self, connection, reflected_table, caplog):
        """A column without AUTO_INCREMENT against metadata asking for it: no op, but a warning."""
        conn_table = reflected_table(
            "compare_autoincrement_changes_warns",
            'CREATE TABLE {name} (id BIGINT NOT NULL) DISTRIBUTED BY RANDOM PROPERTIES("replication_num"="1")',
        )
        caplog.set_level("WARNING")

        assert not self._compare_with_autoincrement_metadata(connection, conn_table)
        assert any(
            "cannot add or remove AUTO_INCREMENT" in r.getMessage() and r.levelname == "WARNING" for r in caplog.records
        )
