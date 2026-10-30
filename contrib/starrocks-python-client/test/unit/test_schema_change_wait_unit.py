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

"""Offline tests for the schema-change wait plumbing.

Covers the pieces that need no cluster: the state enum, the shared
``SHOW ALTER TABLE`` statement builder in the dialect, and the construct
classification used to decide whether to poll at all.
"""

from alembic.ddl import base as alembic_base
from sqlalchemy import Column

from starrocks import INTEGER
from starrocks.alembic.starrocks import SchemaChangeState, StarRocksImpl
from starrocks.dialect import StarRocksDialect
from starrocks.sql.ddl import AlterTableColumns


class TestSchemaChangeState:
    def test_known_states_parse_case_insensitively(self):
        assert SchemaChangeState.parse("finished") is SchemaChangeState.FINISHED
        assert SchemaChangeState.parse("CANCELLED") is SchemaChangeState.CANCELLED
        assert SchemaChangeState.parse("Running") is SchemaChangeState.RUNNING
        assert SchemaChangeState.parse("waiting_txn") is SchemaChangeState.WAITING_TXN
        assert SchemaChangeState.parse("PENDING") is SchemaChangeState.PENDING

    def test_unknown_state_is_not_terminal(self):
        # A state added by a future StarRocks version must keep the caller
        # waiting rather than be mistaken for FINISHED or CANCELLED.
        state = SchemaChangeState.parse("SOME_NEW_STATE")
        assert state is None
        assert state is not SchemaChangeState.FINISHED
        assert state is not SchemaChangeState.CANCELLED


class TestGenShowAlterTableStatement:
    def test_default_shape_is_unchanged(self):
        # Existing callers (e.g. test/system/test_table_lifecycle.py) rely on this.
        stmt = StarRocksDialect.gen_show_alter_table_statement("t", "OPTIMIZE")
        assert stmt == "SHOW ALTER TABLE OPTIMIZE WHERE TableName='t' AND State='RUNNING'"

    def test_schema_is_quoted(self):
        stmt = StarRocksDialect.gen_show_alter_table_statement("t", "COLUMN", schema="my-db")
        assert "FROM `my-db` " in stmt

    def test_poll_shape_used_by_the_wait_feature(self):
        stmt = StarRocksDialect.gen_show_alter_table_statement(
            "t", "COLUMN", schema="db", state=None,
            order_by="JobId DESC", limit=1, bind_table_name=True,
        )
        assert stmt == (
            "SHOW ALTER TABLE COLUMN FROM `db` WHERE TableName=:table_name "
            "ORDER BY JobId DESC LIMIT 1"
        )
        # No state filter: the newest job is wanted whatever state it is in.
        assert "State=" not in stmt

    def test_limit_is_coerced_to_an_int(self):
        stmt = StarRocksDialect.gen_show_alter_table_statement("t", "COLUMN", limit="5")
        assert stmt.endswith("LIMIT 5")


class TestSchemaChangeTarget:
    def test_column_constructs_are_targets(self):
        col = Column("a", INTEGER)
        for construct in (
            AlterTableColumns("t", adds=[col], schema="db"),
            alembic_base.AddColumn("t", col, schema="db"),
            alembic_base.DropColumn("t", col, schema="db"),
        ):
            assert StarRocksImpl._schema_change_target(construct) == ("t", "db")

    def test_unrelated_construct_is_not_a_target(self):
        assert StarRocksImpl._schema_change_target(object()) is None

    def test_missing_table_name_is_not_a_target(self):
        # The declared return type promises a str table name; a construct
        # without one must not reach the poll.
        construct = AlterTableColumns("t", adds=[Column("a", INTEGER)])
        construct.table_name = None
        assert StarRocksImpl._schema_change_target(construct) is None
