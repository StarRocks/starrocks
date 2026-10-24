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

import logging
import re

from sqlalchemy import Column, inspect, testing
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.suite import is_true

from starrocks.common.params import TableInfoKeyWithPrefix
from starrocks.common.types import SystemRunMode
from starrocks.datatype import (
    ARRAY,
    BIGINT,
    BITMAP,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    DECIMAL,
    DOUBLE,
    FLOAT,
    HLL,
    INTEGER,
    JSON,
    LARGEINT,
    MAP,
    SMALLINT,
    STRING,
    STRUCT,
    TINYINT,
    VARCHAR,
)
from starrocks.requirements import IS_SQLALCHEMY_2
from test.system.lifecycle import (
    DOWNGRADE_STR,
    UPGRADE_STR,
    ScriptContentParser,
    _AlembicLifecycleBase,
    wait_for_alter_table_attributes,
    wait_for_alter_table_column_or_optimization,
)


if IS_SQLALCHEMY_2:
    from sqlalchemy.orm import Mapped, mapped_column


logger = logging.getLogger(__name__)


def print_sql_before_execute(conn, cursor, statement, parameters, context, executemany):
    logger.debug("SQL ready to be executed: %s", statement)


def bind_print_sql_before_execute(engine: Engine):
    """Bind the print_sql_before_execute event to the connection."""
    from sqlalchemy import event
    event.listen(engine, "before_cursor_execute", print_sql_before_execute)


class TableLifecycleTest(_AlembicLifecycleBase):
    def test_create_table_simple(self, database, alembic_env, sr_engine):
        """
        Tests the autogeneration of a CREATE TABLE script and its application.
        """
        # 1. Define the initial state (a single table)
        Base = declarative_base()

        class User(Base):
            __tablename__ = "user"
            id = Column(INTEGER, primary_key=True)
            name = Column(STRING, nullable=False)
            __table_args__ = {
                "starrocks_primary_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num" : "1"},
            }

        # 2. Run autogenerate
        alembic_env.harness.generate_autogen_revision(
            metadata=Base.metadata,
            message="Create user table",
        )

        # # 3. Check the generated script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "create_user_table")

        # Normalize script content for robust comparison
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        normalized_content = re.sub(r'[ \t]+', ' ', upgrade_content).lower()
        logger.debug(f"normalized_content: {normalized_content}")
        assert "op.create_table('user'" in normalized_content
        assert "sa.Column('id', integer()".lower() in normalized_content
        assert "sa.Column('name', string()".lower() in normalized_content
        assert "sa.PrimaryKeyConstraint('id')".lower() in normalized_content
        assert "starrocks_PRIMARY_KEY='id'".lower() in normalized_content
        assert "starrocks_DISTRIBUTED_BY='HASH(id)'".lower() in normalized_content
        assert "'replication_num': '1'" in normalized_content

        # 4. Run upgrade
        alembic_env.harness.upgrade("head")

        # 5. Verify the table in the database
        # TODO: need to be more exact
        with sr_engine.connect() as conn:
            inspector = inspect(conn)
            is_true(inspector.has_table("user"))
            table_opts = inspector.get_table_options("user")
            logger.info("table_opts: %s", table_opts)
            eq_(table_opts[TableInfoKeyWithPrefix.PRIMARY_KEY], "id")
            eq_(table_opts[TableInfoKeyWithPrefix.DISTRIBUTED_BY], "HASH(`id`)")

    def test_idempotency_comprehensive(self, database, alembic_env, sr_engine):
        """
        Tests that a second autogenerate run produces no new scripts for a complex table.
        """
        # 1. Define the comprehensive model
        Base = declarative_base()

        class KitchenSink(Base):
            __tablename__ = "t_kitchen_sink"
            # Column Types
            col_pk = Column(INTEGER(8), primary_key=True)
            col_bool = Column(BOOLEAN, primary_key=True)
            col_tinyint = Column(TINYINT(2), comment="a tiny int")
            col_smallint = Column(SMALLINT)
            col_bigint = Column(BIGINT, nullable=False)
            col_largeint = Column(LARGEINT, default=0)
            col_decimal = Column(DECIMAL(10, 2))
            col_double = Column(DOUBLE)
            col_float = Column(FLOAT)
            col_char = Column(CHAR(10))
            col_varchar = Column(VARCHAR(100))
            col_string = Column(STRING, comment="a string")
            col_date = Column(DATE)
            col_datetime = Column(DATETIME)
            col_array = Column(ARRAY(VARCHAR(20)))
            col_map = Column(MAP(STRING, DECIMAL(5, 2)))
            col_struct = Column(STRUCT(name=VARCHAR(50), age=INTEGER(10)))
            col_nested = Column(STRUCT(
                name=VARCHAR(100),
                details=MAP(
                    STRING,
                    ARRAY(STRUCT(item_id=INTEGER, price=DECIMAL(10, 2)))
                )
            ))
            col_json = Column(JSON)
            col_hll = Column(HLL)
            col_bitmap = Column(BITMAP)

            __table_args__ = {
                "comment": "Comprehensive table for idempotency test",
                "starrocks_primary_key": "col_pk, col_bool",
                "starrocks_distributed_by": "HASH(col_pk)",
                "starrocks_partition_by": "RANGE(col_pk) (PARTITION p1 VALUES LESS THAN ('100'))",
                "starrocks_order_by": "col_string",
                "starrocks_properties": {"replication_num": "1", "storage_medium": "HDD"},
            }

        # 2. Generate initial revision and upgrade
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Create kitchen sink")
        ScriptContentParser.check_script_content(alembic_env, 1, "create_kitchen_sink")
        logger.debug("Upgrade to head.")
        alembic_env.harness.upgrade("head")

        # 3. Run autogenerate again
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Second run kitchen sink")

        # 4. Verify that NO new script was generated
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "second_run_kitchen_sink")
        EMPTY_UPGRADE_PATTERN = re.compile(UPGRADE_STR + r"pass")
        EMPTY_DOWNGRADE_PATTERN = re.compile(DOWNGRADE_STR + r"pass")
        is_true(re.search(EMPTY_UPGRADE_PATTERN, script_content), "Upgrade script should be empty")
        is_true(re.search(EMPTY_DOWNGRADE_PATTERN, script_content), "Downgrade script should be empty")

    @testing.skip("starrocks", "SR doesn't support submiting multiple alter table cluases.")
    def test_alter_table_columns_comprehensive(self, database, alembic_env, sr_engine):
        """Tests comprehensive column alterations: ADD, DROP, ALTER.
        TODO: we may batch ALTER COLUMN clauses into one clause.
        """
        # 1. Initial state
        Base = declarative_base()
        class User(Base):
            __tablename__ = "t_alter_columns"
            id = Column(INTEGER, primary_key=True)
            col_to_modify = Column(INTEGER, nullable=False, comment="Original comment")
            col_to_drop = Column(STRING)
            __table_args__ = {
                "starrocks_primary_key": "id",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial columns")
        alembic_env.harness.upgrade("head")

        # 2. Alter columns in metadata
        AlteredBase = declarative_base()
        class AlteredUser(AlteredBase):
            __tablename__ = "t_alter_columns"
            id = Column(INTEGER, primary_key=True)
            col_to_modify = Column(BIGINT, nullable=True, comment="Modified comment")
            # col_to_drop is removed
            col_added = Column(VARCHAR(100))
            __table_args__ = {
                "starrocks_primary_key": "id",
                "starrocks_distributed_by": "HASH(id)",  # auto added by SR
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter columns")

        # 3. Verify and apply the ALTER script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_columns")
        # upgrade_contnt = ScriptContentParser.extract_upgrade_content(script_content).lower()

        assert "op.add_column('t_alter_columns', sa.Column('col_added', VARCHAR(length=100), nullable=True))" in script_content
        assert "op.drop_column('t_alter_columns', 'col_to_drop')" in script_content
        assert "op.alter_column('t_alter_columns', 'col_to_modify'," in script_content
        assert "type_=BIGINT()" in script_content
        assert "nullable=True" in script_content
        assert "comment='Modified comment'" in script_content

        alembic_env.harness.upgrade("head")

        # 4. Verify in DB and then downgrade
        wait_for_alter_table_column_or_optimization(sr_engine, "t_alter_columns", "COLUMN")
        inspector = inspect(sr_engine)
        columns = inspector.get_columns("t_alter_columns")
        col_names = [c['name'] for c in columns]
        assert 'col_to_drop' not in col_names
        assert 'col_added' in col_names
        for col in columns:
            if col['name'] == 'col_to_modify':
                assert isinstance(col['type'], BIGINT)
                assert col['nullable'] is True
                assert col['comment'] == 'Modified comment'

        alembic_env.harness.downgrade("-1")
        inspector.clear_cache()
        columns = inspector.get_columns("t_alter_columns")
        col_names = [c['name'] for c in columns]
        assert 'col_to_drop' in col_names
        assert 'col_added' not in col_names
        for col in columns:
            if col['name'] == 'col_to_modify':
                assert isinstance(col['type'], INTEGER)
                assert col['nullable'] is False
                assert col['comment'] == 'Original comment'

    def test_add_agg_table_key_column(self, database, alembic_env, sr_engine):
        """Tests adding a key column to an AGGREGATE KEY table.
        Adding a key column is time-consuming, so we need to wait for the table to be created.
        Therefore, we can't submit multiple alter table clauses at once.
        """
        # 1. Initial state
        Base = declarative_base()
        class TAggAddKey(Base):
            __tablename__ = "t_agg_add_key"
            id = Column(INTEGER, primary_key=True)
            __table_args__ = {
                "starrocks_aggregate_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial agg key")
        alembic_env.harness.upgrade("head")

        # 2. Alter metadata: add one key column (site_id)
        AlteredBase = declarative_base()
        class TAggAddKeyAltered(AlteredBase):
            __tablename__ = "t_agg_add_key"
            id = Column(INTEGER, primary_key=True)
            site_id = Column(INTEGER, primary_key=True, nullable=True, starrocks_is_agg_key=True)
            __table_args__ = {
                # Keep table-level attributes minimal in alter context; role is on the column
                "starrocks_aggregate_key": "id, site_id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Add agg key column")

        # 3. Verify and apply the ALTER script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "add_agg_key_column")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        assert "op.add_column('t_agg_add_key', sa.Column('site_id', INTEGER(), nullable=True" in upgrade_content
        assert "starrocks_is_agg_key=True" in upgrade_content
        assert "op.drop_column(" not in upgrade_content
        assert "op.alter_column(" not in upgrade_content

        alembic_env.harness.upgrade("head")

        # 4. Verify in DB and then downgrade
        wait_for_alter_table_column_or_optimization(sr_engine, "t_agg_add_key", "COLUMN")
        inspector = inspect(sr_engine)
        inspector.clear_cache()
        columns = inspector.get_columns("t_agg_add_key")
        col_names = [c['name'] for c in columns]
        logger.debug(f"columns after adding key column: {col_names}")
        assert 'site_id' in col_names

        alembic_env.harness.downgrade("-1")
        wait_for_alter_table_column_or_optimization(sr_engine, "t_agg_add_key", "COLUMN")
        inspector.clear_cache()
        columns = inspector.get_columns("t_agg_add_key")
        col_names = [c['name'] for c in columns]
        logger.debug(f"columns after rollback of key column: {col_names}")
        assert 'site_id' not in col_names

    def test_add_agg_table_value_column(self, database, alembic_env, sr_engine):
        """Tests adding value columns to an AGGREGATE KEY table.
        Adding value columns is simple, so it's quick to finish.
        Therefore, we can submit multiple alter table clauses at once.
        """
        # 1. Initial state
        Base = declarative_base()
        class TAggAddValue(Base):
            __tablename__ = "t_agg_add_value"
            id = Column(INTEGER, primary_key=True)
            __table_args__ = {
                "starrocks_aggregate_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial agg value")
        alembic_env.harness.upgrade("head")

        # 2. Alter metadata: add one value column
        AlteredBase = declarative_base()
        class TAggAddValueAltered(AlteredBase):
            __tablename__ = "t_agg_add_value"
            id = Column(INTEGER, primary_key=True)
            page_views2 = Column(INTEGER, server_default='0', nullable=False, starrocks_agg_type='SUM')
            # last_visit_time2 = Column(DATE, nullable=True, starrocks_agg_type='REPLACE')
            __table_args__ = {
                "starrocks_aggregate_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Add agg value column")

        # 3. Verify and apply the ALTER script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "add_agg_value_column")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        assert "op.add_column('t_agg_add_value', sa.Column('page_views2', INTEGER(), server_default='0', nullable=False" in upgrade_content
        assert "starrocks_agg_type='SUM'" in upgrade_content
        # assert "op.add_column('t_agg_add_value', sa.Column('last_visit_time2', DATE(), nullable=True" in upgrade_content
        # assert "starrocks_agg_type='REPLACE'" in upgrade_content
        assert "op.drop_column(" not in upgrade_content
        assert "op.alter_column(" not in upgrade_content

        alembic_env.harness.upgrade("head")

        # 4. Verify in DB and then downgrade
        wait_for_alter_table_column_or_optimization(sr_engine, "t_agg_add_value", "COLUMN")
        inspector = inspect(sr_engine)
        columns = inspector.get_columns("t_agg_add_value")
        col_names = [c['name'] for c in columns]
        logger.debug(f"columns after adding value column: {col_names}")
        assert 'page_views2' in col_names
        # assert 'last_visit_time2' in col_names

        alembic_env.harness.downgrade("-1")
        wait_for_alter_table_column_or_optimization(sr_engine, "t_agg_add_value", "COLUMN")
        inspector.clear_cache()
        columns = inspector.get_columns("t_agg_add_value")
        col_names = [c['name'] for c in columns]
        logger.debug(f"columns after rollback of value column: {col_names}")
        assert 'page_views2' not in col_names
        # assert 'last_visit_time2' not in col_names

    @testing.skip("starrocks", "Adding a key column is time-consuming, so it fails to add a value column immediately after the key column.")
    def test_add_agg_table_key_and_value_columns(self, database, alembic_env, sr_engine):
        """Tests adding both a key column and a value column to an AGGREGATE KEY table."""
        # 1. Initial state
        Base = declarative_base()
        class TAggAddBoth(Base):
            __tablename__ = "t_agg_add_both"
            id = Column(INTEGER, primary_key=True)
            __table_args__ = {
                "starrocks_aggregate_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial agg both")
        alembic_env.harness.upgrade("head")

        # 2. Alter metadata: add one key column and one value column
        AlteredBase = declarative_base()
        class TAggAddBothAltered(AlteredBase):
            __tablename__ = "t_agg_add_both"
            id = Column(INTEGER, primary_key=True)
            site_id = Column(INTEGER, server_default='10', nullable=False, starrocks_is_agg_key=True)
            page_views = Column(INTEGER, nullable=True, starrocks_agg_type='SUM')
            __table_args__ = {
                "starrocks_aggregate_key": "id, site_id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Add agg key and value columns")

        # 3. Verify and apply the ALTER script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "add_agg_key_and_value_columns")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        assert "op.add_column('t_agg_add_both', sa.Column('site_id', INTEGER(), server_default='10', nullable=False" in upgrade_content
        assert "starrocks_is_agg_key=True" in upgrade_content
        assert "op.add_column('t_agg_add_both', sa.Column('page_views', INTEGER(), nullable=True" in upgrade_content
        assert "starrocks_agg_type='SUM'" in upgrade_content
        assert "op.drop_column(" not in upgrade_content
        assert "op.alter_column(" not in upgrade_content

        alembic_env.harness.upgrade("head")

        # 4. Verify in DB and then downgrade
        inspector = inspect(sr_engine)
        wait_for_alter_table_column_or_optimization(sr_engine, "t_agg_add_both", "COLUMN")
        columns = inspector.get_columns("t_agg_add_both")
        col_names = [c['name'] for c in columns]
        logger.debug(f"columns after adding key and value columns: {col_names}")
        assert 'site_id' in col_names
        assert 'page_views' in col_names

        alembic_env.harness.downgrade("-1")
        wait_for_alter_table_column_or_optimization(sr_engine, "t_agg_add_both", "COLUMN")
        inspector.clear_cache()
        columns = inspector.get_columns("t_agg_add_both")
        col_names = [c['name'] for c in columns]
        logger.debug(f"columns after rollback of key and value columns: {col_names}")
        assert 'site_id' not in col_names
        assert 'page_views' not in col_names

    def test_add_table_column_only(self, database, alembic_env, sr_engine):
        """Tests adding multiple columns via autogenerate and applying them in one revision,
        into a PRIMARY KEY table (not an AGGREGATE KEY table).
        Only one column can be added in one revision for Shared-data arch.
        """
        # 1. Initial state
        Base = declarative_base()
        class TAddOnly(Base):
            __tablename__ = "t_add_only"
            id = Column(INTEGER, primary_key=True)
            __table_args__ = {
                "starrocks_primary_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial add only")
        alembic_env.harness.upgrade("head")

        # 2. Alter metadata: add two columns
        AlteredBase = declarative_base()
        class TAddOnlyAltered(AlteredBase):
            __tablename__ = "t_add_only"
            id = Column(INTEGER, primary_key=True)
            col_added_v = Column(VARCHAR(100))
            # col_added_i = Column(INTEGER)
            __table_args__ = {
                "starrocks_primary_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Add column")

        # 3. Verify and apply the ALTER script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "add_column")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content).strip()
        assert "op.add_column('t_add_only', sa.Column('col_added_v', VARCHAR(length=100), nullable=True))" in upgrade_content
        # assert "op.add_column('t_add_only', sa.Column('col_added_i', INTEGER(), nullable=True))" in upgrade_content
        assert "op.drop_column(" not in upgrade_content
        assert "op.alter_column(" not in upgrade_content

        alembic_env.harness.upgrade("head")

        # 4. Verify in DB and then downgrade
        wait_for_alter_table_column_or_optimization(sr_engine, "t_add_only", "COLUMN")
        inspector = inspect(sr_engine)
        columns = inspector.get_columns("t_add_only")
        col_names = [c['name'] for c in columns]
        logger.debug(f"columns after addtion: {col_names}")
        assert 'col_added_v' in col_names
        # assert 'col_added_i' in col_names

        alembic_env.harness.downgrade("-1")
        wait_for_alter_table_column_or_optimization(sr_engine, "t_add_only", "COLUMN")
        inspector.clear_cache()
        columns = inspector.get_columns("t_add_only")
        col_names = [c['name'] for c in columns]
        logger.debug(f"columns after rollback of addtion: {col_names}")
        assert 'col_added_v' not in col_names
        # assert 'col_added_i' not in col_names

    def test_drop_table_column_only(self, database, alembic_env, sr_engine):
        """Tests dropping multiple columns via autogenerate and applying them in one revision.
        multiple dropping is not support in shared-data arch.
        """
        # 1. Initial state
        Base = declarative_base()
        class TDropOnly(Base):
            __tablename__ = "t_drop_only"
            id = Column(INTEGER, primary_key=True)
            col_to_drop1 = Column(STRING)
            # col_to_drop2 = Column(INTEGER)
            __table_args__ = {
                "starrocks_primary_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial drop only")
        alembic_env.harness.upgrade("head")

        # 2. Alter metadata: drop one column
        AlteredBase = declarative_base()
        class TDropOnlyAltered(AlteredBase):
            __tablename__ = "t_drop_only"
            id = Column(INTEGER, primary_key=True)
            __table_args__ = {
                "starrocks_primary_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Drop column")

        # 3. Verify and apply the ALTER script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "drop_column")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        assert "op.drop_column('t_drop_only', 'col_to_drop1')" in upgrade_content
        # assert "op.drop_column('t_drop_only', 'col_to_drop2')" in upgrade_content
        assert "op.add_column(" not in upgrade_content
        assert "op.alter_column(" not in upgrade_content

        alembic_env.harness.upgrade("head")

        # 4. Verify in DB and then downgrade
        wait_for_alter_table_column_or_optimization(sr_engine, "t_drop_only", "COLUMN")
        inspector = inspect(sr_engine)
        columns = inspector.get_columns("t_drop_only")
        col_names = [c['name'] for c in columns]
        logger.debug(f"columns after drop: {col_names}")
        assert 'col_to_drop1' not in col_names
        # assert 'col_to_drop2' not in col_names

        alembic_env.harness.downgrade("-1")
        wait_for_alter_table_column_or_optimization(sr_engine, "t_drop_only", "COLUMN")
        inspector.clear_cache()
        columns = inspector.get_columns("t_drop_only")
        col_names = [c['name'] for c in columns]
        logger.debug(f"columns after rollback of drop: {col_names}")
        assert 'col_to_drop1' in col_names
        # assert 'col_to_drop2' in col_names

    def test_alter_table_column_only(self, database, alembic_env, sr_engine):
        """Tests altering a single column's type/nullability/comment."""
        # 1. Initial state
        Base = declarative_base()
        class TAlterOnly(Base):
            __tablename__ = "t_alter_only"
            id = Column(INTEGER, primary_key=True)
            col_to_modify = Column(INTEGER, nullable=False, comment="Original comment")
            __table_args__ = {
                "starrocks_primary_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial alter only")
        alembic_env.harness.upgrade("head")

        # 2. Alter metadata: change type, nullability, and comment
        AlteredBase = declarative_base()
        class TAlterOnlyAltered(AlteredBase):
            __tablename__ = "t_alter_only"
            id = Column(INTEGER, primary_key=True)
            col_to_modify = Column(BIGINT, nullable=True, comment="Modified comment")
            __table_args__ = {
                "starrocks_primary_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter column")

        # 3. Verify and apply the ALTER script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_column")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        assert "op.alter_column('t_alter_only', 'col_to_modify'," in upgrade_content
        assert "type_=BIGINT()" in script_content
        assert "nullable=True" in upgrade_content
        assert "comment='Modified comment'" in upgrade_content
        assert "op.add_column(" not in upgrade_content
        assert "op.drop_column(" not in upgrade_content

        alembic_env.harness.upgrade("head")

        wait_for_alter_table_column_or_optimization(sr_engine, "t_alter_only", "COLUMN")
        # 4. Verify in DB and then downgrade
        inspector = inspect(sr_engine)
        inspector.clear_cache()
        columns = inspector.get_columns("t_alter_only")
        for col in columns:
            if col['name'] == 'col_to_modify':
                logger.debug(f"col_to_modify after alter: {col}")
                assert isinstance(col['type'], BIGINT)
                assert col['nullable'] is True
                assert col['comment'] == 'Modified comment'

        # Can't rollback from BITINT -> INT
        # alembic_env.harness.downgrade("-1")
        # wait_for_alter_table_column_or_optimization(sr_engine, "t_alter_only", "COLUMN")
        # inspector.clear_cache()
        # columns = inspector.get_columns("t_alter_only")
        # for col in columns:
        #     if col['name'] == 'col_to_modify':
        #         logger.debug(f"col_to_modify after rollback of alter: {col}")
        #         assert isinstance(col['type'], INTEGER)
        #         assert col['nullable'] is False
        #         assert col['comment'] == 'Original comment'

    def test_alter_table_attributes_distribution(self, database, alembic_env, sr_engine):
        """Tests altering table attributes. Includes:
            distribution, comment, and order by. But:
            1. When there is a distribution chagne (time consuming) running, all other ALTER TABLE can't be submitted. So:
                COMMENT, ORDER BY, need to be extracted into another test cases.
            2. engine, key, partition are not tested here, because they are not supported in StarRocks.
            3. And, all operations are tested in test_alter_table_operations.
        """
        # 1. Initial state
        Base = declarative_base()
        class OriginalTableAttr(Base):
            __tablename__ = "t_alter_attr"
            id = Column(INTEGER, primary_key=True)
            id2 = Column(INTEGER, primary_key=True)
            name = Column(STRING)
            __table_args__ = {
                "starrocks_primary_key": "id, id2",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial attr")
        alembic_env.harness.upgrade("head")

        # 2. Alter attributes in metadata
        AlteredBase = declarative_base()
        class AlteredUser(AlteredBase):
            __tablename__ = "t_alter_attr"
            id = Column(INTEGER, primary_key=True)
            id2 = Column(INTEGER, primary_key=True)
            name = Column(STRING)
            __table_args__ = {
                "starrocks_primary_key": "id, id2",
                # "comment": "A new table comment",
                "starrocks_distributed_by": "HASH(id2) BUCKETS 3",
                # "starrocks_order_by": "name",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter attr")

        # 3. Verify and apply the ALTER script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_attr")
        # is_true(re.search(r"op\.create_table_comment\(\s*'t_alter_attr',\s*(:comment=)?\s*'A new table comment'", script_content))
        is_true(re.search(r"op\.alter_table_distribution\(\s*'t_alter_attr',\s*'HASH\(id2\)',\s*buckets=3", script_content))
        # is_true(re.search(r"op\.alter_table_order\(\s*'t_alter_attr',\s*'name'", script_content))

        # bind_print_sql_before_execute(sr_engine)
        alembic_env.harness.upgrade("head")

        # 4. Verify in DB and then downgrade
        logger.debug("Start to verify in DB and then downgrade.")
        inspector = inspect(sr_engine)
        # we need to wait the ALTER TABLE take effect
        options = wait_for_alter_table_attributes(inspector, "t_alter_attr",
            TableInfoKeyWithPrefix.DISTRIBUTED_BY, "HASH(`id2`) BUCKETS 3")
        # assert options.get("starrocks_ORDER_BY") == "name"
        # assert options.get(TableInfoKeyWithPrefix.COMMENT) == "A new table comment"

        # 5. Downgrade
        alembic_env.harness.downgrade("-1")
        # we need to wait the ALTER TABLE take effect
        options = wait_for_alter_table_attributes(inspector, "t_alter_attr",
            TableInfoKeyWithPrefix.DISTRIBUTED_BY, "HASH(`id`)")
        assert options.get(TableInfoKeyWithPrefix.COMMENT) is None
        # The default order by key is the primary key
        assert options.get(TableInfoKeyWithPrefix.ORDER_BY) == "`id`, `id2`"

    def test_alter_table_order_by(self, database, alembic_env, sr_engine):
        """Tests altering table order by separately."""
        # 1. Initial state
        Base = declarative_base()
        class OriginalTable(Base):
            __tablename__ = "t_alter_order"
            id = Column(INTEGER, primary_key=True)
            name = Column(STRING)
            __table_args__ = {
                "starrocks_primary_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial order")
        alembic_env.harness.upgrade("head")

        # 2. Alter order by in metadata
        AlteredBase = declarative_base()
        class AlteredTable(AlteredBase):
            __tablename__ = "t_alter_order"
            id = Column(INTEGER, primary_key=True)
            name = Column(STRING)
            __table_args__ = {
                "starrocks_primary_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_order_by": "name",
                "starrocks_properties": {"replication_num": "1"},
            }
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter order")

        # 3. Verify and apply the ALTER script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_order")
        is_true(re.search(r"op\.alter_table_order\(\s*'t_alter_order',\s*'name'", script_content))

        alembic_env.harness.upgrade("head")

        # 4. Verify in DB and then downgrade
        inspector = inspect(sr_engine)
        options = wait_for_alter_table_attributes(inspector, "t_alter_order", TableInfoKeyWithPrefix.ORDER_BY, "`name`")
        assert options.get(TableInfoKeyWithPrefix.ORDER_BY) == "`name`"

        # 5. Downgrade
        alembic_env.harness.downgrade("-1")
        options = wait_for_alter_table_attributes(inspector, "t_alter_order", TableInfoKeyWithPrefix.ORDER_BY, "`id`")
        assert options.get(TableInfoKeyWithPrefix.ORDER_BY) == "`id`"

    def _alter_table_properties_and_comment(
        self, alembic_env, sr_engine, initial_table_args: dict, altered_table_args: dict
    ) -> str:
        """Alter a table's comment and properties through a migration, then downgrade it step by step.

        - Adds a new table comment.
        - Adds a new property 'replicated_storage'.
        - Changes an existing property 'storage_medium'.

        Returns the generated ALTER script for the caller's own checks.
        """
        # 1. Initial state
        Base = declarative_base()

        class User(Base):
            __tablename__ = "t_alter_props"
            id = Column(INTEGER, primary_key=True)
            __table_args__ = initial_table_args

        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
        alembic_env.harness.upgrade("head")

        # 2. Alter properties in metadata
        AlteredBase = declarative_base()

        class AlteredUser(AlteredBase):
            __tablename__ = "t_alter_props"
            id = Column(INTEGER, primary_key=True)
            __table_args__ = altered_table_args

        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter props")

        # 3. Verify and apply the ALTER script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_props")
        assert "op.alter_table" in script_content
        sr_engine.dialect._init_run_mode(sr_engine.connect())  # make sure the run_mode is set
        if sr_engine.dialect.run_mode == SystemRunMode.SHARED_DATA:
            assert 'default.storage_medium' not in script_content
        else:
            assert "'replicated_storage': 'false'" in script_content
            assert "'default.storage_medium': 'SSD'" in script_content
        assert "comment='A new table comment'" in script_content

        alembic_env.harness.upgrade("head")

        # 4. Verify in DB
        inspector = inspect(sr_engine)
        options = inspector.get_table_options("t_alter_props")
        props = options[TableInfoKeyWithPrefix.PROPERTIES]
        if sr_engine.dialect.run_mode != SystemRunMode.SHARED_DATA:
            assert props['replicated_storage'] == 'false'
            assert props['storage_medium'] == 'SSD'
        assert options.get(TableInfoKeyWithPrefix.COMMENT) == "A new table comment"

        # 5. Downgrade one revision, not to the base
        alembic_env.harness.downgrade("-1")

        inspector.clear_cache()
        options = inspector.get_table_options("t_alter_props")
        props = options[TableInfoKeyWithPrefix.PROPERTIES]
        if sr_engine.dialect.run_mode != SystemRunMode.SHARED_DATA:
            assert props['replicated_storage'] == 'true'
            assert props['storage_medium'] == 'HDD'
        assert options.get(TableInfoKeyWithPrefix.COMMENT) is None

        # 6. Downgrade to base
        alembic_env.harness.downgrade("base")
        inspector.clear_cache()
        is_true(not inspector.has_table("t_alter_props"))
        return script_content

    def test_alter_table_properties_and_comment(self, database, alembic_env, sr_engine):
        """Alter the comment and properties of a PRIMARY KEY table, which StarRocks distributes by HASH."""
        self._alter_table_properties_and_comment(
            alembic_env,
            sr_engine,
            {
                "starrocks_primary_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1", "storage_medium": "HDD"},
            },
            {
                "starrocks_primary_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "comment": "A new table comment",
                "starrocks_properties": {"replication_num": "1", "replicated_storage": "false", "storage_medium": "SSD"},
            },
        )

    def test_alter_table_bucket_size_with_properties_and_comment(self, database, alembic_env, sr_engine):
        """Alter bucket_size with the comment and other properties, on a RANDOM-distributed DUPLICATE KEY table.

        StarRocks 4.1 allows bucket_size only on RANDOM-distributed tables with automatic bucketing, so this
        case cannot use the PRIMARY KEY table above.
        """
        script_content = self._alter_table_properties_and_comment(
            alembic_env,
            sr_engine,
            {
                "starrocks_duplicate_key": "id",
                "starrocks_distributed_by": "RANDOM",
                "starrocks_properties": {"replication_num": "1", "storage_medium": "HDD", "bucket_size": "4294967296"},
            },
            {
                "starrocks_duplicate_key": "id",
                "starrocks_distributed_by": "RANDOM",
                "comment": "A new table comment",
                "starrocks_properties": {
                    "replication_num": "1",
                    "replicated_storage": "false",
                    "storage_medium": "SSD",
                    "bucket_size": "1000000000",
                },
            },
        )
        assert "bucket_size" in script_content

    def test_drop_table(self, database, alembic_env, sr_engine):
        """Tests dropping a table."""
        # 1. Initial state: (create a table)
        Base = declarative_base()
        class User(Base):
            __tablename__ = "user"
            id = Column(INTEGER, primary_key=True)
            __table_args__ = {
                "starrocks_primary_key": "id",
                "starrocks_properties": {"replication_num": "1"}
            }

        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
        alembic_env.harness.upgrade("head")
        inspector = inspect(sr_engine)
        is_true(inspector.has_table("user"))

        # 2. Drop the table from metadata
        logger.debug("Start to compare the table 'user' with the empty metadata.")
        EmptyBase = declarative_base()
        alembic_env.harness.generate_autogen_revision(metadata=EmptyBase.metadata, message="Drop user")

        # 3. Verify and apply the DROP script
        versions_dir = alembic_env.versions_path
        scripts = list(versions_dir.glob("*drop_user.py"))
        eq_(len(scripts), 1)
        script_content: str = scripts[0].read_text()
        logger.debug("script_content: %s", script_content)
        UPGRADE_PATTERN = re.compile(UPGRADE_STR + r"op.drop_table\('user'\)\s*\n\s*#")
        DOWNGRADE_PATTERN = re.compile(DOWNGRADE_STR + r"op.create_table\('user'")
        is_true(UPGRADE_PATTERN.search(script_content), "Upgrade script should contain DROP TABLE operation")
        is_true(DOWNGRADE_PATTERN.search(script_content), "Downgrade script should contain CREATE TABLE operation")

        logger.debug("Start to do upgrade: drop the table 'user'.")
        alembic_env.harness.upgrade("head")
        logger.debug("Upgraded to head: drop the table 'user'.")
        inspector.clear_cache()
        is_true(not inspector.has_table("user"))

        # 4. Downgrade and verify table is restored
        logger.debug("Start to do downgrade: restore the table 'user'.")
        alembic_env.harness.downgrade("-1")
        logger.debug("Downgraded to base: restore the table 'user'.")
        inspector.clear_cache()
        is_true(inspector.has_table("user"))

    def test_drop_duplicate_key_table(self, database, alembic_env, sr_engine):
        """Tests dropping a duplicate key table.
        Alembic should not generate useless 'index' for upgrade script.
        """
        # 1. Initial state: (create a table)
        Base = declarative_base()

        class UserDuplicate(Base):
            __tablename__ = "user_duplicate"
            id = Column(INTEGER, primary_key=True)
            name = Column(STRING, nullable=False)
            __table_args__ = {
                "starrocks_duplicate_key": "id",
                "starrocks_properties": {"replication_num": "1"},
            }

        alembic_env.harness.generate_autogen_revision(
            metadata=Base.metadata, message="Initial duplicate"
        )
        alembic_env.harness.upgrade("head")
        inspector = inspect(sr_engine)
        is_true(inspector.has_table("user_duplicate"))

        # 2. Drop the table from metadata
        logger.debug("Start to compare the table 'user_duplicate' with the empty metadata.")
        EmptyBase = declarative_base()
        alembic_env.harness.generate_autogen_revision(
            metadata=EmptyBase.metadata, message="Drop user_duplicate"
        )

        # 3. Verify and apply the DROP script
        versions_dir = alembic_env.versions_path
        scripts = list(versions_dir.glob("*drop_user_duplicate.py"))
        eq_(len(scripts), 1)
        script_content: str = scripts[0].read_text()
        logger.debug("script_content: %s", script_content)
        UPGRADE_PATTERN = re.compile(UPGRADE_STR + r"op.drop_table\('user_duplicate'\)\s*\n\s*#")
        DOWNGRADE_PATTERN = re.compile(DOWNGRADE_STR + r"op.create_table\('user_duplicate'")
        is_true(
            UPGRADE_PATTERN.search(script_content),
            "Upgrade script should contain DROP TABLE operation",
        )
        is_true(
            DOWNGRADE_PATTERN.search(script_content),
            "Downgrade script should contain CREATE TABLE operation",
        )

    def test_drop_aggregate_key_table(self, database, alembic_env, sr_engine):
        """Tests dropping an aggregate key table.
        Alembic should not generate useless 'index' for upgrade script.
        """
        # 1. Initial state: (create a table)
        Base = declarative_base()

        class UserAggregate(Base):
            __tablename__ = "user_aggregate"
            id = Column(INTEGER, primary_key=True)
            # name = Column(STRING, starrocks_agg_type="SUM")
            name: Mapped[str] = mapped_column(STRING, starrocks_agg_type="REPLACE")
            __table_args__ = {
                "starrocks_aggregate_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }

        alembic_env.harness.generate_autogen_revision(
            metadata=Base.metadata, message="Initial aggregate"
        )
        alembic_env.harness.upgrade("head")
        inspector = inspect(sr_engine)
        is_true(inspector.has_table("user_aggregate"))

        # 2. Drop the table from metadata
        logger.debug("Start to compare the table 'user_aggregate' with the empty metadata.")
        EmptyBase = declarative_base()
        alembic_env.harness.generate_autogen_revision(
            metadata=EmptyBase.metadata, message="Drop user_aggregate"
        )

        # 3. Verify and apply the DROP script
        versions_dir = alembic_env.versions_path
        scripts = list(versions_dir.glob("*drop_user_aggregate.py"))
        eq_(len(scripts), 1)
        script_content: str = scripts[0].read_text()
        logger.debug("script_content: %s", script_content)
        UPGRADE_PATTERN = re.compile(UPGRADE_STR + r"op.drop_table\('user_aggregate'\)\s*\n\s*#")
        DOWNGRADE_PATTERN = re.compile(DOWNGRADE_STR + r"op.create_table\('user_aggregate'")
        is_true(
            UPGRADE_PATTERN.search(script_content),
            "Upgrade script should contain DROP TABLE operation",
        )
        is_true(
            DOWNGRADE_PATTERN.search(script_content),
            "Downgrade script should contain CREATE TABLE operation",
        )

        logger.debug("Start to do upgrade: drop the table 'user_aggregate'.")
        alembic_env.harness.upgrade("head")
        logger.debug("Upgraded to head: drop the table 'user_aggregate'.")
        inspector.clear_cache()
        is_true(not inspector.has_table("user_aggregate"))

        # 4. Downgrade and verify table is restored
        logger.debug("Start to do downgrade: restore the table 'user_aggregate'.")
        alembic_env.harness.downgrade("-1")
        logger.debug("Downgraded to base: restore the table 'user_aggregate'.")
        inspector.clear_cache()
        is_true(inspector.has_table("user_aggregate"))

    def test_drop_unique_key_table(self, database, alembic_env, sr_engine):
        """Tests dropping a unique key table.
        Alembic should not generate useless 'index' for upgrade script.
        """
        # 1. Initial state: (create a table)
        Base = declarative_base()

        class UserUnique(Base):
            __tablename__ = "user_unique"
            id = Column(INTEGER, primary_key=True)
            name = Column(STRING, nullable=False)
            __table_args__ = {
                "starrocks_unique_key": "id",
                "starrocks_distributed_by": "HASH(id)",
                "starrocks_properties": {"replication_num": "1"},
            }

        alembic_env.harness.generate_autogen_revision(
            metadata=Base.metadata, message="Initial unique"
        )
        alembic_env.harness.upgrade("head")
        inspector = inspect(sr_engine)
        is_true(inspector.has_table("user_unique"))

        # 2. Drop the table from metadata
        logger.debug("Start to compare the table 'user_unique' with the empty metadata.")
        EmptyBase = declarative_base()
        alembic_env.harness.generate_autogen_revision(
            metadata=EmptyBase.metadata, message="Drop user_unique"
        )

        # 3. Verify and apply the DROP script
        versions_dir = alembic_env.versions_path
        scripts = list(versions_dir.glob("*drop_user_unique.py"))
        eq_(len(scripts), 1)
        script_content: str = scripts[0].read_text()
        logger.debug("script_content: %s", script_content)
        UPGRADE_PATTERN = re.compile(UPGRADE_STR + r"op.drop_table\('user_unique'\)\s*\n\s*#")
        DOWNGRADE_PATTERN = re.compile(DOWNGRADE_STR + r"op.create_table\('user_unique'")
        is_true(
            UPGRADE_PATTERN.search(script_content),
            "Upgrade script should contain DROP TABLE operation",
        )
        is_true(
            DOWNGRADE_PATTERN.search(script_content),
            "Downgrade script should contain CREATE TABLE operation",
        )
