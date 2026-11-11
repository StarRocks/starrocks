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

from sqlalchemy import Column, MetaData, Table, inspect, text
from sqlalchemy.orm import declarative_base

from starrocks.common.utils import TableAttributeNormalizer
from starrocks.datatype import DATE, INTEGER, VARCHAR
from starrocks.sql.schema import MaterializedView, View
from test.system.lifecycle import (
    ScriptContentParser,
    _AlembicLifecycleBase,
    wait_for_alter_table_column_or_optimization,
)
from test.util import normalize_sql


logger = logging.getLogger(__name__)


class MaterializedViewLifecycleTest(_AlembicLifecycleBase):
    def test_create_mv_basic(self, alembic_env, sr_engine):
        """Tests creating a basic MV."""
        Base = declarative_base()
        # Create a simple base table for MV to reference
        Table("t_basic", Base.metadata, Column("val", INTEGER), starrocks_properties={"replication_num": "1"})
        MaterializedView("mv1", Base.metadata, definition="SELECT val FROM t_basic", starrocks_refresh="MANUAL")
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Create MV")

        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "create_mv")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        assert "op.create_materialized_view('mv1'," in upgrade_content
        assert "MANUAL" in upgrade_content

        alembic_env.harness.upgrade("head")
        inspector = inspect(sr_engine)
        mv_names = inspector.get_materialized_view_names()
        assert "mv1" in mv_names

        alembic_env.harness.downgrade("-1")
        inspector.clear_cache()
        mv_names = inspector.get_materialized_view_names()
        assert "mv1" not in mv_names

    def test_recreate_downgrade_restores_the_mv_columns_and_data(self, alembic_env, sr_engine):
        """A change ALTER cannot make drops and creates the MV; the downgrade recreates the database's MV.

        The MV is sorted by its second SELECT column, which information_schema lists first, and the downgrade's
        column list must still follow the SELECT.
        """
        Base = declarative_base()
        Table(
            "t_recreate",
            Base.metadata,
            Column("k", INTEGER),
            Column("name", VARCHAR(10)),
            starrocks_duplicate_key="k",
            starrocks_properties={"replication_num": "1"},
        )
        mv = MaterializedView(
            "mv_recreate",
            Base.metadata,
            definition="SELECT name, k FROM t_recreate",
            starrocks_refresh="MANUAL",
            starrocks_order_by="k",
            starrocks_properties={"replication_num": "1"},
        )
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Create MV")
        alembic_env.harness.upgrade("head")
        with sr_engine.begin() as conn:
            conn.exec_driver_sql("INSERT INTO t_recreate VALUES (1, 'alice')")

        mv.comment = "changed"
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Comment MV")
        downgrade_content = ScriptContentParser.extract_downgrade_content(
            ScriptContentParser.check_script_content(alembic_env, 1, "comment_mv")
        )
        assert "Reflected" not in downgrade_content
        alembic_env.harness.upgrade("head")
        alembic_env.harness.downgrade("-1")

        with sr_engine.begin() as conn:
            conn.exec_driver_sql("REFRESH MATERIALIZED VIEW mv_recreate WITH SYNC MODE")
            assert conn.exec_driver_sql("SELECT name, k FROM mv_recreate").all() == [("alice", 1)]
        assert [c["name"] for c in inspect(sr_engine).get_columns("mv_recreate")] == ["name", "k"]

    def test_a_changed_definition_with_a_percent_sign_is_recreated(self, alembic_env, sr_engine):
        """A ``%`` in the definitions reaches the temporary views as written, so StarRocks compares them.

        Before 4.0.2 StarRocks stores an MV's SQL rewritten, and without the temporary views the change would only
        be warned about.
        """
        def model(month_format):
            metadata = MetaData()
            Table("t_pct", metadata, Column("id", INTEGER), Column("dt", DATE), starrocks_properties={"replication_num": "1"})
            MaterializedView(
                "mv_month", metadata, definition=f"SELECT id, date_format(dt, '{month_format}') AS ym FROM t_pct",
                starrocks_refresh="MANUAL", starrocks_properties={"replication_num": "1"},
            )
            return metadata

        alembic_env.harness.generate_autogen_revision(metadata=model("%Y-%m"), message="create")
        alembic_env.harness.upgrade("head")

        alembic_env.harness.generate_autogen_revision(metadata=model("%Y%m"), message="new format")

        upgrade_content = ScriptContentParser.extract_upgrade_content(
            ScriptContentParser.check_script_content(alembic_env, 1, "new_format")
        )
        assert "op.drop_materialized_view('mv_month'" in upgrade_content, upgrade_content
        alembic_env.harness.upgrade("head")
        assert "%Y%m" in inspect(sr_engine).get_materialized_view_definition("mv_month")

    def test_an_mv_reading_a_column_the_revision_adds_is_recreated(self, alembic_env, sr_engine):
        """The temporary view fails while the column is missing, but the database's definition goes through it, so
        the change is certain even before 4.0.2, where StarRocks shows the MV's SQL rewritten."""
        def model(*columns):
            metadata = MetaData()
            Table("t_widened", metadata, Column("id", INTEGER), *columns, starrocks_properties={"replication_num": "1"})
            names = ", ".join(["id", *(column.name for column in columns)])
            MaterializedView(
                "mv_widened", metadata, definition=f"SELECT {names} FROM t_widened",
                starrocks_refresh="MANUAL", starrocks_properties={"replication_num": "1"},
            )
            return metadata

        alembic_env.harness.generate_autogen_revision(metadata=model(), message="create")
        alembic_env.harness.upgrade("head")
        alembic_env.harness.generate_autogen_revision(metadata=model(Column("name", VARCHAR(20))), message="widen")

        upgrade_content = ScriptContentParser.extract_upgrade_content(
            ScriptContentParser.check_script_content(alembic_env, 1, "widen")
        )
        assert "op.drop_materialized_view('mv_widened'" in upgrade_content, upgrade_content
        alembic_env.harness.upgrade("head")
        assert "name" in inspect(sr_engine).get_materialized_view_definition("mv_widened")

    def test_create_mv_comprehensive(self, alembic_env, sr_engine):
        """Tests creating a comprehensive MV with all attributes."""
        Base = declarative_base()
        # Create a table to support MV definition
        Table("t_comp", Base.metadata, Column("val", INTEGER), starrocks_properties={"replication_num": "1"})
        MaterializedView(
            "mv_complex",
            Base.metadata,
            definition="SELECT val FROM t_comp",
            comment="Comprehensive MV",
            starrocks_distributed_by="HASH(val) BUCKETS 8",
            starrocks_order_by="val",
            starrocks_refresh="ASYNC EVERY(INTERVAL 1 HOUR)",
            starrocks_properties={"replication_num": "1"},
        )
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Create comprehensive MV")

        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "create_comprehensive_mv")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        assert "starrocks_distributed_by='HASH(val) BUCKETS 8'" in upgrade_content
        assert "starrocks_order_by='val'" in upgrade_content
        assert "starrocks_refresh='ASYNC EVERY(INTERVAL 1 HOUR)'" in upgrade_content
        assert "'replication_num': '1'" in upgrade_content

        alembic_env.harness.upgrade("head")
        # Verification of reflected attributes is covered in reflection tests

    def test_alter_mv_lifecycle(self, alembic_env, sr_engine):
        """Tests altering an MV's mutable attributes."""
        # 1. Initial state
        Base = declarative_base()
        schema = sr_engine.url.database
        # Base table and MV
        Table("t_alter", Base.metadata, Column("id", INTEGER), starrocks_properties={"replication_num": "1"})
        MaterializedView(
            "mv_to_alter",
            Base.metadata,
            definition=f"SELECT t_alter.id FROM {schema}.t_alter",
            starrocks_refresh="ASYNC",
            starrocks_properties={"replication_num": "1"},
        )
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial MV")
        alembic_env.harness.upgrade("head")

        # 2. Altered metadata
        AlteredBase = declarative_base()
        Table("t_alter", AlteredBase.metadata, Column("id", INTEGER), starrocks_properties={"replication_num": "1"})
        MaterializedView(
            "mv_to_alter",
            AlteredBase.metadata,
            definition=f"SELECT t_alter.id FROM {schema}.t_alter",
            starrocks_refresh="MANUAL",
            starrocks_properties={"replication_num": "1", "session.query_timeout": "1000"},
        )
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter MV")

        # 3. Verify and apply ALTER
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_mv")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        assert "op.alter_materialized_view('mv_to_alter'" in upgrade_content
        assert "refresh='MANUAL'" in upgrade_content
        assert "'session.query_timeout': '1000'" in upgrade_content

        alembic_env.harness.upgrade("head")
        # Downgrade and verify is complex, covered by integration tests

    def test_mixed_schema_lifecycle(self, alembic_env, sr_engine):
        """
        Tests a complex migration with mixed operations on Tables, Views, and MVs.
        """
        # 1. Initial state: one of each
        Base = declarative_base()
        Table("t1", Base.metadata,
              Column("id", INTEGER),
              starrocks_properties={"replication_num": "1"})
        View("v1", Base.metadata, definition="SELECT 1")
        View("v2", Base.metadata, definition="SELECT 2") # To be dropped
        MaterializedView("mv1", Base.metadata, definition="SELECT id FROM t1", starrocks_refresh="MANUAL",
            starrocks_properties={"replication_num": "1"},)
        logger.debug("start to generate autogen revision for the 1st time")
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial state")
        logger.debug("start to upgrade head for the 1st time")
        alembic_env.harness.upgrade("head")

        # 2. Altered state: alter table, drop view, create new mv, keep one unchanged
        AlteredBase = declarative_base()
        Table("t1", AlteredBase.metadata,
              Column("id", INTEGER),
              Column("name", VARCHAR(10)),  # add one column
              starrocks_properties={"replication_num": "1"}) # Alter
        View("v1", AlteredBase.metadata, definition="SELECT id FROM t1") # Alter View
        # v2 is dropped
        MaterializedView("mv1", AlteredBase.metadata, definition="SELECT id FROM t1", starrocks_refresh="MANUAL",
            starrocks_properties={"replication_num": "1"},) # Unchanged
        MaterializedView("mv2", AlteredBase.metadata, definition="SELECT id FROM t1", starrocks_refresh="MANUAL") # New

        logger.debug("start to generate autogen revision for the 2nd time")
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Mixed ops")

        # 3. Verify script content
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "mixed_ops")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        upgrade_content = normalize_sql(upgrade_content)
        assert normalize_sql("op.add_column('t1', sa.Column('name', VARCHAR(length=10)") in upgrade_content
        assert 'op.alter_view("v1"' in upgrade_content
        assert "SELECT id FROM t1" in upgrade_content
        assert 'op.drop_view("v2")' in upgrade_content
        assert normalize_sql("op.create_materialized_view('mv2', 'SELECT id FROM t1', starrocks_refresh='MANUAL')") in upgrade_content
        assert "mv1" not in upgrade_content # Unchanged

        logger.debug("start to upgrade head for the 2nd time")
        alembic_env.harness.upgrade("head")

        # 4. Verify DB state
        wait_for_alter_table_column_or_optimization(sr_engine, "t1", "COLUMN")
        inspector = inspect(sr_engine)
        inspector.clear_cache()
        tables = inspector.get_table_names()
        views = inspector.get_view_names()
        mv_names = inspector.get_materialized_view_names()
        assert "t1" in tables
        assert "name" in [c["name"] for c in inspector.get_columns("t1")]
        assert "v1" in views
        view_def = inspector.get_view_definition("v1")
        assert "id FROM t1".lower() in TableAttributeNormalizer.normalize_sql(view_def, remove_qualifiers=True).lower()
        assert "v2" not in views
        assert "mv1" in mv_names
        assert "mv2" in mv_names

        alembic_env.harness.downgrade("-1") # Back to initial state
        wait_for_alter_table_column_or_optimization(sr_engine, "t1", "COLUMN")
        inspector.clear_cache()
        tables = inspector.get_table_names()
        assert tables is not None and "t1" in tables
        views = inspector.get_view_names()
        assert views is not None
        mv_names = inspector.get_materialized_view_names()
        assert "name" not in [c["name"] for c in inspector.get_columns("t1")]
        assert "v1" in views
        view_def = inspector.get_view_definition("v1")
        assert "SELECT 1" in view_def
        assert "v2" in views
        assert "mv1" in mv_names
        assert "mv2" not in mv_names

    def test_multi_schema_support(self, database, alembic_env, sr_engine):
        """Tests that autogenerate correctly handles multiple schemas with filtering.

        The second schema should be ignored. Both are named after the test's database, so parallel workers do not
        share them.
        """
        schema_one = f"{database}_one"
        schema_two = f"{database}_two"

        with sr_engine.connect() as conn:
            for s in [schema_one, schema_two]:
                conn.execute(text(f"DROP SCHEMA IF EXISTS {s} FORCE"))
                conn.execute(text(f"CREATE SCHEMA {s}"))

            # Create an object in the ignored schema to ensure it's not picked up for a DROP operation
            conn.execute(
                text(f"CREATE TABLE {schema_two}.ignored_table (id INT) DISTRIBUTED BY HASH(id) PROPERTIES ('replication_num' = '1')")
            )

        # Store original config attributes to restore later
        cfg = alembic_env.alembic_cfg
        old_include_schemas = cfg.attributes.get("include_schemas")
        old_include_name = cfg.attributes.get("include_name")

        try:
            # 1. Define metadata with objects in default and schema_one
            Base = declarative_base()
            Table("t_default", Base.metadata, Column("id", INTEGER), starrocks_properties={"replication_num": "1"})
            Table("t_schema1", Base.metadata, Column("id", INTEGER), schema=schema_one, starrocks_properties={"replication_num": "1"})
            View("v_schema1", Base.metadata, definition=f"SELECT id FROM {schema_one}.t_schema1", schema=schema_one)

            # 2. Configure autogenerate with include_schemas and include_name via alembic_cfg.attributes
            def include_name(name, type_, parent_names):
                if type_ == "schema":
                    return name in [None, schema_one]  # None represents the default schema
                else:
                    return True

            cfg.attributes["include_schemas"] = True
            cfg.attributes["include_name"] = include_name

            # 3. Generate and verify script
            alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Multi-schema support")
            script_content = ScriptContentParser.check_script_content(alembic_env, 1, "multi_schema_support")
            upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
            upgrade_content = normalize_sql(upgrade_content)

            # Assertions for default schema object (no schema= parameter)
            assert 'op.create_table("t_default"' in upgrade_content
            assert 'create_table("t_default", schema=' not in upgrade_content

            # Assertions for schema_one objects (MUST have schema= parameter)
            assert 'op.create_table("t_schema1"' in upgrade_content
            assert f'schema="{schema_one}"' in upgrade_content
            assert 'op.create_view("v_schema1"' in upgrade_content

            # Assertion to ensure ignored schema is not present
            assert "ignored_table" not in upgrade_content
            assert schema_two not in upgrade_content

            # 4. Upgrade and verify DB state
            alembic_env.harness.upgrade("head")
            inspector = inspect(sr_engine)
            assert "t_default" in inspector.get_table_names()  # default schema
            assert "t_schema1" in inspector.get_table_names(schema=schema_one)
            assert "v_schema1" in inspector.get_view_names(schema=schema_one)
            assert "ignored_table" in inspector.get_table_names(schema=schema_two)

            # 5. Downgrade and verify DB state
            alembic_env.harness.downgrade("-1")
            inspector.clear_cache()
            assert "t_default" not in inspector.get_table_names()
            assert "t_schema1" not in inspector.get_table_names(schema=schema_one)
            assert "v_schema1" not in inspector.get_view_names(schema=schema_one)
            assert "ignored_table" in inspector.get_table_names(schema=schema_two)

        finally:
            with sr_engine.connect() as conn:
                for s in [schema_one, schema_two]:
                    conn.execute(text(f"DROP SCHEMA IF EXISTS {s} FORCE"))

            # Restore original config attributes
            if old_include_schemas is None:
                cfg.attributes.pop("include_schemas", None)
            else:
                cfg.attributes["include_schemas"] = old_include_schemas

            if old_include_name is None:
                cfg.attributes.pop("include_name", None)
            else:
                cfg.attributes["include_name"] = old_include_name
