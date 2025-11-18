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

from sqlalchemy import Column, Table, inspect, text
from sqlalchemy.orm import declarative_base

from starrocks.common.utils import TableAttributeNormalizer
from starrocks.datatype import INTEGER, VARCHAR
from starrocks.sql.schema import MaterializedView, View
from test import test_utils
from test.system.conftest import AlembicTestEnv
from test.system.test_table_lifecycle import ScriptContentParser


logger = logging.getLogger(__name__)


def test_create_mv_basic(alembic_env: AlembicTestEnv, sr_engine):
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


def test_create_mv_comprehensive(alembic_env: AlembicTestEnv, sr_engine):
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


def test_alter_mv_lifecycle(alembic_env: AlembicTestEnv, sr_engine):
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
        starrocks_properties={"replication_num": "1", "session.insert_timeout": "1000"},
    )
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter MV")

    # 3. Verify and apply ALTER
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_mv")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.alter_materialized_view('mv_to_alter'" in upgrade_content
    assert "refresh='MANUAL'" in upgrade_content
    assert "'session.insert_timeout': '1000'" in upgrade_content

    alembic_env.harness.upgrade("head")
    # Downgrade and verify is complex, covered by integration tests


def test_mixed_schema_lifecycle(alembic_env: AlembicTestEnv, sr_engine):
    """
    Tests a complex migration with mixed operations on Tables, Views, and MVs.
    """
    # 1. Initial state: one of each
    Base = declarative_base()
    Table("t1", Base.metadata, Column("id", INTEGER), starrocks_properties={"replication_num": "1"})
    View("v1", Base.metadata, definition="SELECT 1")
    View("v2", Base.metadata, definition="SELECT 2") # To be dropped
    MaterializedView("mv1", Base.metadata, definition="SELECT id FROM t1", starrocks_refresh="MANUAL",
        starrocks_properties={"replication_num": "1"},)
    logger.debug(f"start to generate autogen revision for the 1st time")
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial state")
    logger.debug(f"start to upgrade head for the 1st time")
    alembic_env.harness.upgrade("head")

    # 2. Altered state: alter table, drop view, create new mv, keep one unchanged
    AlteredBase = declarative_base()
    Table("t1", AlteredBase.metadata, Column("id", INTEGER), Column("name", VARCHAR(10)), starrocks_properties={"replication_num": "1"}) # Alter
    View("v1", AlteredBase.metadata, definition="SELECT id FROM t1") # Alter View
    # v2 is dropped
    MaterializedView("mv1", AlteredBase.metadata, definition="SELECT id FROM t1", starrocks_refresh="MANUAL",
        starrocks_properties={"replication_num": "1"},) # Unchanged
    MaterializedView("mv2", AlteredBase.metadata, definition="SELECT id FROM t1", starrocks_refresh="MANUAL") # New

    logger.debug(f"start to generate autogen revision for the 2nd time")
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Mixed ops")

    # 3. Verify script content
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "mixed_ops")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    upgrade_content = test_utils.normalize_sql(upgrade_content)
    assert test_utils.normalize_sql("op.add_column('t1', sa.Column('name', VARCHAR(length=10)") in upgrade_content
    assert 'op.alter_view("v1"' in upgrade_content
    assert "SELECT id FROM t1" in upgrade_content
    assert 'op.drop_view("v2")' in upgrade_content
    assert test_utils.normalize_sql("op.create_materialized_view('mv2', 'SELECT id FROM t1', starrocks_refresh='MANUAL')") in upgrade_content
    assert "mv1" not in upgrade_content # Unchanged

    logger.debug(f"start to upgrade head for the 2nd time")
    alembic_env.harness.upgrade("head")

    # 4. Verify DB state
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
    inspector.clear_cache()
    tables = inspector.get_table_names()
    views = inspector.get_view_names()
    mv_names = inspector.get_materialized_view_names()
    assert "name" not in [c["name"] for c in inspector.get_columns("t1")]
    assert "v1" in views
    view_def = inspector.get_view_definition("v1")
    assert "SELECT 1" in view_def
    assert "v2" in views
    assert "mv1" in mv_names
    assert "mv2" not in mv_names


def test_multi_schema_support(alembic_env: AlembicTestEnv, sr_engine):
    """Tests that autogenerate correctly handles multiple schemas with filtering."""
    schema_one = "alembic_schema_one"
    schema_two = "alembic_schema_two"  # This schema should be ignored

    with sr_engine.connect() as conn:
        for s in [schema_one, schema_two]:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {s}"))
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
        upgrade_content = test_utils.normalize_sql(upgrade_content)

        # Assertions for default schema object (no schema= parameter)
        assert 'op.create_table("t_default"' in upgrade_content
        assert 'create_table("t_default", schema=' not in upgrade_content

        # Assertions for schema_one objects (MUST have schema= parameter)
        assert f'op.create_table("t_schema1"' in upgrade_content
        assert f'schema="{schema_one}"' in upgrade_content
        assert f'op.create_view("v_schema1"' in upgrade_content

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
                conn.execute(text(f"DROP SCHEMA IF EXISTS {s}"))

        # Restore original config attributes
        if old_include_schemas is None:
            cfg.attributes.pop("include_schemas", None)
        else:
            cfg.attributes["include_schemas"] = old_include_schemas

        if old_include_name is None:
            cfg.attributes.pop("include_name", None)
        else:
            cfg.attributes["include_name"] = old_include_name
