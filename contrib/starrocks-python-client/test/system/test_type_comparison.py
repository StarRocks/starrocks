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

"""
Integration test to verify that type comparison logic actually catches real differences.

This test is specifically designed to address the issue mentioned where complex type
comparison might not be implemented correctly, leading to tests passing when they
should fail. It creates scenarios where subtle type differences should be detected
by the comparison logic and cause appropriate migration scripts to be generated.

The test simulates real-world scenarios where a developer changes a complex type
definition and expects Alembic to generate the appropriate migration.
"""
import logging

from sqlalchemy import Column, Engine, inspect
from sqlalchemy.orm import declarative_base

from starrocks.datatype import ARRAY, BOOLEAN, DECIMAL, INTEGER, MAP, STRING, STRUCT, TINYINT, VARCHAR
from test.system.conftest import AlembicTestEnv
from test.system.test_table_lifecycle import ScriptContentParser


logger = logging.getLogger(__name__)


def test_complex_type_comparison_actually_works(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """
    Test that complex type comparison logic actually detects differences and generates migrations.

    This test is crucial because it verifies that the type comparison logic isn't just
    returning "no differences" for all complex types due to incomplete implementation.
    """

    # 1. Create initial table with complex types
    Base1 = declarative_base()

    class ComplexTable1(Base1):
        __tablename__ = "complex_comparison_test"
        id = Column(INTEGER, primary_key=True)

        # Complex nested structure
        user_data = Column(STRUCT(
            profile=STRUCT(
                name=VARCHAR(100),
                age=INTEGER,
                active=BOOLEAN
            ),
            preferences=MAP(STRING, VARCHAR(50)),
            tags=ARRAY(STRING),
            scores=ARRAY(DECIMAL(8, 2))
        ))

        # Simple array for baseline
        simple_tags = Column(ARRAY(VARCHAR(30)))

        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    # Generate and apply initial migration
    alembic_env.harness.generate_autogen_revision(
        metadata=Base1.metadata,
        message="Initial complex table"
    )
    alembic_env.harness.upgrade("head")

    # Verify table exists
    with sr_engine.connect() as conn:
        inspector = inspect(conn)
        assert inspector.has_table("complex_comparison_test")

    # 2. Create modified version with subtle but important differences
    Base2 = declarative_base()

    class ComplexTable2(Base2):
        __tablename__ = "complex_comparison_test"
        id = Column(INTEGER, primary_key=True)

        # CHANGE 1: Modify nested STRUCT field type (age: INTEGER -> STRING)
        user_data = Column(STRUCT(
            profile=STRUCT(
                name=VARCHAR(100),
                age=STRING,  # Changed from INTEGER to STRING
                active=BOOLEAN
            ),
            preferences=MAP(STRING, VARCHAR(50)),
            tags=ARRAY(STRING),
            scores=ARRAY(DECIMAL(8, 2))
        ))

        # CHANGE 2: Modify ARRAY item type (VARCHAR(30) -> VARCHAR(50))
        simple_tags = Column(ARRAY(VARCHAR(50)))  # Changed from VARCHAR(30) to VARCHAR(50)

        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    # 3. Generate migration for the changes
    logger.info("Generating migration for complex type changes...")
    alembic_env.harness.generate_autogen_revision(
        metadata=Base2.metadata,
        message="Modify complex types"
    )

    # 4. Check that migration was actually generated (not empty)
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "modify_complex_types")

    # 5. Verify that the migration contains actual changes
    # The script should NOT be empty - it should contain operations to handle the type changes
    assert "def upgrade(" in script_content
    assert "def downgrade(" in script_content

    # The migration should contain some kind of operation
    # Since StarRocks doesn't support direct column type changes for complex types,
    # the migration might contain warnings or custom operations
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert upgrade_content is not None
    upgrade_content = upgrade_content.strip()

    # The upgrade section should not be empty (should contain at least a pass or some operation)
    # If it only contains comments and pass, that might indicate the comparison logic isn't working
    non_comment_lines = ScriptContentParser.extract_non_comment_lines(upgrade_content)

    # We expect at least some kind of operation or acknowledgment of the change
    # This could be a warning comment, a custom operation, or an error indication
    assert len(non_comment_lines) > 0, \
        f"Migration should contain some operations or acknowledgments of changes, got: {upgrade_content}"

    # If the only non-comment line is 'pass', that might indicate incomplete comparison logic
    if len(non_comment_lines) == 1 and non_comment_lines[0].strip() == 'pass':
        logger.error("Migration only contains 'pass' - this might indicate incomplete type comparison logic")
        assert False, "Migration should contain some operations or acknowledgments of changes"

    assert 'existing_type=STRUCT' in upgrade_content
    assert 'type_=STRUCT' in upgrade_content
    assert 'existing_type=ARRAY' in upgrade_content
    assert 'type_=ARRAY' in upgrade_content

    logger.info("Complex type comparison test completed successfully")


def test_array_item_type_changes_detected(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """
    Test that changes in ARRAY item types are properly detected.
    """

    # 1. Initial table with ARRAY types
    Base1 = declarative_base()

    class ArrayTestTable1(Base1):
        __tablename__ = "array_change_test"
        id = Column(INTEGER, primary_key=True)
        simple_array = Column(ARRAY(INTEGER))
        string_array = Column(ARRAY(VARCHAR(20)))

        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    alembic_env.harness.generate_autogen_revision(
        metadata=Base1.metadata,
        message="Initial array table"
    )
    alembic_env.harness.upgrade("head")

    # 2. Modified version with different array item types
    Base2 = declarative_base()

    class ArrayTestTable2(Base2):
        __tablename__ = "array_change_test"
        id = Column(INTEGER, primary_key=True)
        simple_array = Column(ARRAY(STRING))  # Changed from INTEGER to STRING
        string_array = Column(ARRAY(VARCHAR(50)))  # Changed from VARCHAR(20) to VARCHAR(50)

        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    # 3. Generate migration
    alembic_env.harness.generate_autogen_revision(
        metadata=Base2.metadata,
        message="Change array item types"
    )

    # 4. Verify migration was generated
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "change_array_item_types")

    # Verify it's not an empty migration
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert upgrade_content is not None
    upgrade_content = upgrade_content.strip()
    non_comment_lines = ScriptContentParser.extract_non_comment_lines(upgrade_content)

    assert len(non_comment_lines) > 0, \
        "Array type change migration should contain operations"
    assert 'existing_type=ARRAY' in upgrade_content
    assert 'type_=ARRAY' in upgrade_content


def test_map_key_value_type_changes_detected(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """
    Test that changes in MAP key/value types are properly detected.
    """

    # 1. Initial table with MAP types
    Base1 = declarative_base()

    class MapTestTable1(Base1):
        __tablename__ = "map_change_test"
        id = Column(INTEGER, primary_key=True)
        simple_map = Column(MAP(STRING, INTEGER))
        complex_map = Column(MAP(VARCHAR(30), DECIMAL(8, 2)))

        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    alembic_env.harness.generate_autogen_revision(
        metadata=Base1.metadata,
        message="Initial map table"
    )
    alembic_env.harness.upgrade("head")

    # 2. Modified version with different MAP key/value types
    Base2 = declarative_base()

    class MapTestTable2(Base2):
        __tablename__ = "map_change_test"
        id = Column(INTEGER, primary_key=True)
        simple_map = Column(MAP(STRING, STRING))  # Changed value type from INTEGER to STRING
        complex_map = Column(MAP(VARCHAR(50), DECIMAL(10, 3)))  # Changed both key length and value precision/scale

        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    # 3. Generate migration
    alembic_env.harness.generate_autogen_revision(
        metadata=Base2.metadata,
        message="Change map types"
    )

    # 4. Verify migration was generated
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "change_map_types")

    # Verify it's not an empty migration
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert upgrade_content is not None
    upgrade_content = upgrade_content.strip()

    non_comment_lines = ScriptContentParser.extract_non_comment_lines(upgrade_content)

    assert len(non_comment_lines) > 0, \
        "MAP type change migration should contain operations"
    assert 'existing_type=MAP' in upgrade_content
    assert 'type_=MAP' in upgrade_content


def test_struct_field_changes_detected(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """
    Test that changes in STRUCT field types are properly detected.
    """

    # 1. Initial table with STRUCT types
    Base1 = declarative_base()

    class StructTestTable1(Base1):
        __tablename__ = "struct_change_test"
        id = Column(INTEGER, primary_key=True)
        user_info = Column(STRUCT(
            name=VARCHAR(50),
            age=INTEGER,
            active=BOOLEAN
        ))

        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    alembic_env.harness.generate_autogen_revision(
        metadata=Base1.metadata,
        message="Initial struct table"
    )
    alembic_env.harness.upgrade("head")

    # 2. Modified version with different STRUCT field types
    Base2 = declarative_base()

    class StructTestTable2(Base2):
        __tablename__ = "struct_change_test"
        id = Column(INTEGER, primary_key=True)
        user_info = Column(STRUCT(
            name=VARCHAR(100),  # Changed from VARCHAR(50) to VARCHAR(100)
            age=STRING,         # Changed from INTEGER to STRING
            active=TINYINT(1)   # Changed from BOOLEAN to TINYINT(1) - should be equivalent
        ))

        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    # 3. Generate migration
    alembic_env.harness.generate_autogen_revision(
        metadata=Base2.metadata,
        message="Change struct fields"
    )

    # 4. Verify migration was generated
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "change_struct_fields")

    # The migration should detect the VARCHAR length change and INTEGER->STRING change
    # but should NOT detect the BOOLEAN->TINYINT(1) change due to equivalence rules
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert upgrade_content is not None
    upgrade_content = upgrade_content.strip()

    non_comment_lines = ScriptContentParser.extract_non_comment_lines(upgrade_content)

    # Should contain operations for the real changes (name length, age type)
    assert len(non_comment_lines) > 0, \
        "STRUCT field change migration should contain operations for real changes"
    assert 'existing_type=STRUCT' in upgrade_content
    assert 'type_=STRUCT' in upgrade_content

def test_special_equivalence_rules_work_correctly(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """
    Test that special equivalence rules (BOOLEAN<->TINYINT(1), STRING<->VARCHAR(65533)) work correctly.
    This ensures that equivalent types don't generate unnecessary migrations.
    """

    # 1. Initial table with one form of equivalent types
    Base1 = declarative_base()

    class EquivalenceTestTable1(Base1):
        __tablename__ = "equivalence_test"
        id = Column(INTEGER, primary_key=True)
        flag_col = Column(BOOLEAN)
        text_col = Column(STRING)
        array_flag = Column(ARRAY(BOOLEAN))
        map_text = Column(MAP(STRING, BOOLEAN))
        struct_mixed = Column(STRUCT(
            flag=BOOLEAN,
            text_value=STRING
        ))

        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    alembic_env.harness.generate_autogen_revision(
        metadata=Base1.metadata,
        message="Initial equivalence table"
    )
    alembic_env.harness.upgrade("head")

    # 2. Modified version with equivalent types
    Base2 = declarative_base()

    class EquivalenceTestTable2(Base2):
        __tablename__ = "equivalence_test"
        id = Column(INTEGER, primary_key=True)
        flag_col = Column(TINYINT(1))      # Equivalent to BOOLEAN
        text_col = Column(VARCHAR(65533))  # Equivalent to STRING
        array_flag = Column(ARRAY(TINYINT(1)))  # Array of equivalent type
        map_text = Column(MAP(VARCHAR(65533), TINYINT(1)))  # Map with equivalent types
        struct_mixed = Column(STRUCT(
            flag=TINYINT(1),      # Equivalent to BOOLEAN
            text_value=VARCHAR(65533)   # Equivalent to STRING
        ))

        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    # 3. Generate migration - this should be empty or minimal
    alembic_env.harness.generate_autogen_revision(
        metadata=Base2.metadata,
        message="Test equivalence rules"
    )

    # 4. Check the migration
    versions_dir = alembic_env.root_path / "alembic/versions"
    scripts = list(versions_dir.glob("*test_equivalence_rules.py"))
    assert len(scripts) == 1, "Migration script should be generated (even if empty)"

    script_content = scripts[0].read_text()
    logger.debug("Equivalence test migration: %s", script_content)

    # The migration should be essentially empty since all changes are equivalent types
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert upgrade_content is not None
    upgrade_content = upgrade_content.strip()

    non_comment_lines = ScriptContentParser.extract_non_comment_lines(upgrade_content)
    # For equivalent types, the migration should be empty or contain only 'pass'
    assert len(non_comment_lines) <= 1, \
        f"Equivalent type changes should not generate operations, got: {non_comment_lines}"

    if len(non_comment_lines) == 1:
        assert non_comment_lines[0].strip() == 'pass', \
            f"Only 'pass' should be present for equivalent types, got: {non_comment_lines[0]}"
    else:
        assert False, "Equivalent type changes should not generate operations, except for 'pass'."

    logger.info("Special equivalence rules test completed successfully")
