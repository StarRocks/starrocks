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

import re
from unittest.mock import Mock

from alembic.autogenerate.api import AutogenContext
import pytest

from starrocks.alembic.ops import (
    AlterTableDistributionOp,
    AlterTableOrderOp,
    AlterTablePropertiesOp,
)
from starrocks.alembic.render import (
    _render_alter_table_distribution,
    _render_alter_table_order,
    _render_alter_table_properties,
    render_column_type,
)
from starrocks.datatype import (
    ARRAY,
    BIGINT,
    BINARY,
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
    PERCENTILE,
    SMALLINT,
    STRING,
    STRUCT,
    TINYINT,
    VARBINARY,
    VARCHAR,
)


def _normalize_py_call(s: str) -> str:
    # strip whitespace and collapse multiple spaces, ignore line breaks
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r" , ", ", ", s)
    s = re.sub(r"\( ", "(", s)
    s = re.sub(r" \)", ")", s)
    s = re.sub(r"\[ ", "[", s)
    s = re.sub(r" \]", "]", s)
    return s


class TestTableRendering:
    def test_render_alter_table_distribution(self):
        ctx = Mock()
        op = AlterTableDistributionOp("t1", "HASH(k1, k2)", buckets=10, schema="s1")
        rendered = _render_alter_table_distribution(ctx, op)
        expected = "op.alter_table_distribution('t1', 'HASH(k1, k2)', buckets=10, schema='s1')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

        # No schema
        op = AlterTableDistributionOp("t1", "HASH(k1, k2)", buckets=10, schema=None)
        rendered = _render_alter_table_distribution(ctx, op)
        expected = "op.alter_table_distribution('t1', 'HASH(k1, k2)', buckets=10)"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

        # No buckets
        op = AlterTableDistributionOp("t1", "RANDOM", buckets=None, schema="s1")
        rendered = _render_alter_table_distribution(ctx, op)
        expected = "op.alter_table_distribution('t1', 'RANDOM', schema='s1')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

        # Buckets is 0
        op = AlterTableDistributionOp("t1", "RANDOM", buckets=0, schema="s1")
        rendered = _render_alter_table_distribution(ctx, op)
        expected = "op.alter_table_distribution('t1', 'RANDOM', buckets=0, schema='s1')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

        # No buckets and no schema
        op = AlterTableDistributionOp("t1", "RANDOM", buckets=None, schema=None)
        rendered = _render_alter_table_distribution(ctx, op)
        expected = "op.alter_table_distribution('t1', 'RANDOM')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_alter_table_order(self):
        ctx = Mock()
        op = AlterTableOrderOp("t1", "k1, k2", schema="s1")
        rendered = _render_alter_table_order(ctx, op)
        expected = "op.alter_table_order('t1', 'k1, k2', schema='s1')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

        op = AlterTableOrderOp("t1", "k1", schema=None)
        rendered = _render_alter_table_order(ctx, op)
        assert rendered == "op.alter_table_order('t1', 'k1')"

    def test_render_alter_table_properties(self):
        ctx = Mock()
        op = AlterTablePropertiesOp("t1", {"replication_num": "1", "storage_medium": "SSD"}, schema="s1")
        rendered = _render_alter_table_properties(ctx, op)
        rendered = _normalize_py_call(rendered)
        # repr() on dict is order-sensitive in older pythons, so check keys/values
        assert _normalize_py_call("op.alter_table_properties('t1', ") in rendered
        assert "'replication_num': '1'" in rendered
        assert "'storage_medium': 'SSD'" in rendered
        assert "schema='s1'" in rendered

        # With properties and no schema
        op = AlterTablePropertiesOp("t1", {"replication_num": "1"}, schema=None)
        rendered = _render_alter_table_properties(ctx, op)
        assert "op.alter_table_properties('t1', {'replication_num': '1'})" in rendered

        # With schema and no properties
        op = AlterTablePropertiesOp("t1", {}, schema="s1")
        rendered = _render_alter_table_properties(ctx, op)
        expected = "op.alter_table_properties('t1', {}, schema='s1')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

        # With no properties and no schema
        op = AlterTablePropertiesOp("t1", {}, schema=None)
        rendered = _render_alter_table_properties(ctx, op)
        assert "op.alter_table_properties('t1', {})" in rendered


# Test data for type rendering
BASIC_RENDER_TEST_CASES = [
    # (type_instance, expected_render)
    (INTEGER(), "INTEGER()"),
    (VARCHAR(255), "VARCHAR(length=255)"),
    (DECIMAL(10, 2), "DECIMAL(precision=10, scale=2)"),
    (BOOLEAN(), "BOOLEAN()"),
    (TINYINT(), "TINYINT()"),
    (TINYINT(1), "TINYINT(display_width=1)"),
    (SMALLINT(), "SMALLINT()"),
    (BIGINT(), "BIGINT()"),
    (LARGEINT(), "LARGEINT()"),
    (FLOAT(), "FLOAT()"),
    (DOUBLE(), "DOUBLE(asdecimal=True)"),
    (CHAR(10), "CHAR(length=10)"),
    (STRING(), "STRING()"),
    (BINARY(10), "BINARY(length=10)"),
    (VARBINARY(255), "VARBINARY(length=255)"),
    (DATE(), "DATE()"),
    (DATETIME(), "DATETIME()"),
    (HLL(), "HLL()"),
    (BITMAP(), "BITMAP()"),
    (PERCENTILE(), "PERCENTILE()"),
    (JSON(), "JSON()"),
]

COMPLEX_RENDER_TEST_CASES = [
    # ARRAY types
    (ARRAY(INTEGER), "ARRAY(INTEGER())"),
    (ARRAY(VARCHAR(50)), "ARRAY(VARCHAR(length=50))"),
    (ARRAY(ARRAY(STRING)), "ARRAY(ARRAY(STRING()))"),
    (ARRAY(DECIMAL(10, 2)), "ARRAY(DECIMAL(precision=10, scale=2))"),

    # MAP types
    (MAP(STRING, INTEGER), "MAP(STRING(), INTEGER())"),
    (MAP(VARCHAR(50), DOUBLE), "MAP(VARCHAR(length=50), DOUBLE(asdecimal=True))"),
    (MAP(STRING, MAP(INTEGER, STRING)), "MAP(STRING(), MAP(INTEGER(), STRING()))"),
    (MAP(STRING, DECIMAL(8, 2)), "MAP(STRING(), DECIMAL(precision=8, scale=2))"),

    # STRUCT types
    (STRUCT(name=STRING, age=INTEGER), "STRUCT(name=STRING(), age=INTEGER())"),
    (STRUCT(id=INTEGER, name=VARCHAR(100), active=BOOLEAN),
     "STRUCT(id=INTEGER(), name=VARCHAR(length=100), active=BOOLEAN())"),
    (STRUCT(user=STRUCT(id=INTEGER, name=STRING), metadata=MAP(STRING, STRING)),
     "STRUCT(user=STRUCT(id=INTEGER(), name=STRING()), metadata=MAP(STRING(), STRING()))"),
]

EDGE_CASE_RENDER_TEST_CASES = [
    (VARCHAR(65533), "VARCHAR(length=65533)"),
    (DECIMAL(38, 18), "DECIMAL(precision=38, scale=18)"),
    (STRUCT(id=INTEGER), "STRUCT(id=INTEGER())"),
    (ARRAY(ARRAY(ARRAY(INTEGER))), "ARRAY(ARRAY(ARRAY(INTEGER())))"),
]

NON_STARROCKS_TYPE_TEST_CASES = [
    # These should return False
    ('column', INTEGER()),  # Wrong parameter type
    ('type', "not_a_type"),  # Not a type object
]


class TestDataTypeRendering:
    """Test rendering of StarRocks data types in Alembic autogenerate"""

    def _create_mock_autogen_context(self):
        """Create a mock AutogenContext for testing"""
        ctx = Mock()
        ctx.as_sql = False  # Required by AutogenContext constructor
        ctx.opts = {}  # Required by AutogenContext constructor
        ctx.script = None  # Required by AutogenContext constructor
        ctx.imports = set()
        return AutogenContext(ctx, {}, None, True)

    @pytest.mark.parametrize("type_instance, expected_render", BASIC_RENDER_TEST_CASES)
    def test_render_basic_types(self, type_instance, expected_render):
        """Test rendering of basic data types"""
        autogen_context = self._create_mock_autogen_context()
        result = render_column_type('type', type_instance, autogen_context)
        assert result == expected_render
        assert "from starrocks import *" in autogen_context.imports

    @pytest.mark.parametrize("type_instance, expected_render", COMPLEX_RENDER_TEST_CASES)
    def test_render_complex_types(self, type_instance, expected_render):
        """Test rendering of complex data types"""
        autogen_context = self._create_mock_autogen_context()
        result = render_column_type('type', type_instance, autogen_context)
        assert result == expected_render

    @pytest.mark.parametrize("type_instance, expected_render", EDGE_CASE_RENDER_TEST_CASES)
    def test_render_edge_cases(self, type_instance, expected_render):
        """Test edge cases in type rendering"""
        autogen_context = self._create_mock_autogen_context()
        result = render_column_type('type', type_instance, autogen_context)
        assert result == expected_render

    def test_render_deeply_nested_complex_type(self):
        """Test rendering of deeply nested complex data types"""
        autogen_context = self._create_mock_autogen_context()

        # Test ARRAY of MAP
        result = render_column_type('type', ARRAY(MAP(STRING, INTEGER)), autogen_context)
        assert result == "ARRAY(MAP(STRING(), INTEGER()))"

        # Test MAP of ARRAY
        result = render_column_type('type', MAP(STRING, ARRAY(INTEGER)), autogen_context)
        assert result == "MAP(STRING(), ARRAY(INTEGER()))"

        # Test STRUCT with ARRAY and MAP
        result = render_column_type('type', STRUCT(
            id=INTEGER,
            tags=ARRAY(STRING),
            metadata=MAP(STRING, STRUCT(value=STRING, count=INTEGER))
        ), autogen_context)
        expected = "STRUCT(id=INTEGER(), tags=ARRAY(STRING()), metadata=MAP(STRING(), STRUCT(value=STRING(), count=INTEGER())))"
        assert result == expected

    def test_render_non_starrocks_types(self):
        """Test that non-StarRocks types are not handled by our renderer"""
        from sqlalchemy import Integer as SQLAInteger, String as SQLAString

        autogen_context = self._create_mock_autogen_context()

        # Test SQLAlchemy String (should return False)
        result = render_column_type('type', SQLAString(255), autogen_context)
        assert result is False

        # Test SQLAlchemy Integer (should return False)
        result = render_column_type('type', SQLAInteger(), autogen_context)
        assert result is False

    def test_render_non_type_objects(self):
        """Test that non-type objects are not handled by our renderer"""
        autogen_context = self._create_mock_autogen_context()

        # Test with non-type parameter (should return False)
        result = render_column_type('column', INTEGER(), autogen_context)
        assert result is False

        # Test with string object (should return False)
        result = render_column_type('type', "not_a_type", autogen_context)
        assert result is False

    def test_imports_added_correctly(self):
        """Test that proper imports are added to autogen context"""
        autogen_context = self._create_mock_autogen_context()

        # Initially no imports
        assert len(autogen_context.imports) == 0

        # Render a StarRocks type
        render_column_type('type', INTEGER(), autogen_context)

        # Check that the import was added
        assert "from starrocks import *" in autogen_context.imports
        assert len(autogen_context.imports) == 1

        # Render another type, should not add duplicate import
        render_column_type('type', VARCHAR(50), autogen_context)
        assert len(autogen_context.imports) == 1
