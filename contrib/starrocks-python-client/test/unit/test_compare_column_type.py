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

import pytest
from sqlalchemy import Column

from starrocks.alembic.starrocks import StarRocksImpl
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
from starrocks.dialect import StarRocksDialect


def run_compare(type1, type2):
    """
    Helper function to compare two types using StarRocksImpl.compare_type.
    The method under test is `StarRocksImpl.compare_type`.

    Args:
        type1: The SQLAlchemy type object for the inspector column.
        type2: The SQLAlchemy type object for the metadata column.

    Returns:
        True if types are considered different, False otherwise.
    """
    inspector_column = Column("test_col", type1)
    metadata_column = Column("test_col", type2)

    dialect = StarRocksDialect()
    impl = StarRocksImpl(
        dialect=dialect,
        connection=None,
        as_sql=False,
        transactional_ddl=False,
        output_buffer=None,
        context_opts={},
    )

    # In the implementation, metadata_column is the source of truth,
    # and inspector_column is from the database.
    return impl.compare_type(inspector_column, metadata_column)


# 1. Simple type comparison
simple_type_params = [
    # 1.1 Completely identical
    pytest.param(INTEGER(), INTEGER(), False, id="INTEGER vs INTEGER (same)"),
    pytest.param(VARCHAR(10), VARCHAR(10), False, id="VARCHAR(10) vs VARCHAR(10) (same)"),
    pytest.param(DECIMAL(10, 2), DECIMAL(10, 2), False, id="DECIMAL(10, 2) vs DECIMAL(10, 2) (same)"),
    pytest.param(TINYINT(), TINYINT(), False, id="TINYINT vs TINYINT (same)"),
    pytest.param(SMALLINT(), SMALLINT(), False, id="SMALLINT vs SMALLINT (same)"),
    pytest.param(LARGEINT(), LARGEINT(), False, id="LARGEINT vs LARGEINT (same)"),
    pytest.param(FLOAT(), FLOAT(), False, id="FLOAT vs FLOAT (same)"),
    pytest.param(DOUBLE(), DOUBLE(), False, id="DOUBLE vs DOUBLE (same)"),
    pytest.param(CHAR(10), CHAR(10), False, id="CHAR(10) vs CHAR(10) (same)"),
    pytest.param(DATE(), DATE(), False, id="DATE vs DATE (same)"),
    pytest.param(DATETIME(), DATETIME(), False, id="DATETIME vs DATETIME (same)"),
    pytest.param(HLL(), HLL(), False, id="HLL vs HLL (same)"),
    pytest.param(BITMAP(), BITMAP(), False, id="BITMAP vs BITMAP (same)"),
    # pytest.param(PERCENTILE(), PERCENTILE(), False, id="PERCENTILE vs PERCENTILE (same)"),  # Not supported by compiler yet
    pytest.param(JSON(), JSON(), False, id="JSON vs JSON (same)"),

    # 1.2 Equivalent types (special rules)
    pytest.param(TINYINT(1), BOOLEAN(), False, id="TINYINT(1) vs BOOLEAN (equivalent)"),
    pytest.param(BOOLEAN(), TINYINT(1), False, id="BOOLEAN vs TINYINT(1) (equivalent)"),
    pytest.param(VARCHAR(65533), STRING(), False, id="VARCHAR(65533) vs STRING (equivalent)"),
    pytest.param(STRING(), VARCHAR(65533), False, id="STRING vs VARCHAR(65533) (equivalent)"),

    # 1.3 Different types
    pytest.param(INTEGER(), STRING(), True, id="INTEGER vs STRING (different)"),
    pytest.param(VARCHAR(10), VARCHAR(20), True, id="VARCHAR(10) vs VARCHAR(20) (different)"),
    pytest.param(DECIMAL(10, 2), DECIMAL(12, 4), True, id="DECIMAL(10,2) vs DECIMAL(12,4) (different)"),
    pytest.param(INTEGER(), BIGINT(), True, id="INTEGER vs BIGINT (different)"),
    pytest.param(TINYINT(), SMALLINT(), True, id="TINYINT vs SMALLINT (different)"),
    pytest.param(FLOAT(), DOUBLE(), True, id="FLOAT vs DOUBLE (different)"),
    pytest.param(CHAR(10), VARCHAR(10), True, id="CHAR(10) vs VARCHAR(10) (different)"),
    pytest.param(DATE(), DATETIME(), True, id="DATE vs DATETIME (different)"),
    pytest.param(HLL(), BITMAP(), True, id="HLL vs BITMAP (different)"),
    pytest.param(JSON(), STRING(), True, id="JSON vs STRING (different)"),

    # 1.4 Edge cases with parameters
    pytest.param(TINYINT(2), BOOLEAN(), True, id="TINYINT(2) vs BOOLEAN (not equivalent)"),
    pytest.param(VARCHAR(65532), STRING(), True, id="VARCHAR(65532) vs STRING (not equivalent)"),
    pytest.param(DECIMAL(1, 0), DECIMAL(1, 1), True, id="DECIMAL precision/scale difference"),
    pytest.param(FLOAT(10), FLOAT(20), False, id="FLOAT precision difference (ignored)"),
]

# 2. Complex type vs Simple type
complex_vs_simple_params = [
    pytest.param(INTEGER(), ARRAY(INTEGER), True, id="simple INTEGER vs complex ARRAY"),
    pytest.param(ARRAY(INTEGER), INTEGER(), True, id="complex ARRAY vs simple INTEGER"),
    pytest.param(MAP(STRING, INTEGER), STRING(), True, id="complex MAP vs simple STRING"),
    pytest.param(STRUCT(a=INTEGER), INTEGER(), True, id="complex STRUCT vs simple INTEGER"),
]

# 3. Different complex types
different_complex_types_params = [
    pytest.param(ARRAY(INTEGER), MAP(INTEGER, INTEGER), True, id="ARRAY vs MAP"),
    pytest.param(MAP(STRING, INTEGER), STRUCT(a=STRING, b=INTEGER), True, id="MAP vs STRUCT"),
]

# 4. ARRAY type comparison
array_type_params = [
    pytest.param(ARRAY(INTEGER), ARRAY(INTEGER), False, id="ARRAY<INT> vs ARRAY<INT> (same)"),
    pytest.param(ARRAY(INTEGER), ARRAY(STRING), True, id="ARRAY<INT> vs ARRAY<STR> (different item type)"),
    pytest.param(ARRAY(ARRAY(INTEGER)), ARRAY(ARRAY(INTEGER)), False, id="Nested ARRAY (same)"),
    pytest.param(ARRAY(ARRAY(INTEGER)), ARRAY(ARRAY(STRING)), True, id="Nested ARRAY (different item type)"),
    pytest.param(ARRAY(MAP(STRING, INTEGER)), ARRAY(MAP(STRING, STRING)), True, id="Nested ARRAY with MAP (deep difference)"),
    # Test special equivalence cases in ARRAY item types
    pytest.param(ARRAY(TINYINT(1)), ARRAY(BOOLEAN), False, id="ARRAY item: TINYINT(1) vs BOOLEAN (equivalent)"),
    pytest.param(ARRAY(VARCHAR(65533)), ARRAY(STRING), False, id="ARRAY item: VARCHAR(65533) vs STRING (equivalent)"),
    pytest.param(ARRAY(BOOLEAN), ARRAY(TINYINT(1)), False, id="ARRAY item: BOOLEAN vs TINYINT(1) (equivalent)"),
]

# 5. MAP type comparison
map_type_params = [
    pytest.param(MAP(STRING, INTEGER), MAP(STRING, INTEGER), False, id="MAP<STR,INT> vs MAP<STR,INT> (same)"),
    pytest.param(MAP(INTEGER, STRING), MAP(INTEGER, STRING), False, id="MAP<INT,STR> vs MAP<INT,STR> (same)"),
    pytest.param(MAP(STRING, INTEGER), MAP(INTEGER, INTEGER), True, id="MAP (different key type: STR vs INT)"),
    pytest.param(MAP(INTEGER, STRING), MAP(STRING, STRING), True, id="MAP (different key type: INT vs STR)"),
    pytest.param(MAP(STRING, INTEGER), MAP(STRING, STRING), True, id="MAP (different value type)"),
    pytest.param(MAP(STRING, ARRAY(INTEGER)), MAP(STRING, ARRAY(INTEGER)), False, id="Nested MAP with ARRAY (same)"),
    pytest.param(MAP(STRING, ARRAY(INTEGER)), MAP(STRING, ARRAY(STRING)), True, id="Nested MAP with ARRAY (deep difference)"),
    pytest.param(MAP(STRING, STRUCT(a=INTEGER)), MAP(STRING, STRUCT(a=STRING)), True, id="Nested MAP with STRUCT (deep difference)"),
    # Test special equivalence cases in MAP key/value types
    pytest.param(MAP(VARCHAR(65533), INTEGER), MAP(STRING(), INTEGER), False, id="MAP key: VARCHAR(65533) vs STRING (equivalent)"),
    pytest.param(MAP(STRING(), INTEGER), MAP(VARCHAR(65533), INTEGER), False, id="MAP key: STRING vs VARCHAR(65533) (equivalent)"),
    pytest.param(MAP(STRING, TINYINT(1)), MAP(STRING, BOOLEAN), False, id="MAP value: TINYINT(1) vs BOOLEAN (equivalent)"),
    pytest.param(MAP(TINYINT(1), STRING), MAP(BOOLEAN, STRING), False, id="MAP key: TINYINT(1) vs BOOLEAN (equivalent)"),
]

# 6. STRUCT type comparison
struct_type_params = [
    pytest.param(STRUCT(a=INTEGER, b=STRING), STRUCT(a=INTEGER, b=STRING), False, id="STRUCT (same)"),
    pytest.param(STRUCT(a=INTEGER, b=STRING), STRUCT(b=STRING, a=INTEGER), True, id="STRUCT (different field order)"),
    pytest.param(STRUCT(a=INTEGER), STRUCT(a=INTEGER, b=STRING), True, id="STRUCT (different field count)"),
    pytest.param(STRUCT(a=INTEGER), STRUCT(c=INTEGER), True, id="STRUCT (different field name)"),
    pytest.param(STRUCT(a=INTEGER), STRUCT(a=STRING), True, id="STRUCT (different field type)"),
    pytest.param(STRUCT(a=INTEGER, b=MAP(STRING, INTEGER)), STRUCT(a=INTEGER, b=MAP(STRING, INTEGER)), False, id="Nested STRUCT with MAP (same)"),
    pytest.param(STRUCT(a=INTEGER, b=MAP(STRING, INTEGER)), STRUCT(a=INTEGER, b=MAP(STRING, STRING)), True, id="Nested STRUCT with MAP (deep difference)"),
    # Test special equivalence cases in STRUCT fields
    pytest.param(STRUCT(flag=TINYINT(1)), STRUCT(flag=BOOLEAN), False, id="STRUCT field: TINYINT(1) vs BOOLEAN (equivalent)"),
    pytest.param(STRUCT(text=VARCHAR(65533)), STRUCT(text=STRING), False, id="STRUCT field: VARCHAR(65533) vs STRING (equivalent)"),
    pytest.param(STRUCT(flag=BOOLEAN), STRUCT(flag=TINYINT(1)), False, id="STRUCT field: BOOLEAN vs TINYINT(1) (equivalent)"),
]

# 7. Complex nested types (ARRAY, MAP, STRUCT combined)
complex_nested_params = [
    pytest.param(
        ARRAY(MAP(STRING, STRUCT(a=INTEGER, b=ARRAY(STRING)))),
        ARRAY(MAP(STRING, STRUCT(a=INTEGER, b=ARRAY(STRING)))),
        False,
        id="ARRAY<MAP<STR,STRUCT<INT,ARRAY<STR>>>> (same)"
    ),
    pytest.param(
        ARRAY(MAP(STRING, STRUCT(a=INTEGER, b=ARRAY(STRING)))),
        ARRAY(MAP(STRING, STRUCT(a=INTEGER, b=ARRAY(INTEGER)))),
        True,
        id="ARRAY<MAP<STR,STRUCT<INT,ARRAY<STR>>>> vs ARRAY<MAP<STR,STRUCT<INT,ARRAY<INT>>>> (deep difference)"
    ),
    pytest.param(
        MAP(INTEGER, ARRAY(STRUCT(a=STRING, b=MAP(STRING, INTEGER)))),
        MAP(INTEGER, ARRAY(STRUCT(a=STRING, b=MAP(STRING, INTEGER)))),
        False,
        id="MAP<INT,ARRAY<STRUCT<STR,MAP<STR,INT>>>> (same)"
    ),
    pytest.param(
        MAP(INTEGER, ARRAY(STRUCT(a=STRING, b=MAP(STRING, INTEGER)))),
        MAP(INTEGER, ARRAY(STRUCT(a=STRING, b=MAP(INTEGER, INTEGER)))),
        True,
        id="MAP<INT,ARRAY<STRUCT<STR,MAP<STR,INT>>>> vs MAP<INT,ARRAY<STRUCT<STR,MAP<INT,INT>>>> (deep difference)"
    ),
    pytest.param(
        STRUCT(
            id=INTEGER,
            tags=ARRAY(STRING),
            metadata=MAP(STRING, STRUCT(value=STRING, count=INTEGER))
        ),
        STRUCT(
            id=INTEGER,
            tags=ARRAY(STRING),
            metadata=MAP(STRING, STRUCT(value=STRING, count=INTEGER))
        ),
        False,
        id="STRUCT<INT,ARRAY<STR>,MAP<STR,STRUCT<STR,INT>>> (same)"
    ),
    pytest.param(
        STRUCT(
            id=INTEGER,
            tags=ARRAY(STRING),
            metadata=MAP(STRING, STRUCT(value=STRING, count=INTEGER))
        ),
        STRUCT(
            id=INTEGER,
            tags=ARRAY(INTEGER),
            metadata=MAP(STRING, STRUCT(value=STRING, count=INTEGER))
        ),
        True,
        id="STRUCT<INT,ARRAY<STR>,MAP<STR,STRUCT<STR,INT>>> vs STRUCT<INT,ARRAY<INT>,MAP<STR,STRUCT<STR,INT>>> (deep difference)"
    ),
]


class TestCompareColumnType:
    """Test suite for StarRocksImpl.compare_type."""

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", simple_type_params)
    def test_simple_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of simple (non-nested) data types."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", complex_vs_simple_params)
    def test_complex_vs_simple_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison between complex and simple types."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", different_complex_types_params)
    def test_different_complex_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison between different kinds of complex types (e.g., ARRAY vs MAP)."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", array_type_params)
    def test_array_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of ARRAY types, including nested scenarios."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", map_type_params)
    def test_map_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of MAP types, including nested scenarios."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", struct_type_params)
    def test_struct_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of STRUCT types, including nested scenarios."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", complex_nested_params)
    def test_complex_nested_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of deeply nested complex types combining ARRAY, MAP, and STRUCT."""
        assert run_compare(inspector_type, metadata_type) == is_different

    def test_real_world_scenarios(self):
        """Test real-world complex type scenarios that might be problematic."""

        # E-commerce product catalog scenario
        product_v1 = STRUCT(
            id=BIGINT,
            name=VARCHAR(255),
            price=DECIMAL(10, 2),
            categories=ARRAY(VARCHAR(50)),
            attributes=MAP(VARCHAR(100), STRING),
            inventory=STRUCT(
                warehouse_id=INTEGER,
                quantity=INTEGER,
                last_updated=DATETIME
            )
        )

        # Same structure
        product_v2 = STRUCT(
            id=BIGINT,
            name=VARCHAR(255),
            price=DECIMAL(10, 2),
            categories=ARRAY(VARCHAR(50)),
            attributes=MAP(VARCHAR(100), STRING),
            inventory=STRUCT(
                warehouse_id=INTEGER,
                quantity=INTEGER,
                last_updated=DATETIME
            )
        )

        # Should be the same
        assert run_compare(product_v1, product_v2) is False

        # Subtle difference: quantity INTEGER -> BIGINT
        product_v3 = STRUCT(
            id=BIGINT,
            name=VARCHAR(255),
            price=DECIMAL(10, 2),
            categories=ARRAY(VARCHAR(50)),
            attributes=MAP(VARCHAR(100), STRING),
            inventory=STRUCT(
                warehouse_id=INTEGER,
                quantity=BIGINT,  # Changed from INTEGER
                last_updated=DATETIME
            )
        )

        # Should be different
        assert run_compare(product_v1, product_v3) is True

    def test_edge_case_parameters(self):
        """Test edge cases with type parameters that should be carefully handled."""

        # VARCHAR length edge cases
        assert run_compare(VARCHAR(1), VARCHAR(1)) is False
        assert run_compare(VARCHAR(1), VARCHAR(2)) is True
        assert run_compare(VARCHAR(65533), VARCHAR(65533)) is False

        # DECIMAL precision/scale edge cases
        assert run_compare(DECIMAL(1, 0), DECIMAL(1, 0)) is False
        assert run_compare(DECIMAL(1, 0), DECIMAL(1, 1)) is True
        assert run_compare(DECIMAL(38, 18), DECIMAL(38, 18)) is False
        assert run_compare(DECIMAL(38, 18), DECIMAL(38, 17)) is True

        # TINYINT display width edge cases
        assert run_compare(TINYINT(1), TINYINT(1)) is False
        assert run_compare(TINYINT(1), TINYINT(2)) is False  # no print width in SR?

    def test_special_equivalence_in_deep_nesting(self):
        """Test that special equivalence rules work correctly in deeply nested structures."""

        # Deep nesting with BOOLEAN <-> TINYINT(1) equivalence
        deep1 = ARRAY(MAP(STRING, STRUCT(
            flags=ARRAY(BOOLEAN),
            metadata=MAP(VARCHAR(65533), TINYINT(1))
        )))

        deep2 = ARRAY(MAP(STRING, STRUCT(
            flags=ARRAY(TINYINT(1)),
            metadata=MAP(STRING, BOOLEAN)
        )))

        # Should be equivalent due to special rules
        assert run_compare(deep1, deep2) is False

        # But this should be different (TINYINT(2) != BOOLEAN)
        deep3 = ARRAY(MAP(STRING, STRUCT(
            flags=ARRAY(TINYINT(2)),  # Not equivalent to BOOLEAN
            metadata=MAP(STRING, BOOLEAN)
        )))

        assert run_compare(deep1, deep3) is True

    def test_case_sensitivity_in_struct_fields(self):
        """Test case sensitivity behavior in STRUCT field names."""

        # Different case in field names
        struct1 = STRUCT(Name=STRING, Age=INTEGER)
        struct2 = STRUCT(name=STRING, age=INTEGER)

        # Current implementation treats names as case-sensitive, so they differ
        # TODO: we may need to implement case-insensitive comparison
        result = run_compare(struct1, struct2)
        assert result is True
