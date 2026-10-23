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

from sqlalchemy import Column, testing
from sqlalchemy.testing import fixtures

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
    ("INTEGER vs INTEGER (same)", INTEGER(), INTEGER(), False),
    ("VARCHAR(10) vs VARCHAR(10) (same)", VARCHAR(10), VARCHAR(10), False),
    ("DECIMAL(10, 2) vs DECIMAL(10, 2) (same)", DECIMAL(10, 2), DECIMAL(10, 2), False),
    ("TINYINT vs TINYINT (same)", TINYINT(), TINYINT(), False),
    ("SMALLINT vs SMALLINT (same)", SMALLINT(), SMALLINT(), False),
    ("LARGEINT vs LARGEINT (same)", LARGEINT(), LARGEINT(), False),
    ("FLOAT vs FLOAT (same)", FLOAT(), FLOAT(), False),
    ("DOUBLE vs DOUBLE (same)", DOUBLE(), DOUBLE(), False),
    ("CHAR(10) vs CHAR(10) (same)", CHAR(10), CHAR(10), False),
    ("DATE vs DATE (same)", DATE(), DATE(), False),
    ("DATETIME vs DATETIME (same)", DATETIME(), DATETIME(), False),
    ("HLL vs HLL (same)", HLL(), HLL(), False),
    ("BITMAP vs BITMAP (same)", BITMAP(), BITMAP(), False),
    # ("PERCENTILE vs PERCENTILE (same)", PERCENTILE(), PERCENTILE(), False),  # Not supported by compiler yet
    ("JSON vs JSON (same)", JSON(), JSON(), False),

    # 1.2 Equivalent types (special rules)
    ("TINYINT(1) vs BOOLEAN (equivalent)", TINYINT(1), BOOLEAN(), False),
    ("BOOLEAN vs TINYINT(1) (equivalent)", BOOLEAN(), TINYINT(1), False),
    ("VARCHAR(65533) vs STRING (equivalent)", VARCHAR(65533), STRING(), False),
    ("STRING vs VARCHAR(65533) (equivalent)", STRING(), VARCHAR(65533), False),

    # 1.3 Different types
    ("INTEGER vs STRING (different)", INTEGER(), STRING(), True),
    ("VARCHAR(10) vs VARCHAR(20) (different)", VARCHAR(10), VARCHAR(20), True),
    ("DECIMAL(10,2) vs DECIMAL(12,4) (different)", DECIMAL(10, 2), DECIMAL(12, 4), True),
    ("INTEGER vs BIGINT (different)", INTEGER(), BIGINT(), True),
    ("TINYINT vs SMALLINT (different)", TINYINT(), SMALLINT(), True),
    ("FLOAT vs DOUBLE (different)", FLOAT(), DOUBLE(), True),
    ("CHAR(10) vs VARCHAR(10) (different)", CHAR(10), VARCHAR(10), True),
    ("DATE vs DATETIME (different)", DATE(), DATETIME(), True),
    ("HLL vs BITMAP (different)", HLL(), BITMAP(), True),
    ("JSON vs STRING (different)", JSON(), STRING(), True),

    # 1.4 Edge cases with parameters
    ("TINYINT(2) vs BOOLEAN (not equivalent)", TINYINT(2), BOOLEAN(), True),
    ("TINYINT(4) vs BOOLEAN (not equivalent)", TINYINT(4), BOOLEAN(), True),
    ("BOOLEAN vs TINYINT(2) (not equivalent)", BOOLEAN(), TINYINT(2), True),
    ("BOOLEAN vs TINYINT(4) (not equivalent)", BOOLEAN(), TINYINT(4), True),
    ("VARCHAR(65532) vs STRING (not equivalent)", VARCHAR(65532), STRING(), True),
    ("DECIMAL precision/scale difference", DECIMAL(1, 0), DECIMAL(1, 1), True),
    ("FLOAT precision difference (ignored, StarRocks doesn't support FLOAT precision)", FLOAT(10), FLOAT(20), False),

    # 1.5 Integer types with different display_width (should be ignored)
    ("TINYINT(1) vs TINYINT(2) (display_width ignored)", TINYINT(1), TINYINT(2), False),
    ("TINYINT(2) vs TINYINT(1) (display_width ignored)", TINYINT(2), TINYINT(1), False),
    ("TINYINT() vs TINYINT(1) (display_width ignored)", TINYINT(), TINYINT(1), False),
    ("TINYINT(1) vs TINYINT() (display_width ignored)", TINYINT(1), TINYINT(), False),
    ("TINYINT(5) vs TINYINT(10) (display_width ignored)", TINYINT(5), TINYINT(10), False),
    ("SMALLINT(1) vs SMALLINT(2) (display_width ignored)", SMALLINT(1), SMALLINT(2), False),
    ("SMALLINT(2) vs SMALLINT(1) (display_width ignored)", SMALLINT(2), SMALLINT(1), False),
    ("SMALLINT() vs SMALLINT(1) (display_width ignored)", SMALLINT(), SMALLINT(1), False),
    ("SMALLINT(1) vs SMALLINT() (display_width ignored)", SMALLINT(1), SMALLINT(), False),
    ("SMALLINT(5) vs SMALLINT(10) (display_width ignored)", SMALLINT(5), SMALLINT(10), False),
    ("INTEGER(1) vs INTEGER(2) (display_width ignored)", INTEGER(1), INTEGER(2), False),
    ("INTEGER(2) vs INTEGER(1) (display_width ignored)", INTEGER(2), INTEGER(1), False),
    ("INTEGER() vs INTEGER(1) (display_width ignored)", INTEGER(), INTEGER(1), False),
    ("INTEGER(1) vs INTEGER() (display_width ignored)", INTEGER(1), INTEGER(), False),
    ("INTEGER(5) vs INTEGER(10) (display_width ignored)", INTEGER(5), INTEGER(10), False),
    ("INTEGER(11) vs INTEGER(20) (display_width ignored)", INTEGER(11), INTEGER(20), False),
    ("BIGINT(1) vs BIGINT(2) (display_width ignored)", BIGINT(1), BIGINT(2), False),
    ("BIGINT(2) vs BIGINT(1) (display_width ignored)", BIGINT(2), BIGINT(1), False),
    ("BIGINT() vs BIGINT(1) (display_width ignored)", BIGINT(), BIGINT(1), False),
    ("BIGINT(1) vs BIGINT() (display_width ignored)", BIGINT(1), BIGINT(), False),
    ("BIGINT(5) vs BIGINT(10) (display_width ignored)", BIGINT(5), BIGINT(10), False),
    ("BIGINT(11) vs BIGINT(20) (display_width ignored)", BIGINT(11), BIGINT(20), False),
]

# 2. Complex type vs Simple type
complex_vs_simple_params = [
    ("simple INTEGER vs complex ARRAY", INTEGER(), ARRAY(INTEGER), True),
    ("complex ARRAY vs simple INTEGER", ARRAY(INTEGER), INTEGER(), True),
    ("complex MAP vs simple STRING", MAP(STRING, INTEGER), STRING(), True),
    ("complex STRUCT vs simple INTEGER", STRUCT(a=INTEGER), INTEGER(), True),
]

# 3. Different complex types
different_complex_types_params = [
    ("ARRAY vs MAP", ARRAY(INTEGER), MAP(INTEGER, INTEGER), True),
    ("MAP vs STRUCT", MAP(STRING, INTEGER), STRUCT(a=STRING, b=INTEGER), True),
]

# 4. ARRAY type comparison
array_type_params = [
    ("ARRAY<INT> vs ARRAY<INT> (same)", ARRAY(INTEGER), ARRAY(INTEGER), False),
    ("ARRAY<INT> vs ARRAY<STR> (different item type)", ARRAY(INTEGER), ARRAY(STRING), True),
    ("Nested ARRAY (same)", ARRAY(ARRAY(INTEGER)), ARRAY(ARRAY(INTEGER)), False),
    ("Nested ARRAY (different item type)", ARRAY(ARRAY(INTEGER)), ARRAY(ARRAY(STRING)), True),
    ("Nested ARRAY with MAP (deep difference)", ARRAY(MAP(STRING, INTEGER)), ARRAY(MAP(STRING, STRING)), True),
    # Test special equivalence cases in ARRAY item types
    ("ARRAY item: TINYINT(1) vs BOOLEAN (equivalent)", ARRAY(TINYINT(1)), ARRAY(BOOLEAN), False),
    ("ARRAY item: VARCHAR(65533) vs STRING (equivalent)", ARRAY(VARCHAR(65533)), ARRAY(STRING), False),
    ("ARRAY item: BOOLEAN vs TINYINT(1) (equivalent)", ARRAY(BOOLEAN), ARRAY(TINYINT(1)), False),
]

# 5. MAP type comparison
map_type_params = [
    ("MAP<STR,INT> vs MAP<STR,INT> (same)", MAP(STRING, INTEGER), MAP(STRING, INTEGER), False),
    ("MAP<INT,STR> vs MAP<INT,STR> (same)", MAP(INTEGER, STRING), MAP(INTEGER, STRING), False),
    ("MAP (different key type: STR vs INT)", MAP(STRING, INTEGER), MAP(INTEGER, INTEGER), True),
    ("MAP (different key type: INT vs STR)", MAP(INTEGER, STRING), MAP(STRING, STRING), True),
    ("MAP (different value type)", MAP(STRING, INTEGER), MAP(STRING, STRING), True),
    ("Nested MAP with ARRAY (same)", MAP(STRING, ARRAY(INTEGER)), MAP(STRING, ARRAY(INTEGER)), False),
    ("Nested MAP with ARRAY (deep difference)", MAP(STRING, ARRAY(INTEGER)), MAP(STRING, ARRAY(STRING)), True),
    ("Nested MAP with STRUCT (deep difference)", MAP(STRING, STRUCT(a=INTEGER)), MAP(STRING, STRUCT(a=STRING)), True),
    # Test special equivalence cases in MAP key/value types
    ("MAP key: VARCHAR(65533) vs STRING (equivalent)", MAP(VARCHAR(65533), INTEGER), MAP(STRING(), INTEGER), False),
    ("MAP key: STRING vs VARCHAR(65533) (equivalent)", MAP(STRING(), INTEGER), MAP(VARCHAR(65533), INTEGER), False),
    ("MAP value: TINYINT(1) vs BOOLEAN (equivalent)", MAP(STRING, TINYINT(1)), MAP(STRING, BOOLEAN), False),
    ("MAP key: TINYINT(1) vs BOOLEAN (equivalent)", MAP(TINYINT(1), STRING), MAP(BOOLEAN, STRING), False),
]

# 6. STRUCT type comparison
struct_type_params = [
    ("STRUCT (same)", STRUCT(a=INTEGER, b=STRING), STRUCT(a=INTEGER, b=STRING), False),
    ("STRUCT (different field order)", STRUCT(a=INTEGER, b=STRING), STRUCT(b=STRING, a=INTEGER), True),
    ("STRUCT (different field count)", STRUCT(a=INTEGER), STRUCT(a=INTEGER, b=STRING), True),
    ("STRUCT (different field name)", STRUCT(a=INTEGER), STRUCT(c=INTEGER), True),
    ("STRUCT (different field type)", STRUCT(a=INTEGER), STRUCT(a=STRING), True),
    ("Nested STRUCT with MAP (same)", STRUCT(a=INTEGER, b=MAP(STRING, INTEGER)), STRUCT(a=INTEGER, b=MAP(STRING, INTEGER)), False),
    ("Nested STRUCT with MAP (deep difference)", STRUCT(a=INTEGER, b=MAP(STRING, INTEGER)), STRUCT(a=INTEGER, b=MAP(STRING, STRING)), True),
    # Test special equivalence cases in STRUCT fields
    ("STRUCT field: TINYINT(1) vs BOOLEAN (equivalent)", STRUCT(flag=TINYINT(1)), STRUCT(flag=BOOLEAN), False),
    ("STRUCT field: VARCHAR(65533) vs STRING (equivalent)", STRUCT(text=VARCHAR(65533)), STRUCT(text=STRING), False),
    ("STRUCT field: BOOLEAN vs TINYINT(1) (equivalent)", STRUCT(flag=BOOLEAN), STRUCT(flag=TINYINT(1)), False),
]

# 7. Complex nested types (ARRAY, MAP, STRUCT combined)
complex_nested_params = [
    (
        "ARRAY<MAP<STR,STRUCT<INT,ARRAY<STR>>>> (same)",
        ARRAY(MAP(STRING, STRUCT(a=INTEGER, b=ARRAY(STRING)))),
        ARRAY(MAP(STRING, STRUCT(a=INTEGER, b=ARRAY(STRING)))),
        False,
    ),
    (
        "ARRAY<MAP<STR,STRUCT<INT,ARRAY<STR>>>> vs ARRAY<MAP<STR,STRUCT<INT,ARRAY<INT>>>> (deep difference)",
        ARRAY(MAP(STRING, STRUCT(a=INTEGER, b=ARRAY(STRING)))),
        ARRAY(MAP(STRING, STRUCT(a=INTEGER, b=ARRAY(INTEGER)))),
        True,
    ),
    (
        "MAP<INT,ARRAY<STRUCT<STR,MAP<STR,INT>>>> (same)",
        MAP(INTEGER, ARRAY(STRUCT(a=STRING, b=MAP(STRING, INTEGER)))),
        MAP(INTEGER, ARRAY(STRUCT(a=STRING, b=MAP(STRING, INTEGER)))),
        False,
    ),
    (
        "MAP<INT,ARRAY<STRUCT<STR,MAP<STR,INT>>>> vs MAP<INT,ARRAY<STRUCT<STR,MAP<INT,INT>>>> (deep difference)",
        MAP(INTEGER, ARRAY(STRUCT(a=STRING, b=MAP(STRING, INTEGER)))),
        MAP(INTEGER, ARRAY(STRUCT(a=STRING, b=MAP(INTEGER, INTEGER)))),
        True,
    ),
    (
        "STRUCT<INT,ARRAY<STR>,MAP<STR,STRUCT<STR,INT>>> (same)",
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
    ),
    (
        "STRUCT<INT,ARRAY<STR>,MAP<STR,STRUCT<STR,INT>>> vs STRUCT<INT,ARRAY<INT>,MAP<STR,STRUCT<STR,INT>>> (deep difference)",
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
    ),
]


class CompareColumnTypeTest(fixtures.TestBase):
    """Test suite for StarRocksImpl.compare_type."""

    @testing.combinations(
        *simple_type_params, argnames="inspector_type, metadata_type, is_different", id_="iaaa"
    )
    def test_simple_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of simple (non-nested) data types."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @testing.combinations(
        *complex_vs_simple_params, argnames="inspector_type, metadata_type, is_different", id_="iaaa"
    )
    def test_complex_vs_simple_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison between complex and simple types."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @testing.combinations(
        *different_complex_types_params, argnames="inspector_type, metadata_type, is_different", id_="iaaa"
    )
    def test_different_complex_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison between different kinds of complex types (e.g., ARRAY vs MAP)."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @testing.combinations(
        *array_type_params, argnames="inspector_type, metadata_type, is_different", id_="iaaa"
    )
    def test_array_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of ARRAY types, including nested scenarios."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @testing.combinations(
        *map_type_params, argnames="inspector_type, metadata_type, is_different", id_="iaaa"
    )
    def test_map_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of MAP types, including nested scenarios."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @testing.combinations(
        *struct_type_params, argnames="inspector_type, metadata_type, is_different", id_="iaaa"
    )
    def test_struct_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of STRUCT types, including nested scenarios."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @testing.combinations(
        *complex_nested_params, argnames="inspector_type, metadata_type, is_different", id_="iaaa"
    )
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

        # FLOAT precision edge cases - StarRocks doesn't support FLOAT precision, so all FLOAT types are the same
        assert run_compare(FLOAT(), FLOAT()) is False
        assert run_compare(FLOAT(10), FLOAT(10)) is False
        assert run_compare(FLOAT(10), FLOAT(20)) is False  # precision ignored
        assert run_compare(FLOAT(), FLOAT(10)) is False  # precision ignored
        assert run_compare(FLOAT(10), FLOAT()) is False  # precision ignored

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

        # Should be equivalent due to special rules (BOOLEAN <-> TINYINT(1))
        assert run_compare(deep1, deep2) is False

        # TINYINT(2) is NOT equivalent to BOOLEAN (only TINYINT(1) is)
        deep3 = ARRAY(MAP(STRING, STRUCT(
            flags=ARRAY(TINYINT(2)),  # Not equivalent to BOOLEAN
            metadata=MAP(STRING, BOOLEAN)
        )))

        assert run_compare(deep1, deep3) is True

        # INTEGER is also different from BOOLEAN
        deep4 = ARRAY(MAP(STRING, STRUCT(
            flags=ARRAY(INTEGER),  # Different type, not equivalent to BOOLEAN
            metadata=MAP(STRING, BOOLEAN)
        )))

        assert run_compare(deep1, deep4) is True

    def test_case_sensitivity_in_struct_fields(self):
        """Test case sensitivity behavior in STRUCT field names."""

        # Different case in field names
        struct1 = STRUCT(Name=STRING, Age=INTEGER)
        struct2 = STRUCT(name=STRING, age=INTEGER)

        # Current implementation treats names as case-sensitive, so they differ
        # TODO: we may need to implement case-insensitive comparison
        result = run_compare(struct1, struct2)
        assert result is True
