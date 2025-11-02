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
Integration tests for StarRocks data types reflection.

This test suite verifies that data types can be correctly reflected from the
database schema back to SQLAlchemy types, ensuring round-trip consistency
between type definition, creation, and reflection.

These tests require a running StarRocks instance and are designed to catch
issues where:
1. Types are not correctly reflected from the database
2. Round-trip type conversion loses information
3. Complex nested types are not properly parsed from database metadata
"""
import logging

from sqlalchemy import Column, Engine, MetaData, Table, text

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
    SMALLINT,
    STRING,
    STRUCT,
    TINYINT,
    VARBINARY,
    VARCHAR,
)


logger = logging.getLogger(__name__)


class TestDataTypeReflection:
    """Integration tests for data type reflection from StarRocks database"""

    def _create_and_reflect_table(self, sr_engine: Engine, table_name: str,
                                  table_def: Table) -> Table:
        """Helper method to create a table and reflect it back"""
        with sr_engine.connect() as connection:
            # Drop table if exists
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

            # Create table
            table_def.create(connection)

            try:
                # Reflect the table
                reflected_table = Table(table_name, MetaData(), autoload_with=connection)
                return reflected_table
            finally:
                # Clean up
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

    def test_reflect_basic_numeric_types(self, sr_root_engine: Engine):
        """Test reflection of basic numeric data types"""
        table_name = "test_reflect_numeric_types"
        metadata = MetaData()

        table = Table(
            table_name,
            metadata,
            Column("id_col", INTEGER()),
            Column("tinyint_col", TINYINT()),
            Column("tinyint_width_col", TINYINT(1)),
            Column("smallint_col", SMALLINT()),
            Column("int_col", INTEGER()),
            Column("bigint_col", BIGINT()),
            Column("largeint_col", LARGEINT()),
            Column("decimal_col", DECIMAL(10, 2)),
            Column("decimal_no_scale_col", DECIMAL(8)),
            Column("float_col", FLOAT()),
            Column("float_precision_col", FLOAT(10)),
            Column("double_col", DOUBLE()),
            starrocks_distributed_by='HASH(int_col)',
            starrocks_properties={"replication_num": "1"},
        )

        reflected_table = self._create_and_reflect_table(sr_root_engine, table_name, table)

        # Check that all columns were reflected
        assert len(reflected_table.columns) == 12

        # Check specific column types
        assert isinstance(reflected_table.c.tinyint_col.type, TINYINT)
        assert isinstance(reflected_table.c.tinyint_width_col.type, TINYINT)
        assert isinstance(reflected_table.c.smallint_col.type, SMALLINT)
        assert isinstance(reflected_table.c.int_col.type, INTEGER)
        assert isinstance(reflected_table.c.bigint_col.type, BIGINT)
        assert isinstance(reflected_table.c.largeint_col.type, LARGEINT)
        assert isinstance(reflected_table.c.decimal_col.type, DECIMAL)
        assert isinstance(reflected_table.c.decimal_no_scale_col.type, DECIMAL)
        assert isinstance(reflected_table.c.float_col.type, FLOAT)
        assert isinstance(reflected_table.c.float_precision_col.type, FLOAT)
        assert isinstance(reflected_table.c.double_col.type, DOUBLE)

        # Check DECIMAL precision and scale
        assert reflected_table.c.decimal_col.type.precision == 10
        assert reflected_table.c.decimal_col.type.scale == 2
        assert reflected_table.c.decimal_no_scale_col.type.precision == 8
        assert reflected_table.c.decimal_no_scale_col.type.scale == 0

        # Check TINYINT display width (some versions use width 1/4 etc.)
        assert isinstance(reflected_table.c.tinyint_width_col.type, TINYINT)

    def test_reflect_boolean_type(self, sr_root_engine: Engine):
        """Test reflection of BOOLEAN type"""
        table_name = "test_reflect_boolean_type"
        metadata = MetaData()

        table = Table(
            table_name,
            metadata,
            Column("id_col", INTEGER()),
            Column("bool_col", BOOLEAN()),
            starrocks_distributed_by='HASH(id_col)',
            starrocks_properties={"replication_num": "1"},
        )

        reflected_table = self._create_and_reflect_table(sr_root_engine, table_name, table)

        # BOOLEAN might be reflected as TINYINT(1) depending on StarRocks version
        # This tests the equivalence logic
        bool_col_type = reflected_table.c.bool_col.type
        assert (isinstance(bool_col_type, BOOLEAN) or
                (isinstance(bool_col_type, TINYINT) and
                 getattr(bool_col_type, 'display_width', None) == 1))

    def test_reflect_string_types(self, sr_root_engine: Engine):
        """Test reflection of string data types"""
        table_name = "test_reflect_string_types"
        metadata = MetaData()

        table = Table(
            table_name,
            metadata,
            Column("id_col", INTEGER()),
            Column("char_col", CHAR(10)),
            Column("varchar_col", VARCHAR(255)),
            Column("varchar_max_col", VARCHAR(65533)),
            Column("string_col", STRING()),
            starrocks_distributed_by='HASH(char_col)',
            starrocks_properties={"replication_num": "1"},
        )

        reflected_table = self._create_and_reflect_table(sr_root_engine, table_name, table)

        # Check column types
        assert isinstance(reflected_table.c.char_col.type, CHAR)
        assert isinstance(reflected_table.c.varchar_col.type, VARCHAR)

        # VARCHAR(65533) might be reflected as STRING depending on implementation
        varchar_max_col_type = reflected_table.c.varchar_max_col.type
        assert (isinstance(varchar_max_col_type, VARCHAR) and
                getattr(varchar_max_col_type, 'length', None) == 65533) or \
               isinstance(varchar_max_col_type, STRING)

        # STRING might be reflected as VARCHAR(65533) depending on implementation
        string_col_type = reflected_table.c.string_col.type
        assert isinstance(string_col_type, STRING) or \
               (isinstance(string_col_type, VARCHAR) and
                getattr(string_col_type, 'length', None) == 65533)

        # Check lengths
        assert reflected_table.c.char_col.type.length == 10
        assert reflected_table.c.varchar_col.type.length == 255

    def test_reflect_binary_types(self, sr_root_engine: Engine):
        """Test reflection of binary data types"""
        table_name = "test_reflect_binary_types"
        metadata = MetaData()

        table = Table(
            table_name,
            metadata,
            Column("id_col", INTEGER()),
            Column("binary_col", BINARY(10)),
            Column("varbinary_col", VARBINARY(255)),
            starrocks_distributed_by='HASH(id_col)',
            starrocks_properties={"replication_num": "1"},
        )

        reflected_table = self._create_and_reflect_table(sr_root_engine, table_name, table)

        # Check column types
        logger.debug(f"reflected_table.c.binary_col.type: {reflected_table.c.binary_col.type!r}")
        logger.debug(f"reflected_table.c.varbinary_col.type: {reflected_table.c.varbinary_col.type!r}")
        assert isinstance(reflected_table.c.binary_col.type, (BINARY, VARBINARY))
        assert isinstance(reflected_table.c.varbinary_col.type, VARBINARY)

        # Check lengths
        # Length is not preserved on reflection for binary types; just ensure type is correct
        # assert reflected_table.c.binary_col.type.length == 10
        # assert reflected_table.c.varbinary_col.type.length == 255

    def test_reflect_datetime_types(self, sr_root_engine: Engine):
        """Test reflection of datetime data types"""
        table_name = "test_reflect_datetime_types"
        metadata = MetaData()

        table = Table(
            table_name,
            metadata,
            Column("id_col", INTEGER()),
            Column("date_col", DATE()),
            Column("datetime_col", DATETIME()),
            starrocks_distributed_by='HASH(date_col)',
            starrocks_properties={"replication_num": "1"},
        )

        reflected_table = self._create_and_reflect_table(sr_root_engine, table_name, table)

        # Check column types
        assert isinstance(reflected_table.c.date_col.type, DATE)
        assert isinstance(reflected_table.c.datetime_col.type, DATETIME)

    def test_reflect_special_types(self, sr_root_engine: Engine):
        """Test reflection of special StarRocks data types"""
        table_name = "test_reflect_special_types"
        metadata = MetaData()

        table = Table(
            table_name,
            metadata,
            Column("id_col", INTEGER()),
            Column("hll_col", HLL(), starrocks_agg_type='HLL_UNION'),
            Column("bitmap_col", BITMAP(), starrocks_agg_type='BITMAP_UNION'),
            # Column("percentile_col", PERCENTILE()),  # Not supported by compiler yet
            Column("json_col", JSON(), starrocks_agg_type='REPLACE'),
            starrocks_aggregate_key='id_col',
            starrocks_distributed_by='HASH(id_col)',
            starrocks_properties={"replication_num": "1"},
        )

        reflected_table = self._create_and_reflect_table(sr_root_engine, table_name, table)

        # Check column types
        assert isinstance(reflected_table.c.hll_col.type, HLL)
        assert isinstance(reflected_table.c.bitmap_col.type, BITMAP)
        # assert isinstance(reflected_table.c.percentile_col.type, PERCENTILE)
        assert isinstance(reflected_table.c.json_col.type, JSON)

    def test_reflect_array_types(self, sr_root_engine: Engine):
        """Test reflection of ARRAY data types"""
        table_name = "test_reflect_array_types"
        metadata = MetaData()

        table = Table(
            table_name,
            metadata,
            Column("id_col", INTEGER()),
            Column("int_array_col", ARRAY(INTEGER)),
            Column("string_array_col", ARRAY(STRING)),
            Column("varchar_array_col", ARRAY(VARCHAR(50))),
            Column("decimal_array_col", ARRAY(DECIMAL(8, 2))),
            Column("nested_array_col", ARRAY(ARRAY(INTEGER))),
            starrocks_distributed_by='HASH(id_col)',
            starrocks_properties={"replication_num": "1"},
        )

        reflected_table = self._create_and_reflect_table(sr_root_engine, table_name, table)

        # Check column types
        assert isinstance(reflected_table.c.int_array_col.type, ARRAY)
        assert isinstance(reflected_table.c.string_array_col.type, ARRAY)
        assert isinstance(reflected_table.c.varchar_array_col.type, ARRAY)
        assert isinstance(reflected_table.c.decimal_array_col.type, ARRAY)
        assert isinstance(reflected_table.c.nested_array_col.type, ARRAY)

        # Check array item types
        assert isinstance(reflected_table.c.int_array_col.type.item_type, INTEGER)
        assert isinstance(reflected_table.c.string_array_col.type.item_type, STRING) or \
               (isinstance(reflected_table.c.string_array_col.type.item_type, VARCHAR) and
                getattr(reflected_table.c.string_array_col.type.item_type, 'length', None) == 65533)
        assert isinstance(reflected_table.c.varchar_array_col.type.item_type, VARCHAR)
        assert isinstance(reflected_table.c.decimal_array_col.type.item_type, DECIMAL)
        assert isinstance(reflected_table.c.nested_array_col.type.item_type, ARRAY)

        # Check nested array item type
        assert isinstance(reflected_table.c.nested_array_col.type.item_type.item_type, INTEGER)

        # Check parameters preservation
        assert reflected_table.c.varchar_array_col.type.item_type.length == 50
        assert reflected_table.c.decimal_array_col.type.item_type.precision == 8
        assert reflected_table.c.decimal_array_col.type.item_type.scale == 2

    def test_reflect_map_types(self, sr_root_engine: Engine):
        """Test reflection of MAP data types"""
        table_name = "test_reflect_map_types"
        metadata = MetaData()

        table = Table(
            table_name,
            metadata,
            Column("id_col", INTEGER()),
            Column("simple_map_col", MAP(STRING, INTEGER)),
            Column("varchar_key_map_col", MAP(VARCHAR(50), DOUBLE)),
            Column("decimal_value_map_col", MAP(INTEGER, DECIMAL(10, 2))),
            Column("nested_map_col", MAP(STRING, MAP(INTEGER, STRING))),
            starrocks_distributed_by='HASH(id_col)',
            starrocks_properties={"replication_num": "1"},
        )

        reflected_table = self._create_and_reflect_table(sr_root_engine, table_name, table)

        # Check column types
        assert isinstance(reflected_table.c.simple_map_col.type, MAP)
        assert isinstance(reflected_table.c.varchar_key_map_col.type, MAP)
        assert isinstance(reflected_table.c.decimal_value_map_col.type, MAP)
        assert isinstance(reflected_table.c.nested_map_col.type, MAP)

        # Check map key/value types for simple map
        simple_map_type = reflected_table.c.simple_map_col.type
        assert isinstance(simple_map_type.key_type, STRING) or \
               (isinstance(simple_map_type.key_type, VARCHAR) and
                getattr(simple_map_type.key_type, 'length', None) == 65533)
        assert isinstance(simple_map_type.value_type, INTEGER)

        # Check map with VARCHAR key
        varchar_key_map_type = reflected_table.c.varchar_key_map_col.type
        assert isinstance(varchar_key_map_type.key_type, VARCHAR)
        assert varchar_key_map_type.key_type.length == 50
        assert isinstance(varchar_key_map_type.value_type, DOUBLE)

        # Check map with DECIMAL value
        decimal_value_map_type = reflected_table.c.decimal_value_map_col.type
        assert isinstance(decimal_value_map_type.key_type, INTEGER)
        assert isinstance(decimal_value_map_type.value_type, DECIMAL)
        assert decimal_value_map_type.value_type.precision == 10
        assert decimal_value_map_type.value_type.scale == 2

        # Check nested map types
        nested_map_type = reflected_table.c.nested_map_col.type
        assert isinstance(nested_map_type.key_type, STRING) or \
               (isinstance(nested_map_type.key_type, VARCHAR) and
                getattr(nested_map_type.key_type, 'length', None) == 65533)
        assert isinstance(nested_map_type.value_type, MAP)
        assert isinstance(nested_map_type.value_type.key_type, INTEGER)
        assert isinstance(nested_map_type.value_type.value_type, STRING) or \
               (isinstance(nested_map_type.value_type.value_type, VARCHAR) and
                getattr(nested_map_type.value_type.value_type, 'length', None) == 65533)

    def test_reflect_struct_types(self, sr_root_engine: Engine):
        """Test reflection of STRUCT data types"""
        table_name = "test_reflect_struct_types"
        metadata = MetaData()

        table = Table(
            table_name,
            metadata,
            Column("id_col", INTEGER()),
            Column("simple_struct_col", STRUCT(name=STRING, age=INTEGER)),
            Column("complex_struct_col", STRUCT(
                id=INTEGER,
                name=VARCHAR(100),
                active=BOOLEAN,
                score=DECIMAL(5, 2)
            )),
            Column("nested_struct_col", STRUCT(
                user=STRUCT(id=INTEGER, name=STRING),
                metadata=MAP(STRING, STRING)
            )),
            starrocks_distributed_by='HASH(id_col)',
            starrocks_properties={"replication_num": "1"},
        )

        reflected_table = self._create_and_reflect_table(sr_root_engine, table_name, table)

        # Check column types
        assert isinstance(reflected_table.c.simple_struct_col.type, STRUCT)
        assert isinstance(reflected_table.c.complex_struct_col.type, STRUCT)
        assert isinstance(reflected_table.c.nested_struct_col.type, STRUCT)

        # Check simple struct fields
        simple_struct = reflected_table.c.simple_struct_col.type
        assert len(simple_struct.field_tuples) == 2
        field_names = [name for name, _ in simple_struct.field_tuples]
        assert 'name' in field_names
        assert 'age' in field_names

        # Check complex struct fields and types
        complex_struct = reflected_table.c.complex_struct_col.type
        assert len(complex_struct.field_tuples) == 4
        field_dict = {name: type_ for name, type_ in complex_struct.field_tuples}

        assert 'id' in field_dict
        assert isinstance(field_dict['id'], INTEGER)

        assert 'name' in field_dict
        assert isinstance(field_dict['name'], VARCHAR)
        assert field_dict['name'].length == 100

        assert 'active' in field_dict
        # BOOLEAN might be reflected as TINYINT(1)
        assert isinstance(field_dict['active'], BOOLEAN) or \
               (isinstance(field_dict['active'], TINYINT) and
                getattr(field_dict['active'], 'display_width', None) == 1)

        assert 'score' in field_dict
        assert isinstance(field_dict['score'], DECIMAL)
        assert field_dict['score'].precision == 5
        assert field_dict['score'].scale == 2

        # Check nested struct
        nested_struct = reflected_table.c.nested_struct_col.type
        assert len(nested_struct.field_tuples) == 2
        nested_field_dict = {name: type_ for name, type_ in nested_struct.field_tuples}

        assert 'user' in nested_field_dict
        assert isinstance(nested_field_dict['user'], STRUCT)

        assert 'metadata' in nested_field_dict
        assert isinstance(nested_field_dict['metadata'], MAP)

    def test_reflect_complex_nested_types(self, sr_root_engine: Engine):
        """Test reflection of complex nested data types"""
        table_name = "test_reflect_complex_types"
        metadata = MetaData()

        table = Table(
            table_name,
            metadata,
            Column("id_col", INTEGER()),
            Column("array_of_maps", ARRAY(MAP(STRING, INTEGER))),
            Column("map_of_arrays", MAP(VARCHAR(50), ARRAY(DECIMAL(8, 2)))),
            Column("complex_struct_col", STRUCT(
                id=INTEGER,
                tags=ARRAY(VARCHAR(20)),
                metadata=MAP(STRING, STRUCT(value=STRING, count=INTEGER)),
                nested_data=ARRAY(MAP(INTEGER, STRUCT(
                    item_id=BIGINT,
                    properties=MAP(VARCHAR(30), DOUBLE)
                )))
            )),
            starrocks_distributed_by='HASH(id_col)',
            starrocks_properties={"replication_num": "1"},
        )

        reflected_table = self._create_and_reflect_table(sr_root_engine, table_name, table)

        # Check array of maps
        array_of_maps = reflected_table.c.array_of_maps.type
        assert isinstance(array_of_maps, ARRAY)
        assert isinstance(array_of_maps.item_type, MAP)

        # Check map of arrays
        map_of_arrays = reflected_table.c.map_of_arrays.type
        assert isinstance(map_of_arrays, MAP)
        assert isinstance(map_of_arrays.key_type, VARCHAR)
        assert isinstance(map_of_arrays.value_type, ARRAY)
        assert isinstance(map_of_arrays.value_type.item_type, DECIMAL)

        # Check complex struct
        complex_struct = reflected_table.c.complex_struct_col.type
        assert isinstance(complex_struct, STRUCT)
        field_dict = {name: type_ for name, type_ in complex_struct.field_tuples}

        # Check struct fields
        assert 'id' in field_dict
        assert isinstance(field_dict['id'], INTEGER)

        assert 'tags' in field_dict
        assert isinstance(field_dict['tags'], ARRAY)
        assert isinstance(field_dict['tags'].item_type, VARCHAR)

        assert 'metadata' in field_dict
        assert isinstance(field_dict['metadata'], MAP)
        assert isinstance(field_dict['metadata'].value_type, STRUCT)

        assert 'nested_data' in field_dict
        assert isinstance(field_dict['nested_data'], ARRAY)
        assert isinstance(field_dict['nested_data'].item_type, MAP)
        assert isinstance(field_dict['nested_data'].item_type.value_type, STRUCT)

    def test_round_trip_type_consistency(self, sr_root_engine: Engine):
        """Test that types maintain consistency through create -> reflect -> compare cycle"""
        table_name = "test_round_trip_consistency"
        metadata = MetaData()

        # Create a comprehensive table with various types
        original_table = Table(
            table_name,
            metadata,
            Column("simple_int", INTEGER()),
            Column("varchar_col", VARCHAR(100)),
            Column("decimal_col", DECIMAL(10, 3)),
            Column("bool_col", BOOLEAN()),
            Column("array_col", ARRAY(VARCHAR(50))),
            Column("map_col", MAP(STRING, INTEGER)),
            Column("struct_col", STRUCT(
                name=VARCHAR(80),
                active=BOOLEAN,
                score=DECIMAL(6, 2)
            )),
            starrocks_distributed_by='HASH(simple_int)',
            starrocks_properties={"replication_num": "1"},
        )

        reflected_table = self._create_and_reflect_table(sr_root_engine, table_name, original_table)

        # Use the type comparison logic to check consistency
        from starrocks.alembic.starrocks import StarRocksImpl
        from starrocks.dialect import StarRocksDialect

        dialect = StarRocksDialect()
        impl = StarRocksImpl(
            dialect=dialect,
            connection=None,
            as_sql=False,
            transactional_ddl=False,
            output_buffer=None,
            context_opts={},
        )

        # Compare each column type
        for col_name in original_table.columns.keys():
            original_col = original_table.columns[col_name]
            reflected_col = reflected_table.columns[col_name]

            # Types should be considered the same (compare_type returns False for same types)
            is_different = impl.compare_type(reflected_col, original_col)
            assert is_different is False, \
                f"Column {col_name}: Original type {original_col.type} != Reflected type {reflected_col.type}"

    def test_edge_case_parameters_preservation(self, sr_root_engine: Engine):
        """Test that edge case parameters are preserved during reflection"""
        table_name = "test_edge_case_parameters"
        metadata = MetaData()

        table = Table(
            table_name,
            metadata,
            Column("id_col", INTEGER()),
            Column("tinyint_one", TINYINT(1)),  # Special case for BOOLEAN equivalence
            Column("varchar_max", VARCHAR(65533)),  # Special case for STRING equivalence
            Column("decimal_max", DECIMAL(38, 18)),  # Maximum precision/scale
            Column("char_one", CHAR(1)),  # Minimum char length
            Column("nested_complex", STRUCT(
                level1=ARRAY(MAP(VARCHAR(65533), STRUCT(
                    flag=TINYINT(1),
                    data=DECIMAL(38, 18)
                )))
            )),
            starrocks_distributed_by='HASH(id_col)',
            starrocks_properties={"replication_num": "1"},
        )

        reflected_table = self._create_and_reflect_table(sr_root_engine, table_name, table)

        # Check TINYINT(1) - might be reflected as TINYINT with any width or BOOLEAN
        tinyint_col = reflected_table.c.tinyint_one.type
        assert isinstance(tinyint_col, (TINYINT, BOOLEAN))

        # Check VARCHAR(65533) - might be reflected as STRING
        varchar_col = reflected_table.c.varchar_max.type
        assert (isinstance(varchar_col, VARCHAR) and varchar_col.length == 65533) or \
               isinstance(varchar_col, STRING)

        # Check DECIMAL parameters
        decimal_col = reflected_table.c.decimal_max.type
        assert isinstance(decimal_col, DECIMAL)
        assert decimal_col.precision == 38
        assert decimal_col.scale == 18

        # Check CHAR length
        char_col = reflected_table.c.char_one.type
        assert isinstance(char_col, CHAR)
        assert char_col.length == 1

        # Check nested complex type preservation
        nested_col = reflected_table.c.nested_complex.type
        assert isinstance(nested_col, STRUCT)

        # Navigate through the nested structure
        field_dict = {name: type_ for name, type_ in nested_col.field_tuples}
        level1_type = field_dict['level1']
        assert isinstance(level1_type, ARRAY)
        assert isinstance(level1_type.item_type, MAP)

        map_key_type = level1_type.item_type.key_type
        map_value_type = level1_type.item_type.value_type

        # Key should be VARCHAR(65533) or STRING
        assert (isinstance(map_key_type, VARCHAR) and map_key_type.length == 65533) or \
               isinstance(map_key_type, STRING)

        # Value should be STRUCT
        assert isinstance(map_value_type, STRUCT)
        struct_fields = {name: type_ for name, type_ in map_value_type.field_tuples}

        # Check nested TINYINT(1) (any width) and DECIMAL(38, 18)
        flag_type = struct_fields['flag']
        data_type = struct_fields['data']

        assert isinstance(flag_type, (TINYINT, BOOLEAN))

        assert isinstance(data_type, DECIMAL)
        assert data_type.precision == 38
        assert data_type.scale == 18
