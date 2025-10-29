// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TColumnDefinition;
import com.starrocks.thrift.TColumnType;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TShowResultSet;
import com.starrocks.thrift.TShowResultSetMetaData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Combined test class for ShowResultSetMetaData and ShowResultSet.
 * This class tests both metadata creation/manipulation and result set operations.
 */
public class ShowResultTest {

    // ==================== Helper Methods ====================

    private ShowResultSetMetaData createTestMetaData() {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("string_col", ScalarType.createType(PrimitiveType.VARCHAR)))
                .addColumn(new Column("int_col", ScalarType.createType(PrimitiveType.INT)))
                .addColumn(new Column("long_col", ScalarType.createType(PrimitiveType.BIGINT)))
                .addColumn(new Column("short_col", ScalarType.createType(PrimitiveType.SMALLINT)))
                .addColumn(new Column("byte_col", ScalarType.createType(PrimitiveType.TINYINT)))
                .build();
    }

    private ShowResultSetMetaData createDecimalMetaData() {
        return ShowResultSetMetaData.builder()
                .addColumn(new Column("decimal_col", ScalarType.createDecimalV2Type(10, 2)))
                .addColumn(new Column("char_col", ScalarType.createCharType(5)))
                .addColumn(new Column("varchar_col", ScalarType.createVarcharType(255)))
                .build();
    }

    // ==================== ShowResultSetMetaData Tests ====================

    @Test
    public void testMetaDataEmpty() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder().build();
        Assertions.assertEquals(0, metaData.getColumnCount());
        Assertions.assertTrue(metaData.getColumns().isEmpty());
    }

    @Test
    public void testMetaDataBasicCreation() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("col1", ScalarType.createType(PrimitiveType.INT)))
                .addColumn(new Column("col2", ScalarType.createType(PrimitiveType.VARCHAR)))
                .build();

        Assertions.assertEquals(2, metaData.getColumnCount());
        Assertions.assertEquals("col1", metaData.getColumn(0).getName());
        Assertions.assertEquals("col2", metaData.getColumn(1).getName());
        Assertions.assertEquals(PrimitiveType.INT, metaData.getColumn(0).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.VARCHAR, metaData.getColumn(1).getType().getPrimitiveType());
    }

    @Test
    public void testMetaDataColumnIndexOutOfBounds() {
        assertThrows(IndexOutOfBoundsException.class, () -> {
            ShowResultSetMetaData metaData = ShowResultSetMetaData.builder().build();
            metaData.getColumn(1);
        });
    }

    @Test
    public void testMetaDataBuilderWithColumn() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .column("test_col", ScalarType.createType(PrimitiveType.BIGINT))
                .column("varchar_col", ScalarType.createVarcharType(255))
                .column("decimal_col", ScalarType.createDecimalV2Type(10, 2))
                .build();

        Assertions.assertEquals(3, metaData.getColumnCount());
        Assertions.assertEquals("test_col", metaData.getColumn(0).getName());
        Assertions.assertEquals("varchar_col", metaData.getColumn(1).getName());
        Assertions.assertEquals("decimal_col", metaData.getColumn(2).getName());
        
        Assertions.assertEquals(PrimitiveType.BIGINT, metaData.getColumn(0).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.VARCHAR, metaData.getColumn(1).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.DECIMALV2, metaData.getColumn(2).getType().getPrimitiveType());
    }

    @Test
    public void testMetaDataGetColumns() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("col1", ScalarType.createType(PrimitiveType.INT)))
                .addColumn(new Column("col2", ScalarType.createType(PrimitiveType.VARCHAR)))
                .build();

        List<Column> columns = metaData.getColumns();
        Assertions.assertEquals(2, columns.size());
        Assertions.assertEquals("col1", columns.get(0).getName());
        Assertions.assertEquals("col2", columns.get(1).getName());
    }

    @Test
    public void testMetaDataGetColumnIdx() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("FirstColumn", ScalarType.createType(PrimitiveType.INT)))
                .addColumn(new Column("SecondColumn", ScalarType.createType(PrimitiveType.VARCHAR)))
                .addColumn(new Column("ThirdColumn", ScalarType.createType(PrimitiveType.DOUBLE)))
                .build();

        // Test exact case match
        Assertions.assertEquals(0, metaData.getColumnIdx("FirstColumn"));
        Assertions.assertEquals(1, metaData.getColumnIdx("SecondColumn"));
        Assertions.assertEquals(2, metaData.getColumnIdx("ThirdColumn"));

        // Test case insensitive match
        Assertions.assertEquals(0, metaData.getColumnIdx("firstcolumn"));
        Assertions.assertEquals(1, metaData.getColumnIdx("SECONDCOLUMN"));
        Assertions.assertEquals(2, metaData.getColumnIdx("ThIrDcOlUmN"));
    }

    @Test
    public void testMetaDataGetColumnIdxNotFound() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("col1", ScalarType.createType(PrimitiveType.INT)))
                .addColumn(new Column("col2", ScalarType.createType(PrimitiveType.VARCHAR)))
                .build();

        assertThrows(StarRocksPlannerException.class, () -> {
            metaData.getColumnIdx("NonExistentColumn");
        });
    }

    @Test
    public void testMetaDataGetColumnIdxEmptyName() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("col1", ScalarType.createType(PrimitiveType.INT)))
                .build();

        assertThrows(StarRocksPlannerException.class, () -> {
            metaData.getColumnIdx("");
        });
    }

    @Test
    public void testMetaDataGetColumnIdxNullName() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("col1", ScalarType.createType(PrimitiveType.INT)))
                .build();

        assertThrows(StarRocksPlannerException.class, () -> {
            metaData.getColumnIdx(null);
        });
    }

    @Test
    public void testMetaDataMixedBuilderMethods() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("explicit_col", ScalarType.createType(PrimitiveType.INT)))
                .column("builder_col", ScalarType.createType(PrimitiveType.VARCHAR))
                .addColumn(new Column("another_explicit", ScalarType.createCharType(10)))
                .column("final_col", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4))
                .build();

        Assertions.assertEquals(4, metaData.getColumnCount());
        Assertions.assertEquals("explicit_col", metaData.getColumn(0).getName());
        Assertions.assertEquals("builder_col", metaData.getColumn(1).getName());
        Assertions.assertEquals("another_explicit", metaData.getColumn(2).getName());
        Assertions.assertEquals("final_col", metaData.getColumn(3).getName());
        
        // Verify types are preserved correctly
        Assertions.assertEquals(PrimitiveType.INT, metaData.getColumn(0).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.VARCHAR, metaData.getColumn(1).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.CHAR, metaData.getColumn(2).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.DECIMAL64, metaData.getColumn(3).getType().getPrimitiveType());
    }

    @Test
    public void testMetaDataSpecialColumnNames() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .column("", ScalarType.createType(PrimitiveType.INT)) // Empty name
                .column("Column With Spaces", ScalarType.createType(PrimitiveType.VARCHAR))
                .column("column_with_underscores", ScalarType.createType(PrimitiveType.DOUBLE))
                .column("UPPERCASE", ScalarType.createType(PrimitiveType.BOOLEAN))
                .column("MiXeDcAsE", ScalarType.createType(PrimitiveType.DATE))
                .column("123numeric", ScalarType.createType(PrimitiveType.TIME))
                .column("special@#$%chars", ScalarType.createType(PrimitiveType.DATETIME))
                .build();

        Assertions.assertEquals(7, metaData.getColumnCount());
        
        // Test that all names are preserved as-is
        Assertions.assertEquals("", metaData.getColumn(0).getName());
        Assertions.assertEquals("Column With Spaces", metaData.getColumn(1).getName());
        Assertions.assertEquals("column_with_underscores", metaData.getColumn(2).getName());
        Assertions.assertEquals("UPPERCASE", metaData.getColumn(3).getName());
        Assertions.assertEquals("MiXeDcAsE", metaData.getColumn(4).getName());
        Assertions.assertEquals("123numeric", metaData.getColumn(5).getName());
        Assertions.assertEquals("special@#$%chars", metaData.getColumn(6).getName());
        
        // Test case-insensitive lookup
        Assertions.assertEquals(1, metaData.getColumnIdx("column with spaces"));
        Assertions.assertEquals(2, metaData.getColumnIdx("COLUMN_WITH_UNDERSCORES"));
        Assertions.assertEquals(3, metaData.getColumnIdx("uppercase"));
        Assertions.assertEquals(4, metaData.getColumnIdx("mixedcase"));
    }

    @Test
    public void testMetaDataAllScalarTypes() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .column("boolean_col", ScalarType.createType(PrimitiveType.BOOLEAN))
                .column("tinyint_col", ScalarType.createType(PrimitiveType.TINYINT))
                .column("smallint_col", ScalarType.createType(PrimitiveType.SMALLINT))
                .column("int_col", ScalarType.createType(PrimitiveType.INT))
                .column("bigint_col", ScalarType.createType(PrimitiveType.BIGINT))
                .column("largeint_col", ScalarType.createType(PrimitiveType.LARGEINT))
                .column("float_col", ScalarType.createType(PrimitiveType.FLOAT))
                .column("double_col", ScalarType.createType(PrimitiveType.DOUBLE))
                .column("date_col", ScalarType.createType(PrimitiveType.DATE))
                .column("datetime_col", ScalarType.createType(PrimitiveType.DATETIME))
                .column("time_col", ScalarType.createType(PrimitiveType.TIME))
                .column("char_col", ScalarType.createCharType(10))
                .column("varchar_col", ScalarType.createVarcharType(255))
                .column("varbinary_col", ScalarType.createVarbinary(1024))
                .column("hll_col", ScalarType.createHllType())
                .column("bitmap_col", ScalarType.createType(PrimitiveType.BITMAP))
                .column("percentile_col", ScalarType.createType(PrimitiveType.PERCENTILE))
                .column("json_col", ScalarType.createType(PrimitiveType.JSON))
                .build();

        Assertions.assertEquals(18, metaData.getColumnCount());
        
        // Verify each type is correctly stored
        Assertions.assertEquals(PrimitiveType.BOOLEAN, metaData.getColumn(0).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.TINYINT, metaData.getColumn(1).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.SMALLINT, metaData.getColumn(2).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.INT, metaData.getColumn(3).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.BIGINT, metaData.getColumn(4).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.LARGEINT, metaData.getColumn(5).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.FLOAT, metaData.getColumn(6).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.DOUBLE, metaData.getColumn(7).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.DATE, metaData.getColumn(8).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.DATETIME, metaData.getColumn(9).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.TIME, metaData.getColumn(10).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.CHAR, metaData.getColumn(11).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.VARCHAR, metaData.getColumn(12).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.VARBINARY, metaData.getColumn(13).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.HLL, metaData.getColumn(14).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.BITMAP, metaData.getColumn(15).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.PERCENTILE, metaData.getColumn(16).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.JSON, metaData.getColumn(17).getType().getPrimitiveType());
    }

    @Test
    public void testMetaDataDecimalTypes() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .column("decimalv2_col", ScalarType.createDecimalV2Type(10, 2))
                .column("decimal32_col", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2))
                .column("decimal64_col", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4))
                .column("decimal128_col", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 8))
                .column("decimal256_col", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL256, 76, 16))
                .build();

        Assertions.assertEquals(5, metaData.getColumnCount());
        
        // Verify decimal types and their precision/scale
        ScalarType decimalV2 = (ScalarType) metaData.getColumn(0).getType();
        Assertions.assertEquals(PrimitiveType.DECIMALV2, decimalV2.getPrimitiveType());
        Assertions.assertEquals(10, decimalV2.getScalarPrecision());
        Assertions.assertEquals(2, decimalV2.getScalarScale());
        
        ScalarType decimal32 = (ScalarType) metaData.getColumn(1).getType();
        Assertions.assertEquals(PrimitiveType.DECIMAL32, decimal32.getPrimitiveType());
        Assertions.assertEquals(9, decimal32.getScalarPrecision());
        Assertions.assertEquals(2, decimal32.getScalarScale());
        
        ScalarType decimal64 = (ScalarType) metaData.getColumn(2).getType();
        Assertions.assertEquals(PrimitiveType.DECIMAL64, decimal64.getPrimitiveType());
        Assertions.assertEquals(18, decimal64.getScalarPrecision());
        Assertions.assertEquals(4, decimal64.getScalarScale());
        
        ScalarType decimal128 = (ScalarType) metaData.getColumn(3).getType();
        Assertions.assertEquals(PrimitiveType.DECIMAL128, decimal128.getPrimitiveType());
        Assertions.assertEquals(38, decimal128.getScalarPrecision());
        Assertions.assertEquals(8, decimal128.getScalarScale());
        
        ScalarType decimal256 = (ScalarType) metaData.getColumn(4).getType();
        Assertions.assertEquals(PrimitiveType.DECIMAL256, decimal256.getPrimitiveType());
        Assertions.assertEquals(76, decimal256.getScalarPrecision());
        Assertions.assertEquals(16, decimal256.getScalarScale());
    }

    @Test
    public void testMetaDataBuilderIndependence() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        
        // Create first metadata with 2 columns
        ShowResultSetMetaData metaData1 = builder
                .column("col1", ScalarType.createType(PrimitiveType.INT))
                .column("col2", ScalarType.createType(PrimitiveType.VARCHAR))
                .build();
        
        // Create new builder for second metadata
        ShowResultSetMetaData metaData2 = ShowResultSetMetaData.builder()
                .column("col1", ScalarType.createType(PrimitiveType.INT))
                .column("col2", ScalarType.createType(PrimitiveType.VARCHAR))
                .column("col3", ScalarType.createType(PrimitiveType.DOUBLE))
                .build();
        
        // First metadata should have 2 columns
        Assertions.assertEquals(2, metaData1.getColumnCount());
        Assertions.assertEquals("col1", metaData1.getColumn(0).getName());
        Assertions.assertEquals("col2", metaData1.getColumn(1).getName());
        
        // Second metadata should have 3 columns
        Assertions.assertEquals(3, metaData2.getColumnCount());
        Assertions.assertEquals("col1", metaData2.getColumn(0).getName());
        Assertions.assertEquals("col2", metaData2.getColumn(1).getName());
        Assertions.assertEquals("col3", metaData2.getColumn(2).getName());
    }

    @Test
    public void testMetaDataDuplicateColumnNames() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .column("duplicate", ScalarType.createType(PrimitiveType.INT))
                .column("Duplicate", ScalarType.createType(PrimitiveType.VARCHAR))  // Different case
                .column("duplicate", ScalarType.createType(PrimitiveType.DOUBLE))   // Exact duplicate
                .build();

        Assertions.assertEquals(3, metaData.getColumnCount());
        
        // getColumnIdx should return the first match (case-insensitive)
        Assertions.assertEquals(0, metaData.getColumnIdx("duplicate"));
        Assertions.assertEquals(0, metaData.getColumnIdx("DUPLICATE"));
    }

    // ==================== ShowResultSet Tests ====================

    @Test
    public void testResultSetBasic() {
        ShowResultSetMetaData metaData = createTestMetaData();
        List<List<String>> rows = Lists.newArrayList();

        rows.add(Lists.newArrayList("col1-0", "100", "200", "300", "50"));
        rows.add(Lists.newArrayList("col1-1", "123", "456", "789", "25"));
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        
        Assertions.assertEquals(rows, resultSet.getResultRows());
        Assertions.assertEquals(metaData, resultSet.getMetaData());
        
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("col1-0", resultSet.getString(0));
        Assertions.assertEquals(100, resultSet.getInt(1));
        Assertions.assertEquals(200L, resultSet.getLong(2));
        Assertions.assertEquals((short) 300, resultSet.getShort(3));
        Assertions.assertEquals((byte) 50, resultSet.getByte(4));
        
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("col1-1", resultSet.getString(0));
        Assertions.assertEquals(123, resultSet.getInt(1));
        Assertions.assertEquals(456L, resultSet.getLong(2));
        Assertions.assertEquals((short) 789, resultSet.getShort(3));
        Assertions.assertEquals((byte) 25, resultSet.getByte(4));
        
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    public void testResultSetAccessBeforeNext() {
        assertThrows(IndexOutOfBoundsException.class, () -> {
            ShowResultSetMetaData metaData = createTestMetaData();
            List<List<String>> rows = Lists.newArrayList();

            rows.add(Lists.newArrayList("col1-0", "100", "200", "300", "50"));
            ShowResultSet resultSet = new ShowResultSet(metaData, rows);
            resultSet.getString(0); // Should fail because next() was not called
        });
    }

    @Test
    public void testResultSetBadNumberFormat() {
        assertThrows(NumberFormatException.class, () -> {
            ShowResultSetMetaData metaData = createTestMetaData();
            List<List<String>> rows = Lists.newArrayList();

            rows.add(Lists.newArrayList("col1-0", " 123", "456", "789", "25")); // Leading space causes NumberFormatException
            ShowResultSet resultSet = new ShowResultSet(metaData, rows);
            resultSet.next();
            resultSet.getInt(1);
        });
    }

    @Test
    public void testResultSetEmpty() {
        ShowResultSetMetaData metaData = createTestMetaData();
        List<List<String>> rows = Lists.newArrayList();
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        
        Assertions.assertEquals(0, resultSet.getResultRows().size());
        Assertions.assertFalse(resultSet.next());
        Assertions.assertEquals(5, resultSet.numColumns());
    }

    @Test
    public void testResultSetSingleRow() {
        ShowResultSetMetaData metaData = createTestMetaData();
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("test", "42", "1000", "10", "5"));
        
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("test", resultSet.getString(0));
        Assertions.assertEquals(42, resultSet.getInt(1));
        Assertions.assertEquals(1000L, resultSet.getLong(2));
        Assertions.assertEquals((short) 10, resultSet.getShort(3));
        Assertions.assertEquals((byte) 5, resultSet.getByte(4));
        
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    public void testResultSetNumberBoundaryValues() {
        ShowResultSetMetaData metaData = createTestMetaData();
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("test", "32767", "9223372036854775807", "32767", "127"));
        rows.add(Lists.newArrayList("test2", "-32768", "-9223372036854775808", "-32768", "-128"));
        
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        
        // Test max values
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(32767, resultSet.getInt(1));
        Assertions.assertEquals(9223372036854775807L, resultSet.getLong(2));
        Assertions.assertEquals((short) 32767, resultSet.getShort(3));
        Assertions.assertEquals((byte) 127, resultSet.getByte(4));
        
        // Test min values
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(-32768, resultSet.getInt(1));
        Assertions.assertEquals(-9223372036854775808L, resultSet.getLong(2));
        Assertions.assertEquals((short) -32768, resultSet.getShort(3));
        Assertions.assertEquals((byte) -128, resultSet.getByte(4));
    }

    @Test
    public void testResultSetNumberFormatExceptions() {
        ShowResultSetMetaData metaData = createTestMetaData();
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("test", "not_a_number", "123", "456", "78"));
        
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        resultSet.next();
        
        // Test each number parsing method throws NumberFormatException for invalid input
        assertThrows(NumberFormatException.class, () -> resultSet.getInt(1));
        assertThrows(NumberFormatException.class, () -> resultSet.getLong(1));
        assertThrows(NumberFormatException.class, () -> resultSet.getShort(1));
        assertThrows(NumberFormatException.class, () -> resultSet.getByte(1));
    }

    @Test
    public void testResultSetColumnIndexOutOfBounds() {
        ShowResultSetMetaData metaData = createTestMetaData();
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("test", "123", "456", "789", "12"));
        
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        resultSet.next();
        
        // Test accessing invalid column indices
        assertThrows(IndexOutOfBoundsException.class, () -> resultSet.getString(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> resultSet.getString(5));
        assertThrows(IndexOutOfBoundsException.class, () -> resultSet.getInt(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> resultSet.getInt(5));
    }

    @Test
    public void testResultSetRowIndexBehavior() {
        ShowResultSetMetaData metaData = createTestMetaData();
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("test", "123", "456", "789", "12"));
        
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        
        // Try to access data before calling next()
        assertThrows(IndexOutOfBoundsException.class, () -> resultSet.getString(0));
        
        resultSet.next();
        resultSet.getString(0); // This should work
        
        // When next() returns false, rowIdx remains at the last valid index
        // so accessing data after calling next() beyond the end should still work
        Assertions.assertFalse(resultSet.next()); // Move past the last row, but rowIdx doesn't change
        resultSet.getString(0); // This should still work since rowIdx is still 0
    }

    @Test
    public void testResultSetNumColumns() {
        ShowResultSetMetaData metaData1 = ShowResultSetMetaData.builder().build();
        ShowResultSet resultSet1 = new ShowResultSet(metaData1, Lists.newArrayList());
        Assertions.assertEquals(0, resultSet1.numColumns());
        
        ShowResultSetMetaData metaData2 = createTestMetaData();
        ShowResultSet resultSet2 = new ShowResultSet(metaData2, Lists.newArrayList());
        Assertions.assertEquals(5, resultSet2.numColumns());
    }

    @Test
    public void testResultSetTShowResultSetConstructor() {
        // Create TShowResultSet with various column types
        TShowResultSet tResultSet = new TShowResultSet();
        TShowResultSetMetaData tMetaData = new TShowResultSetMetaData();
        
        // Add VARCHAR column
        TColumnDefinition varcharCol = new TColumnDefinition();
        varcharCol.setColumnName("varchar_col");
        TColumnType varcharType = new TColumnType();
        varcharType.setType(TPrimitiveType.VARCHAR);
        varcharCol.setColumnType(varcharType);
        tMetaData.addToColumns(varcharCol);
        
        // Add INT column
        TColumnDefinition intCol = new TColumnDefinition();
        intCol.setColumnName("int_col");
        TColumnType intType = new TColumnType();
        intType.setType(TPrimitiveType.INT);
        intCol.setColumnType(intType);
        tMetaData.addToColumns(intCol);
        
        // Add BIGINT column
        TColumnDefinition bigintCol = new TColumnDefinition();
        bigintCol.setColumnName("bigint_col");
        TColumnType bigintType = new TColumnType();
        bigintType.setType(TPrimitiveType.BIGINT);
        bigintCol.setColumnType(bigintType);
        tMetaData.addToColumns(bigintCol);
        
        tResultSet.setMetaData(tMetaData);
        
        // Add test data
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("test1", "100", "1000"));
        rows.add(Lists.newArrayList("test2", "200", "2000"));
        tResultSet.setResultRows(rows);
        
        // Create ShowResultSet from TShowResultSet
        ShowResultSet resultSet = new ShowResultSet(tResultSet);
        
        // Verify metadata
        Assertions.assertEquals(3, resultSet.numColumns());
        Assertions.assertEquals("varchar_col", resultSet.getMetaData().getColumn(0).getName());
        Assertions.assertEquals("int_col", resultSet.getMetaData().getColumn(1).getName());
        Assertions.assertEquals("bigint_col", resultSet.getMetaData().getColumn(2).getName());
        
        // Verify data
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("test1", resultSet.getString(0));
        Assertions.assertEquals(100, resultSet.getInt(1));
        Assertions.assertEquals(1000L, resultSet.getLong(2));
        
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("test2", resultSet.getString(0));
        Assertions.assertEquals(200, resultSet.getInt(1));
        Assertions.assertEquals(2000L, resultSet.getLong(2));
        
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    public void testResultSetToThrift() {
        ShowResultSetMetaData metaData = createDecimalMetaData();
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("123.45", "hello", "world"));
        rows.add(Lists.newArrayList("678.90", "test", "data"));
        rows.add(Lists.newArrayList(null, null, null)); // Test null values
        
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        TShowResultSet tResultSet = resultSet.tothrift();
        
        // Verify metadata conversion
        Assertions.assertEquals(3, tResultSet.getMetaData().getColumnsSize());
        
        TColumnDefinition decimalCol = tResultSet.getMetaData().getColumns().get(0);
        Assertions.assertEquals("decimal_col", decimalCol.getColumnName());
        Assertions.assertEquals(TPrimitiveType.DECIMALV2, decimalCol.getColumnType().getType());
        
        TColumnDefinition charCol = tResultSet.getMetaData().getColumns().get(1);
        Assertions.assertEquals("char_col", charCol.getColumnName());
        Assertions.assertEquals(TPrimitiveType.CHAR, charCol.getColumnType().getType());
        Assertions.assertEquals(5, charCol.getColumnType().getLen());
        
        TColumnDefinition varcharCol = tResultSet.getMetaData().getColumns().get(2);
        Assertions.assertEquals("varchar_col", varcharCol.getColumnName());
        Assertions.assertEquals(TPrimitiveType.VARCHAR, varcharCol.getColumnType().getType());
        Assertions.assertEquals(255, varcharCol.getColumnType().getLen());
        
        // Verify data conversion
        Assertions.assertEquals(3, tResultSet.getResultRowsSize());
        Assertions.assertEquals("123.45", tResultSet.getResultRows().get(0).get(0));
        Assertions.assertEquals("hello", tResultSet.getResultRows().get(0).get(1));
        Assertions.assertEquals("world", tResultSet.getResultRows().get(0).get(2));
        
        Assertions.assertEquals("678.90", tResultSet.getResultRows().get(1).get(0));
        Assertions.assertEquals("test", tResultSet.getResultRows().get(1).get(1));
        Assertions.assertEquals("data", tResultSet.getResultRows().get(1).get(2));
        
        // Verify null values are converted to empty strings
        Assertions.assertEquals("", tResultSet.getResultRows().get(2).get(0));
        Assertions.assertEquals("", tResultSet.getResultRows().get(2).get(1));
        Assertions.assertEquals("", tResultSet.getResultRows().get(2).get(2));
    }

    @Test
    public void testResultSetToThriftWithDecimalV3Types() {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("decimal32_col", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2)))
                .addColumn(new Column("decimal64_col", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4)))
                .addColumn(new Column("decimal128_col", ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 8)))
                .build();
        
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("123.45", "1234567890.1234", "12345678901234567890.12345678"));
        
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        TShowResultSet tResultSet = resultSet.tothrift();
        
        // Verify decimal V3 types are properly converted
        TColumnDefinition decimal32Col = tResultSet.getMetaData().getColumns().get(0);
        Assertions.assertEquals(TPrimitiveType.DECIMAL32, decimal32Col.getColumnType().getType());
        Assertions.assertEquals(9, decimal32Col.getColumnType().getPrecision());
        Assertions.assertEquals(2, decimal32Col.getColumnType().getScale());
        
        TColumnDefinition decimal64Col = tResultSet.getMetaData().getColumns().get(1);
        Assertions.assertEquals(TPrimitiveType.DECIMAL64, decimal64Col.getColumnType().getType());
        Assertions.assertEquals(18, decimal64Col.getColumnType().getPrecision());
        Assertions.assertEquals(4, decimal64Col.getColumnType().getScale());
        
        TColumnDefinition decimal128Col = tResultSet.getMetaData().getColumns().get(2);
        Assertions.assertEquals(TPrimitiveType.DECIMAL128, decimal128Col.getColumnType().getType());
        Assertions.assertEquals(38, decimal128Col.getColumnType().getPrecision());
        Assertions.assertEquals(8, decimal128Col.getColumnType().getScale());
    }

    @Test
    public void testResultSetThriftRoundTrip() {
        ShowResultSetMetaData originalMetaData = createTestMetaData();
        List<List<String>> originalRows = Lists.newArrayList();
        originalRows.add(Lists.newArrayList("test1", "100", "1000", "10", "5"));
        originalRows.add(Lists.newArrayList("test2", "200", "2000", "20", "10"));
        
        ShowResultSet originalResultSet = new ShowResultSet(originalMetaData, originalRows);
        
        // Convert to Thrift and back
        TShowResultSet tResultSet = originalResultSet.tothrift();
        ShowResultSet roundTripResultSet = new ShowResultSet(tResultSet);
        
        // Verify metadata is preserved
        Assertions.assertEquals(originalResultSet.numColumns(), roundTripResultSet.numColumns());
        
        // Verify data is preserved
        Assertions.assertEquals(originalRows.size(), roundTripResultSet.getResultRows().size());
        
        for (int i = 0; i < originalRows.size(); i++) {
            for (int j = 0; j < originalRows.get(i).size(); j++) {
                Assertions.assertEquals(originalRows.get(i).get(j), 
                                      roundTripResultSet.getResultRows().get(i).get(j));
            }
        }
    }

    @Test
    public void testResultSetLargeDataSet() {
        ShowResultSetMetaData metaData = createTestMetaData();
        List<List<String>> rows = Lists.newArrayList();
        
        // Create a large dataset
        for (int i = 0; i < 1000; i++) {
            rows.add(Lists.newArrayList("row_" + i, String.valueOf(i), String.valueOf(i * 2L), 
                                      String.valueOf((short) (i % 1000)), String.valueOf((byte) (i % 128))));
        }
        
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        
        Assertions.assertEquals(1000, resultSet.getResultRows().size());
        
        // Verify we can iterate through all rows
        int count = 0;
        while (resultSet.next()) {
            Assertions.assertEquals("row_" + count, resultSet.getString(0));
            Assertions.assertEquals(count, resultSet.getInt(1));
            Assertions.assertEquals(count * 2L, resultSet.getLong(2));
            count++;
        }
        
        Assertions.assertEquals(1000, count);
    }

    @Test
    public void testResultSetEdgeCaseValues() {
        ShowResultSetMetaData metaData = createTestMetaData();
        List<List<String>> rows = Lists.newArrayList();
        
        // Test with edge case values
        rows.add(Lists.newArrayList("", "0", "0", "0", "0")); // Empty string and zeros
        rows.add(Lists.newArrayList("  ", "   123   ", "   456   ", "   789   ", "   12   ")); // Whitespace
        
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        
        // First row - empty and zero values
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("", resultSet.getString(0));
        Assertions.assertEquals(0, resultSet.getInt(1));
        Assertions.assertEquals(0L, resultSet.getLong(2));
        Assertions.assertEquals((short) 0, resultSet.getShort(3));
        Assertions.assertEquals((byte) 0, resultSet.getByte(4));
        
        // Second row - whitespace should cause NumberFormatException for numeric parsing
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("  ", resultSet.getString(0));
        
        // These should fail due to leading/trailing whitespace
        assertThrows(NumberFormatException.class, () -> resultSet.getInt(1));
        assertThrows(NumberFormatException.class, () -> resultSet.getLong(2));
        assertThrows(NumberFormatException.class, () -> resultSet.getShort(3));
        assertThrows(NumberFormatException.class, () -> resultSet.getByte(4));
    }
}
