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
//

package com.starrocks.load.loadv2.dpp;

import com.starrocks.load.loadv2.etl.EtlJobConfig;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public class DppUtilsTest {

    @Test
    public void testGetClassFromDataType() {
        DppUtils dppUtils = new DppUtils();

        Class stringResult = dppUtils.getClassFromDataType(DataTypes.StringType);
        Assertions.assertEquals(String.class, stringResult);

        Class booleanResult = dppUtils.getClassFromDataType(DataTypes.BooleanType);
        Assertions.assertEquals(Boolean.class, booleanResult);

        Class shortResult = dppUtils.getClassFromDataType(DataTypes.ShortType);
        Assertions.assertEquals(Short.class, shortResult);

        Class integerResult = dppUtils.getClassFromDataType(DataTypes.IntegerType);
        Assertions.assertEquals(Integer.class, integerResult);

        Class longResult = dppUtils.getClassFromDataType(DataTypes.LongType);
        Assertions.assertEquals(Long.class, longResult);

        Class floatResult = dppUtils.getClassFromDataType(DataTypes.FloatType);
        Assertions.assertEquals(Float.class, floatResult);

        Class doubleResult = dppUtils.getClassFromDataType(DataTypes.DoubleType);
        Assertions.assertEquals(Double.class, doubleResult);

        Class dateResult = dppUtils.getClassFromDataType(DataTypes.DateType);
        Assertions.assertEquals(Date.class, dateResult);
    }

    @Test
    public void testGetClassFromColumn() {
        DppUtils dppUtils = new DppUtils();

        try {
            EtlJobConfig.EtlColumn column = new EtlJobConfig.EtlColumn();
            column.columnType = "CHAR";
            Class charResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(String.class, charResult);

            column.columnType = "HLL";
            Class hllResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(String.class, hllResult);

            column.columnType = "OBJECT";
            Class objectResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(String.class, objectResult);

            column.columnType = "BOOLEAN";
            Class booleanResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(Boolean.class, booleanResult);

            column.columnType = "TINYINT";
            Class tinyResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(Short.class, tinyResult);

            column.columnType = "SMALLINT";
            Class smallResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(Short.class, smallResult);

            column.columnType = "INT";
            Class integerResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(Integer.class, integerResult);

            column.columnType = "DATETIME";
            Class datetimeResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(java.sql.Timestamp.class, datetimeResult);

            column.columnType = "FLOAT";
            Class floatResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(Float.class, floatResult);

            column.columnType = "DOUBLE";
            Class doubleResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(Double.class, doubleResult);

            column.columnType = "DATE";
            Class dateResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(Date.class, dateResult);

            column.columnType = "DECIMALV2";
            column.precision = 10;
            column.scale = 2;
            Class decimalResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(BigDecimal.valueOf(10, 2).getClass(), decimalResult);

            column.columnType = "DECIMAL32";
            column.precision = 7;
            column.scale = 2;
            decimalResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(BigDecimal.valueOf(7, 2).getClass(), decimalResult);

            column.columnType = "DECIMAL64";
            column.precision = 15;
            column.scale = 3;
            decimalResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(BigDecimal.valueOf(15, 3).getClass(), decimalResult);

            column.columnType = "DECIMAL128";
            column.precision = 34;
            column.scale = 4;
            decimalResult = dppUtils.getClassFromColumn(column);
            Assertions.assertEquals(BigDecimal.valueOf(34, 4).getClass(), decimalResult);
        } catch (Exception e) {
            Assertions.assertFalse(false);
        }

    }

    @Test
    public void testGetDataTypeFromColumn() {
        DppUtils dppUtils = new DppUtils();

        try {
            EtlJobConfig.EtlColumn column = new EtlJobConfig.EtlColumn();
            column.columnType = "VARCHAR";
            DataType stringResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.StringType, stringResult);

            column.columnType = "CHAR";
            DataType charResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.StringType, charResult);

            column.columnType = "HLL";
            DataType hllResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.StringType, hllResult);

            column.columnType = "OBJECT";
            DataType objectResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.StringType, objectResult);

            column.columnType = "BOOLEAN";
            DataType booleanResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.StringType, booleanResult);

            column.columnType = "TINYINT";
            DataType tinyResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.ByteType, tinyResult);

            column.columnType = "SMALLINT";
            DataType smallResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.ShortType, smallResult);

            column.columnType = "INT";
            DataType integerResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.IntegerType, integerResult);

            column.columnType = "BIGINT";
            DataType longResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.LongType, longResult);

            column.columnType = "DATETIME";
            DataType datetimeResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.TimestampType, datetimeResult);

            column.columnType = "FLOAT";
            DataType floatResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.FloatType, floatResult);

            column.columnType = "DOUBLE";
            DataType doubleResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.DoubleType, doubleResult);

            column.columnType = "DATE";
            DataType dateResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DataTypes.DateType, dateResult);

            column.columnType = "DECIMAL32";
            column.precision = 7;
            column.scale = 2;
            DataType decimalResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DecimalType.apply(7, 2), decimalResult);

            column.columnType = "DECIMAL64";
            column.precision = 15;
            column.scale = 3;
            decimalResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DecimalType.apply(15, 3), decimalResult);

            column.columnType = "DECIMAL128";
            column.precision = 34;
            column.scale = 4;
            decimalResult = dppUtils.getDataTypeFromColumn(column, false);
            Assertions.assertEquals(DecimalType.apply(34, 4), decimalResult);
        } catch (Exception e) {
            Assertions.assertTrue(false);
        }
    }

    @Test
    public void testCreateDstTableSchema() {
        DppUtils dppUtils = new DppUtils();

        EtlJobConfig.EtlColumn column1 = new EtlJobConfig.EtlColumn(
                "column1", "INT",
                true, true,
                "NONE", "0",
                0, 0, 0);
        EtlJobConfig.EtlColumn column2 = new EtlJobConfig.EtlColumn(
                "column2", "SMALLINT",
                true, true,
                "NONE", "0",
                0, 0, 0);
        List<EtlJobConfig.EtlColumn> columns = new ArrayList<>();
        columns.add(column1);
        columns.add(column2);

        try {
            StructType schema = dppUtils.createDstTableSchema(columns, false, false);
            Assertions.assertEquals(2, schema.fieldNames().length);
            Assertions.assertEquals("column1", schema.fieldNames()[0]);
            Assertions.assertEquals("column2", schema.fieldNames()[1]);

            StructType schema2 = dppUtils.createDstTableSchema(columns, true, false);
            Assertions.assertEquals(3, schema2.fieldNames().length);
            Assertions.assertEquals("__bucketId__", schema2.fieldNames()[0]);
            Assertions.assertEquals("column1", schema2.fieldNames()[1]);
            Assertions.assertEquals("column2", schema2.fieldNames()[2]);
        } catch (Exception e) {
            Assertions.assertTrue(false);
        }
    }

    @Test
    public void testParseColumnsFromPath() {
        DppUtils dppUtils = new DppUtils();

        String path = "/path/to/file/city=beijing/date=2020-04-10/data";
        List<String> columnFromPaths = new ArrayList<>();
        columnFromPaths.add("city");
        columnFromPaths.add("date");
        try {
            List<String> columnFromPathValues = dppUtils.parseColumnsFromPath(path, columnFromPaths);
            Assertions.assertEquals(2, columnFromPathValues.size());
            Assertions.assertEquals("beijing", columnFromPathValues.get(0));
            Assertions.assertEquals("2020-04-10", columnFromPathValues.get(1));
        } catch (Exception e) {
            Assertions.assertTrue(false);
        }
    }

    @Test
    public void testGetHashValue() {
        CRC32 hashValue = new CRC32();
        ByteBuffer bf = null;

        EtlJobConfig.EtlColumn column = new EtlJobConfig.EtlColumn("k1", "INT", true, true, "NONE", "0", 0, 0, 0);
        bf = DppUtils.getHashValue(null, DataTypes.IntegerType, column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(558161692L, hashValue.getValue());

        column = new EtlJobConfig.EtlColumn("k1", "TINYINT", true, true, "NONE", "0", 0, 0, 0);
        bf = DppUtils.getHashValue(Byte.valueOf((byte) 1), DataTypes.ByteType, column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(2768625435L, hashValue.getValue());

        column = new EtlJobConfig.EtlColumn("k1", "SMALLINT", true, true, "NONE", "0", 0, 0, 0);
        bf = DppUtils.getHashValue(Short.valueOf((short) 1), DataTypes.ShortType, column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(1489118142L, hashValue.getValue());

        column = new EtlJobConfig.EtlColumn("k1", "INT", true, true, "NONE", "0", 0, 0, 0);
        bf = DppUtils.getHashValue(Integer.valueOf(1), DataTypes.IntegerType, column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(2583214201L, hashValue.getValue());

        column = new EtlJobConfig.EtlColumn("k1", "BIGINT", true, true, "NONE", "0", 0, 0, 0);
        bf = DppUtils.getHashValue(Long.valueOf(1), DataTypes.LongType, column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(2844319735L, hashValue.getValue());

        column = new EtlJobConfig.EtlColumn("k1", "VARCHAR", true, true, "NONE", "0", 100, 0, 0);
        bf = DppUtils.getHashValue("12345abcde", DataTypes.StringType, column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(603981213L, hashValue.getValue());

        column = new EtlJobConfig.EtlColumn("k1", "BOOLEAN", true, true, "NONE", "0", 0, 0, 0);
        bf = DppUtils.getHashValue(Boolean.valueOf(true), DataTypes.BooleanType, column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(2768625435L, hashValue.getValue());

        // date
        column = new EtlJobConfig.EtlColumn("k1", "DATE", true, true, "NONE", "0", 0, 0, 0);
        bf = DppUtils.getHashValue(Date.valueOf("2023-07-11"), DataTypes.DateType, column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(791129379L, hashValue.getValue());

        // datetime
        column = new EtlJobConfig.EtlColumn("k1", "DATETIME", true, true, "NONE", "0", 0, 0, 0);
        bf = DppUtils.getHashValue(Timestamp.valueOf("2023-07-11 12:12:12"), DataTypes.TimestampType, column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(541277948L, hashValue.getValue());

        column = new EtlJobConfig.EtlColumn("k1", "DATETIME", true, true, "NONE", "0", 0, 0, 0);
        bf = DppUtils.getHashValue(Timestamp.valueOf("2023-07-11 12:12:12.123456"), DataTypes.TimestampType, column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(2559661226L, hashValue.getValue());

        // decimal
        column = new EtlJobConfig.EtlColumn("k1", "DECIMALV2", true, true, "NONE", "0", 0, 27, 9);
        bf = DppUtils.getHashValue(new BigDecimal("1.234"), DecimalType.apply(27, 9), column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(3199857971L, hashValue.getValue());

        column = new EtlJobConfig.EtlColumn("k1", "DECIMALV2", true, true, "NONE", "0", 0, 20, 3);
        bf = DppUtils.getHashValue(new BigDecimal("1.234"), DecimalType.apply(20, 3), column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(3199857971L, hashValue.getValue());

        column = new EtlJobConfig.EtlColumn("k1", "DECIMAL32", true, true, "NONE", "0", 0, 9, 3);
        bf = DppUtils.getHashValue(new BigDecimal("1.234"), DecimalType.apply(9, 3), column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(2853177042L, hashValue.getValue());

        column = new EtlJobConfig.EtlColumn("k1", "DECIMAL64", true, true, "NONE", "0", 0, 18, 6);
        bf = DppUtils.getHashValue(new BigDecimal("1.234"), DecimalType.apply(18, 6), column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(563521641L, hashValue.getValue());

        column = new EtlJobConfig.EtlColumn("k1", "DECIMAL128", true, true, "NONE", "0", 0, 38, 9);
        bf = DppUtils.getHashValue(new BigDecimal("1.234"), DecimalType.apply(38, 9), column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(4291267310L, hashValue.getValue());

        column = new EtlJobConfig.EtlColumn("k1", "DECIMAL128", true, true, "NONE", "0", 0, 27, 9);
        bf = DppUtils.getHashValue(new BigDecimal("1.234"), DecimalType.apply(27, 9), column);
        hashValue.reset();
        hashValue.update(bf.array(), 0, bf.limit());
        Assertions.assertEquals(3199857971L, hashValue.getValue());
    }
}