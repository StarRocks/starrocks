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

package com.starrocks.load.loadv2.dpp;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.common.SparkDppException;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

public class DppUtils {
    public static final String BUCKET_ID = "__bucketId__";

    private static final BigDecimal[] SCALE_FACTOR = new BigDecimal[39];

    static {
        for (int i = 0; i < 39; ++i) {
            SCALE_FACTOR[i] = new BigDecimal("1" + Strings.repeat("0", i));
        }
    }

    public static Class getClassFromDataType(DataType dataType) {
        if (dataType == null) {
            return null;
        }
        if (dataType.equals(DataTypes.BooleanType)) {
            return Boolean.class;
        } else if (dataType.equals(DataTypes.ShortType)) {
            return Short.class;
        } else if (dataType.equals(DataTypes.IntegerType)) {
            return Integer.class;
        } else if (dataType.equals(DataTypes.LongType)) {
            return Long.class;
        } else if (dataType.equals(DataTypes.FloatType)) {
            return Float.class;
        } else if (dataType.equals(DataTypes.DoubleType)) {
            return Double.class;
        } else if (dataType.equals(DataTypes.DateType)) {
            return Date.class;
        } else if (dataType.equals(DataTypes.StringType)) {
            return String.class;
        } else if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            return BigDecimal.valueOf(decimalType.precision(), decimalType.scale()).getClass();
        } else if (dataType.equals(DataTypes.TimestampType)) {
            return Long.class;
        }
        return null;
    }

    public static Class getClassFromColumn(EtlJobConfig.EtlColumn column) throws SparkDppException {
        switch (column.columnType) {
            case "BOOLEAN":
                return Boolean.class;
            case "TINYINT":
            case "SMALLINT":
                return Short.class;
            case "INT":
                return Integer.class;
            case "DATETIME":
                return Timestamp.class;
            case "BIGINT":
                return Long.class;
            case "LARGEINT":
                throw new SparkDppException("LARGEINT is not supported now");
            case "FLOAT":
                return Float.class;
            case "DOUBLE":
                return Double.class;
            case "DATE":
                return Date.class;
            case "HLL":
            case "CHAR":
            case "VARCHAR":
            case "BITMAP":
            case "OBJECT":
            case "PERCENTILE":
                return String.class;
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                return BigDecimal.valueOf(column.precision, column.scale).getClass();
            default:
                return String.class;
        }
    }

    public static DataType getDataTypeFromColumn(EtlJobConfig.EtlColumn column, boolean regardDistinctColumnAsBinary) {
        DataType dataType = DataTypes.StringType;
        switch (column.columnType) {
            case "BOOLEAN":
                dataType = DataTypes.StringType;
                break;
            case "TINYINT":
                dataType = DataTypes.ByteType;
                break;
            case "SMALLINT":
                dataType = DataTypes.ShortType;
                break;
            case "INT":
                dataType = DataTypes.IntegerType;
                break;
            case "DATETIME":
                dataType = DataTypes.TimestampType;
                break;
            case "BIGINT":
                dataType = DataTypes.LongType;
                break;
            case "LARGEINT":
                dataType = DataTypes.StringType;
                break;
            case "FLOAT":
                dataType = DataTypes.FloatType;
                break;
            case "DOUBLE":
                dataType = DataTypes.DoubleType;
                break;
            case "DATE":
                dataType = DataTypes.DateType;
                break;
            case "CHAR":
            case "VARCHAR":
            case "OBJECT":
            case "PERCENTILE":
                dataType = DataTypes.StringType;
                break;
            case "HLL":
            case "BITMAP":
                dataType = regardDistinctColumnAsBinary ? DataTypes.BinaryType : DataTypes.StringType;
                break;
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                dataType = DecimalType.apply(column.precision, column.scale);
                break;
            default:
                throw new RuntimeException("Reason: invalid column type:" + column);
        }
        return dataType;
    }

    private static ByteBuffer getDecimalV2ByteBuffer(BigDecimal d) {
        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        long integerValue = d.longValue();
        BigDecimal integerPart = new BigDecimal(d.toBigInteger());
        BigDecimal fracPart = d.subtract(integerPart);
        fracPart = fracPart.setScale(9, BigDecimal.ROUND_DOWN);
        fracPart = fracPart.movePointRight(9);
        int fracValue = fracPart.intValue();
        buffer.putLong(integerValue);
        buffer.putInt(fracValue);
        return buffer;
    }

    private static ByteBuffer getDecimalByteBuffer(BigDecimal d, EtlJobConfig.EtlColumn column) {
        ByteBuffer buffer;
        switch (column.columnType) {
            case "DECIMALV2": {
                buffer = getDecimalV2ByteBuffer(d);
                break;
            }
            case "DECIMAL32": {
                buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                BigDecimal scaledValue = d.multiply(SCALE_FACTOR[column.scale]);
                buffer.putInt(scaledValue.intValue());
                break;
            }
            case "DECIMAL64": {
                buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                BigDecimal scaledValue = d.multiply(SCALE_FACTOR[column.scale]);
                buffer.putLong(scaledValue.longValue());
                break;
            }
            case "DECIMAL128": {
                int precision = column.precision;
                int scale = column.scale;
                if (precision == 27 && scale == 9) {
                    buffer = getDecimalV2ByteBuffer(d);
                } else {
                    BigDecimal scaledValue = d.multiply(SCALE_FACTOR[scale]);
                    BigInteger v = scaledValue.toBigInteger();
                    buffer = ByteBuffer.allocate(16);
                    buffer.order(ByteOrder.LITTLE_ENDIAN);
                    byte[] byteArray = v.toByteArray();
                    int len = byteArray.length;
                    int end = 0;
                    if (len > 16) {
                        end = len - 16;
                    }
                    for (int i = len - 1; i >= end; --i) {
                        buffer.put(byteArray[i]);
                    }
                    if (v.signum() >= 0) {
                        while (len++ < 16) {
                            buffer.put((byte) 0);
                        }
                    } else {
                        while (len++ < 16) {
                            buffer.put((byte) 0xFF);
                        }
                    }
                }
                break;
            }
            default:
                buffer = ByteBuffer.allocate(8);
                break;
        }
        return buffer;
    }

    public static ByteBuffer getHashValue(Object o, DataType type, EtlJobConfig.EtlColumn column) {
        // null as int 0
        if (o == null) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(0);
            buffer.flip();
            return buffer;
        }

        // varchar and char
        if (type.equals(DataTypes.StringType)) {
            try {
                String str = String.valueOf(o);
                return ByteBuffer.wrap(str.getBytes("UTF-8"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // date
        // fe/fe-core/src/main/java/com/starrocks/analysis/DateLiteral.java#L273
        if (type.equals(DataTypes.DateType)) {
            try {
                Date d = (Date) o;
                String str = ColumnParser.DATE_FORMATTER.format(d.toLocalDate());
                return ByteBuffer.wrap(str.getBytes("UTF-8"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // datetime
        // fe/fe-core/src/main/java/com/starrocks/analysis/DateLiteral.java#L273
        if (type.equals(DataTypes.TimestampType)) {
            try {
                Timestamp t = (Timestamp) o;
                String str;
                if (t.getNanos() > 1000) {
                    str = ColumnParser.DATE_TIME_WITH_MS_FORMATTER.format(t.toLocalDateTime());
                } else {
                    str = ColumnParser.DATE_TIME_FORMATTER.format(t.toLocalDateTime());
                }
                return ByteBuffer.wrap(str.getBytes("UTF-8"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // decimal
        // fe/fe-core/src/main/java/com/starrocks/analysis/DecimalLiteral.java#L292
        if (type instanceof DecimalType) {
            BigDecimal d = (BigDecimal) o;
            ByteBuffer buffer = getDecimalByteBuffer(d, column);
            buffer.flip();
            return buffer;
        }

        // bool tinyint smallint int bigint
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        if (type.equals(DataTypes.BooleanType)) {
            byte b = (boolean) o ? (byte) 1 : (byte) 0;
            buffer.put(b);
        } else if (type.equals(DataTypes.ByteType)) {
            buffer.put((byte) o);
        } else if (type.equals(DataTypes.ShortType)) {
            buffer.putShort((Short) o);
        } else if (type.equals(DataTypes.IntegerType)) {
            buffer.putInt((Integer) o);
        } else if (type.equals(DataTypes.LongType)) {
            buffer.putLong((Long) o);
        }
        buffer.flip();
        return buffer;
    }

    public static long getHashValue(Row row, List<EtlJobConfig.EtlColumn> distributeColumns,
                                    StructType dstTableSchema) {
        CRC32 hashValue = new CRC32();
        for (EtlJobConfig.EtlColumn distColumn : distributeColumns) {
            Object columnObject = row.get(row.fieldIndex(distColumn.columnName));
            ByteBuffer buffer = getHashValue(
                    columnObject, dstTableSchema.apply(distColumn.columnName).dataType(), distColumn);
            hashValue.update(buffer.array(), 0, buffer.limit());
        }
        return hashValue.getValue();
    }

    public static StructType createDstTableSchema(List<EtlJobConfig.EtlColumn> columns, boolean addBucketIdColumn,
                                                  boolean regardDistinctColumnAsBinary) {
        List<StructField> fields = new ArrayList<>();
        if (addBucketIdColumn) {
            StructField bucketIdField = DataTypes.createStructField(BUCKET_ID, DataTypes.StringType, true);
            fields.add(bucketIdField);
        }
        for (EtlJobConfig.EtlColumn column : columns) {
            DataType structColumnType = getDataTypeFromColumn(column, regardDistinctColumnAsBinary);
            StructField field = DataTypes.createStructField(column.columnName, structColumnType, column.isAllowNull);
            fields.add(field);
        }
        StructType dstSchema = DataTypes.createStructType(fields);
        return dstSchema;
    }

    public static List<String> parseColumnsFromPath(String filePath, List<String> columnsFromPath)
            throws SparkDppException {
        if (columnsFromPath == null || columnsFromPath.isEmpty()) {
            return Collections.emptyList();
        }
        String[] strings = filePath.split("/");
        if (strings.length < 2) {
            System.err
                    .println("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            throw new SparkDppException(
                    "Reason: Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
        }
        String[] columns = new String[columnsFromPath.size()];
        int size = 0;
        for (int i = strings.length - 2; i >= 0; i--) {
            String str = strings[i];
            if (str != null && str.isEmpty()) {
                continue;
            }
            if (str == null || !str.contains("=")) {
                System.err.println(
                        "Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
                throw new SparkDppException(
                        "Reason: Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " +
                                filePath);
            }
            String[] pair = str.split("=", 2);
            if (pair.length != 2) {
                System.err.println(
                        "Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
                throw new SparkDppException(
                        "Reason: Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " +
                                filePath);
            }
            int index = columnsFromPath.indexOf(pair[0]);
            if (index == -1) {
                continue;
            }
            columns[index] = pair[1];
            size++;
            if (size >= columnsFromPath.size()) {
                break;
            }
        }
        if (size != columnsFromPath.size()) {
            System.err
                    .println("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            throw new SparkDppException(
                    "Reason: Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
        }
        return Lists.newArrayList(columns);
    }
}