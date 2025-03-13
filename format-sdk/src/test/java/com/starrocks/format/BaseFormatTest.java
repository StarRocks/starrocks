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

package com.starrocks.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.format.util.ArrowUtils;
import com.starrocks.format.util.DataType;
import com.starrocks.proto.LakeTypes;
import com.starrocks.proto.TabletSchema.ColumnPB;
import com.starrocks.proto.TabletSchema.TabletSchemaPB;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.format.util.ArrowUtils.MK_COLUMN_AGG_TYPE;
import static com.starrocks.format.util.ArrowUtils.MK_COLUMN_ID;
import static com.starrocks.format.util.ArrowUtils.MK_COLUMN_IS_AUTO_INCREMENT;
import static com.starrocks.format.util.ArrowUtils.MK_COLUMN_IS_KEY;
import static com.starrocks.format.util.ArrowUtils.MK_COLUMN_MAX_LENGTH;
import static com.starrocks.format.util.ArrowUtils.MK_COLUMN_TYPE;
import static com.starrocks.format.util.ArrowUtils.MK_TABLE_ID;
import static com.starrocks.format.util.ArrowUtils.MK_TABLE_KEYS_TYPE;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseFormatTest {

    protected static final ObjectMapper JSON = new ObjectMapper();

    protected static final ZoneId DEFAULT_TZ = ZoneId.systemDefault();

    protected static long writeTabletMeta(String tabletRootPath, long version, TabletSchemaPB schema)
            throws IOException {
        long tabletId = schema.getId();

        File dataDir = new File(tabletRootPath + "/data");
        assertTrue(dataDir.mkdirs());

        File logDir = new File(tabletRootPath + "/log");
        assertTrue(logDir.mkdirs());

        File metaDir = new File(tabletRootPath + "/meta");
        assertTrue(metaDir.mkdirs());

        LakeTypes.TabletMetadataPB metadata = LakeTypes.TabletMetadataPB.newBuilder()
                .setSchema(schema)
                .build();

        File mf1 = new File(metaDir, String.format("%016X_%016X.meta", tabletId, version));
        try (FileOutputStream fos = new FileOutputStream(mf1)) {
            System.out.println("Write meta to " + mf1.getAbsolutePath());
            metadata.writeTo(fos);
        }

        File mf2 = new File(metaDir, String.format("%016X_%016X.meta", tabletId, version + 1L));
        try (FileOutputStream fos = new FileOutputStream(mf2)) {
            System.out.println("Write meta to " + mf2.getAbsolutePath());
            metadata.writeTo(fos);
        }

        return version + 1L;
    }

    protected static Schema toArrowSchema(TabletSchemaPB tabletSchema) {
        Map<String, String> metadata = new HashMap<String, String>() {
            private static final long serialVersionUID = -1564086805896748379L;

            {
                put(MK_TABLE_ID, String.valueOf(tabletSchema.getId()));
                put(MK_TABLE_KEYS_TYPE, String.valueOf(tabletSchema.getKeysType()));
            }
        };
        List<Field> fields = tabletSchema.getColumnList().stream()
                .map(BaseFormatTest::toArrowField)
                .collect(Collectors.toList());
        return new Schema(fields, metadata);
    }

    public static Field toArrowField(ColumnPB column) {
        ArrowType arrowType = ArrowUtils.toArrowType(
                column.getType(),
                DEFAULT_TZ,
                column.getPrecision(),
                column.getFrac()
        );
        Map<String, String> metadata = new HashMap<String, String>() {
            {
                put(MK_COLUMN_ID, String.valueOf(column.getUniqueId()));
                put(MK_COLUMN_TYPE, column.getType());
                put(MK_COLUMN_IS_KEY, String.valueOf(column.getIsKey()));
                put(MK_COLUMN_MAX_LENGTH, String.valueOf(column.getLength()));
                put(MK_COLUMN_AGG_TYPE, StringUtils.defaultIfBlank(column.getAggregation(), "NONE"));
                put(MK_COLUMN_IS_AUTO_INCREMENT, String.valueOf(column.getIsAutoIncrement()));
            }
        };


        List<Field> children = new ArrayList<>();
        if (DataType.MAP.is(column.getType())) {
            List<Field> mapChildren = new ArrayList<>();
            for (ColumnPB child : column.getChildrenColumnsList()) {
                Field childField = toArrowField(child);
                mapChildren.add(childField);
            }
            Field childField = new Field("entries",
                    new FieldType(false, ArrowType.Struct.INSTANCE, null, metadata), mapChildren);
            children.add(childField);
        } else {
            for (ColumnPB child : column.getChildrenColumnsList()) {
                Field childField = toArrowField(child);
                children.add(childField);
            }
        }
        return new Field(column.getName(), new FieldType(column.getIsNullable(), arrowType, null, metadata), children);
    }

    protected static void fillRowData(VectorSchemaRoot vsr, int startRowId, int numRows) throws Exception {
        for (int colIdx = 0; colIdx < vsr.getSchema().getFields().size(); colIdx++) {
            Field column = vsr.getSchema().getFields().get(colIdx);
            FieldVector fieldVector = vsr.getVector(colIdx);
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                int rowId = startRowId + rowIdx;
                if ("id".equalsIgnoreCase(column.getName())) {
                    ((BigIntVector) fieldVector).setSafe(rowIdx, rowId);
                    continue;
                }
                fillColData(column, fieldVector, rowIdx, rowId, 0);
            }
            fieldVector.setValueCount(numRows);
        }

        vsr.setRowCount(numRows);
    }

    private static void fillColData(Field field, FieldVector fieldVector, int rowIdx, int rowId, int depth)
            throws Exception {
        int sign = (rowId % 2 == 0) ? -1 : 1;
        DataType colType = DataType.of(field.getFieldType().getMetadata().get(MK_COLUMN_TYPE));
        switch (colType) {
            case BOOLEAN:
                ((BitVector) fieldVector).setSafe(rowIdx, rowId % 2);
                break;
            case TINYINT:
                TinyIntVector tinyIntVector = (TinyIntVector) fieldVector;
                if (rowId == 0) {
                    tinyIntVector.setSafe(rowIdx, Byte.MAX_VALUE);
                } else if (rowId == 1) {
                    tinyIntVector.setSafe(rowIdx, Byte.MIN_VALUE);
                } else {
                    tinyIntVector.setSafe(rowIdx, rowId * sign);
                }
                break;
            case SMALLINT:
                SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
                if (rowId == 0) {
                    smallIntVector.setSafe(rowIdx, Short.MAX_VALUE);
                } else if (rowId == 1) {
                    smallIntVector.setSafe(rowIdx, Short.MIN_VALUE);
                } else {
                    smallIntVector.setSafe(rowIdx, (short) (rowId * 10 * sign));
                }
                break;
            case INT:
                IntVector intVector = (IntVector) fieldVector;
                if (rowId == 0) {
                    intVector.setSafe(rowIdx, Integer.MAX_VALUE);
                } else if (rowId == 1) {
                    intVector.setSafe(rowIdx, Integer.MIN_VALUE);
                } else {
                    intVector.setSafe(rowIdx, rowId * 100 * sign + depth);
                }
                break;
            case BIGINT:
                BigIntVector bigIntVector = (BigIntVector) fieldVector;
                if (rowId == 0) {
                    bigIntVector.setSafe(rowIdx, Long.MAX_VALUE);
                } else if (rowId == 1) {
                    bigIntVector.setSafe(rowIdx, Long.MIN_VALUE);
                } else {
                    bigIntVector.setSafe(rowIdx, rowId * 1000L * sign);
                }
                break;
            case LARGEINT:
                VarCharVector largeIntVector = (VarCharVector) fieldVector;
                if (rowId == 0) {
                    largeIntVector.setSafe(rowIdx,
                            "170141183460469231731687303715884105727".getBytes(StandardCharsets.UTF_8));
                } else if (rowId == 1) {
                    largeIntVector.setSafe(rowIdx,
                            "-170141183460469231731687303715884105727".getBytes(StandardCharsets.UTF_8));
                } else {
                    largeIntVector.setSafe(rowIdx, String.valueOf(rowId * 10000L * sign).getBytes(StandardCharsets.UTF_8));
                }
                break;
            case FLOAT:
                ((Float4Vector) fieldVector).setSafe(rowIdx, 123.45678901234f * rowId * sign);
                break;
            case DOUBLE:
                ((Float8Vector) fieldVector).setSafe(rowIdx, 23456.78901234 * rowId * sign);
                break;
            case DECIMAL:
                // decimal v2 type
                BigDecimal bdv2;
                if (rowId == 0) {
                    bdv2 = new BigDecimal("-12345678901234567890123.4567");
                } else if (rowId == 1) {
                    bdv2 = new BigDecimal("999999999999999999999999.9999");
                } else {
                    bdv2 = new BigDecimal("1234.56789");
                    bdv2 = bdv2.multiply(BigDecimal.valueOf(sign));
                }
                ((DecimalVector) fieldVector).setSafe(rowIdx, bdv2);
                break;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getFieldType().getType();
                BigDecimal bd;
                if (rowId == 0) {
                    if (decimalType.getPrecision() <= 9) {
                        bd = new BigDecimal("9999999.5678");
                    } else if (decimalType.getPrecision() <= 18) {
                        bd = new BigDecimal("999999999999999.56789");
                    } else {
                        bd = new BigDecimal("9999999999999999999999999999999999.56789");
                    }
                } else if (rowId == 1) {
                    if (decimalType.getPrecision() <= 9) {
                        bd = new BigDecimal("-9999999.5678");
                    } else if (decimalType.getPrecision() <= 18) {
                        bd = new BigDecimal("-999999999999999.56789");
                    } else {
                        bd = new BigDecimal("-9999999999999999999999999999999999.56789");
                    }
                } else {
                    if (decimalType.getPrecision() <= 9) {
                        bd = new BigDecimal("12345.5678");
                    } else if (decimalType.getPrecision() <= 18) {
                        bd = new BigDecimal("123456789012.56789");
                    } else {
                        bd = new BigDecimal("12345678901234567890123.56789");
                    }
                    bd = bd.multiply(BigDecimal.valueOf((long) rowId * sign));
                }
                bd = bd.setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                ((DecimalVector) fieldVector).setSafe(rowIdx, bd);
                break;
            case DATE:
                Date dt;
                if (rowId == 0) {
                    dt = Date.valueOf("1900-1-1");
                } else if (rowId == 1) {
                    dt = Date.valueOf("4096-12-31");
                } else {
                    dt = Date.valueOf("2023-10-31");
                    dt.setYear(123 + rowId * sign + depth);
                }
                if (fieldVector instanceof DateDayVector) {
                    ((DateDayVector) fieldVector).setSafe(rowIdx, (int) dt.toLocalDate().toEpochDay());
                } else if (fieldVector instanceof DateMilliVector) {
                    ((DateMilliVector) fieldVector).setSafe(rowIdx, dt.toLocalDate().toEpochDay() * 24 * 3600 * 1000);
                } else {
                    throw new IllegalStateException("unsupported column type: " + field.getType());
                }
                break;
            case DATETIME:
                LocalDateTime ts;
                if (rowId == 0) {
                    ts = LocalDateTime.parse("1800-11-20T12:34:56");
                } else if (rowId == 1) {
                    ts = LocalDateTime.parse("4096-11-30T11:22:33");
                } else {
                    ts = LocalDateTime.parse("2023-12-30T22:33:44");
                    ts = ts.withYear(1900 + 123 + rowId * sign);
                }
                ((TimeStampMicroTZVector) fieldVector)
                        .setSafe(rowIdx, ts.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() * 1000L);
                break;
            case CHAR:
            case VARCHAR:
                String strValue = field.getName() + ":name" + rowId;
                if (depth > 0) {
                    strValue += ",d:" + depth;
                }
                ((VarCharVector) fieldVector).setSafe(rowIdx, strValue.getBytes());
                break;
            case BINARY:
            case VARBINARY:
                String valuePrefix = field.getName() + ":name" + rowId + ":";
                ByteBuffer buffer = ByteBuffer.allocate(valuePrefix.getBytes().length + 4);
                buffer.put(valuePrefix.getBytes());
                buffer.putInt(rowId);
                ((VarBinaryVector) fieldVector).setSafe(rowIdx, buffer.array());
                break;
            case BITMAP:
            case OBJECT:
                byte[] bitmapValue = new byte[] {0x00};
                switch (rowId % 4) {
                    case 0:
                        bitmapValue = new byte[] {0x01, 0x00, 0x00, 0x00, 0x00};
                        break;
                    case 1:
                        bitmapValue = new byte[] {0x01, (byte) 0xE8, 0x03, 0x00, 0x00};
                        break;
                    case 3:
                        bitmapValue = new byte[] {0x1, (byte) 0xB8, 0xB, 0x0, 0x0};
                        break;
                }
                ((VarBinaryVector) fieldVector).setSafe(rowIdx, bitmapValue);
                break;
            case HLL:
                byte[] hllValue = new byte[] {0x00};
                switch (rowId % 4) {
                    case 0:
                        hllValue = new byte[] {0x00};
                        break;
                    case 1:
                        hllValue = new byte[] {
                                0x1, 0x1, 0x44, 0x6, (byte) 0xC3, (byte) 0x80, (byte) 0x9E, (byte) 0x9D, (byte) 0xE6, 0x14
                        };
                        break;
                    case 3:
                        hllValue = new byte[] {0x1, 0x1, (byte) 0x9A, 0x5, (byte) 0xE4, (byte) 0xE6, 0x65, 0x76, 0x4, 0x28};
                        break;
                }
                ((VarBinaryVector) fieldVector).setSafe(rowIdx, hllValue);
                break;
            case ARRAY: {
                ListVector listVector = (ListVector) fieldVector;
                List<FieldVector> children = fieldVector.getChildrenFromFields();
                int elementSize = (rowId + depth) % 4;
                listVector.startNewValue(rowIdx);
                for (FieldVector childVector : children) {
                    if (childVector instanceof IntVector) {
                        int intVal = rowId * 100 * sign;
                        int startOffset = childVector.getValueCount();
                        for (int arrayIndex = 0; arrayIndex < elementSize; arrayIndex++) {
                            ((IntVector) childVector).setSafe(startOffset + arrayIndex, intVal + depth + arrayIndex);
                        }
                        childVector.setValueCount(startOffset + elementSize);
                    }
                }
                listVector.endValue(rowIdx, elementSize);
            }
            break;
            case JSON: {
                Map<String, Object> jsonMap = new HashMap<>();
                jsonMap.put("rowid", rowId);
                boolean boolVal = rowId % 2 == 0;
                jsonMap.put("bool", boolVal);
                int intVal = 0;
                if (rowId == 0) {
                    intVal = Integer.MAX_VALUE;
                } else if (rowId == 1) {
                    intVal = Integer.MIN_VALUE;
                } else {
                    intVal = rowId * 100 * sign;
                }
                jsonMap.put("int", intVal);
                jsonMap.put("varchar", field.getName() + ":name" + rowId);
                String json = JSON.writeValueAsString(jsonMap);
                ((VarCharVector) fieldVector).setSafe(rowIdx, json.getBytes(), 0, json.getBytes().length);
            }
            break;
            case MAP: {
                int elementSize = rowId % 4;
                ((ListVector) fieldVector).startNewValue(rowIdx);
                UnionMapWriter mapWriter = ((MapVector) fieldVector).getWriter();
                mapWriter.setPosition(rowIdx);
                mapWriter.startMap();
                int intVal = rowId * 100 * sign;
                for (int arrayIndex = 0; arrayIndex < elementSize; arrayIndex++) {
                    mapWriter.startEntry();
                    mapWriter.key().integer().writeInt(intVal + depth + arrayIndex);
                    mapWriter.value().varChar().writeVarChar("mapvalue:" + (intVal + depth + arrayIndex));
                    mapWriter.endEntry();
                }
                mapWriter.endMap();
            }
            break;
            case STRUCT:
                List<FieldVector> children = ((StructVector) fieldVector).getChildrenFromFields();
                for (FieldVector childVector : children) {
                    fillColData(childVector.getField(), childVector, rowId, rowIdx, depth + 1);
                }
                ((StructVector) fieldVector).setIndexDefined(rowIdx);
                break;
            default:
                throw new IllegalStateException("unsupported column type: " + field.getType());
        }
    }

    protected static class ColumnType {

        private DataType dataType;
        private Integer length;
        private Integer precision;
        private Integer scale;

        public ColumnType(DataType dataType, Integer length) {
            this.dataType = dataType;
            this.length = length;
        }

        public ColumnType(DataType dataType, Integer length, Integer precision, Integer scale) {
            this.dataType = dataType;
            this.length = length;
            this.precision = precision;
            this.scale = scale;
        }

        public DataType getDataType() {
            return dataType;
        }

        public void setDataType(DataType dataType) {
            this.dataType = dataType;
        }

        public Integer getLength() {
            return length;
        }

        public void setLength(Integer length) {
            this.length = length;
        }

        public Integer getPrecision() {
            return precision;
        }

        public void setPrecision(Integer precision) {
            this.precision = precision;
        }

        public Integer getScale() {
            return scale;
        }

        public void setScale(Integer scale) {
            this.scale = scale;
        }
    }

}
