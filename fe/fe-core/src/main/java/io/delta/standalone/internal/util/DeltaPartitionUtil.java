// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package io.delta.standalone.internal.util;

import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.internal.exception.DeltaErrors;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DeltaPartitionUtil {
    //process partition value null
    public static String DElTA_DEFAULT_PARTITION_VALUE = "__HIVE_DEFAULT_PARTITION__";

    public static Set<String> getAllPushedPartitionValues(Metadata metadata,
                                                          Set<Map<String, String>> allPartitions,
                                                          Expression expression) {
        StructType partitionedSchema = ConversionUtils.convertMetadataJ(metadata).partitionSchema();
        String[] partitionColumns = partitionedSchema.getFieldNames();
        Seq<String> partitionSchema = JavaConverters.asScalaIterator(Arrays.asList(partitionColumns).iterator()).toSeq();
        Option<Expression> metadataConjunction = PartitionUtils
                .splitMetadataAndDataPredicates(expression, partitionSchema)._1;

        if (metadataConjunction.isEmpty()) {
            return new HashSet<>();
        } else {
            Set<Map<String, String>> collect = allPartitions.parallelStream().filter(
                    partitionValues -> {
                        DeltaPartitionRowRecord partitionRowRecord =
                                new DeltaPartitionRowRecord(partitionedSchema, partitionValues);
                        Boolean eval = (Boolean) metadataConjunction.get().eval(partitionRowRecord);
                        return eval;
                    }).collect(Collectors.toSet());
            return toHivePartitionFormat(metadata.getPartitionColumns(), collect);
        }
    }

    public static Set<String> toHivePartitionFormat(List<String> partitionColumns,
                                                    Set<Map<String, String>> partitionSet) {

        return partitionSet.parallelStream().map(
                partitions -> {
                    StringBuilder sb = new StringBuilder();
                    for (String p : partitionColumns) {
                        sb.append(p).append("=");
                        if (partitions.containsKey(p) && null != partitions.get(p)) {
                            sb.append(partitions.get(p));
                        } else {
                            sb.append(DElTA_DEFAULT_PARTITION_VALUE);
                        }
                        sb.append("/");
                    }
                    return sb.deleteCharAt(sb.length() - 1).toString();
                }

        ).collect(Collectors.toSet());
    }

    public static Set<Map<String, String>> revertPartitions(Set<String> allPartitions) {
        Set<Map<String, String>> mapSet = allPartitions.parallelStream().map(partition -> {
            String[] splits = partition.split("/");
            Map<String, String> map = new HashMap<>();
            for (String str : splits) {
                String[] partitionNameValues = str.split("=");
                if (DElTA_DEFAULT_PARTITION_VALUE.equals(partitionNameValues[1])) {
                    map.put(partitionNameValues[0], null);
                } else {
                    map.put(partitionNameValues[0], partitionNameValues[1]);
                }
            }
            return map;
        }).collect(Collectors.toSet());
        return mapSet;
    }

    static class DeltaPartitionRowRecord implements RowRecord {
        private StructType partitionSchema;
        private Map<String, String> partitionValues;

        public DeltaPartitionRowRecord(StructType partitionSchema,
                                       Map<String, String> partitionValues) {
            this.partitionSchema = partitionSchema;
            this.partitionValues = partitionValues;
        }

        private String getPrimitive(StructField field) throws Throwable {
            String partitionValue = partitionValues.get(field.getName());
            if (partitionValue == null) {
                throw DeltaErrors.nullValueFoundForPrimitiveTypes(field.getName());
            }

            return partitionValue;
        }

        @Override
        public StructType getSchema() {
            return partitionSchema;
        }

        @Override
        public int getLength() {
            return partitionSchema.getFieldNames().length;
        }

        @Override
        public boolean isNullAt(String fieldName) {
            partitionSchema.get(fieldName);
            return partitionValues.get(fieldName) == null;
        }

        @Override
        public int getInt(String fieldName) {
            StructField structField = partitionSchema.get(fieldName);
            if (!(structField.getDataType() instanceof IntegerType)) {
                try {
                    throw DeltaErrors.fieldTypeMismatch(fieldName, structField.getDataType(), "integer");
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                return Integer.parseInt(getPrimitive(structField));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }


        }

        @Override
        public long getLong(String fieldName) {
            StructField structField = partitionSchema.get(fieldName);
            if (!(structField.getDataType() instanceof LongType)) {
                try {
                    throw DeltaErrors.fieldTypeMismatch(fieldName, structField.getDataType(), "long");
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                return Long.parseLong(getPrimitive(structField));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte getByte(String fieldName) {
            StructField structField = partitionSchema.get(fieldName);
            if (!(structField.getDataType() instanceof ByteType)) {
                try {
                    throw DeltaErrors.fieldTypeMismatch(fieldName, structField.getDataType(), "byte");
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                return Byte.parseByte(getPrimitive(structField));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public short getShort(String fieldName) {
            StructField structField = partitionSchema.get(fieldName);
            if (!(structField.getDataType() instanceof ShortType)) {
                try {
                    throw DeltaErrors.fieldTypeMismatch(fieldName, structField.getDataType(), "short");
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                return Short.parseShort(getPrimitive(structField));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean getBoolean(String fieldName) {
            StructField structField = partitionSchema.get(fieldName);
            if (!(structField.getDataType() instanceof BooleanType)) {
                try {
                    throw DeltaErrors.fieldTypeMismatch(fieldName, structField.getDataType(), "boolean");
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                return Boolean.parseBoolean(getPrimitive(structField));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public float getFloat(String fieldName) {
            StructField structField = partitionSchema.get(fieldName);
            if (!(structField.getDataType() instanceof FloatType)) {
                try {
                    throw DeltaErrors.fieldTypeMismatch(fieldName, structField.getDataType(), "float");
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                return Float.parseFloat(getPrimitive(structField));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public double getDouble(String fieldName) {
            StructField structField = partitionSchema.get(fieldName);
            if (!(structField.getDataType() instanceof DoubleType)) {
                try {
                    throw DeltaErrors.fieldTypeMismatch(fieldName, structField.getDataType(), "double");
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                return Double.parseDouble(getPrimitive(structField));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getString(String fieldName) {
            StructField structField = partitionSchema.get(fieldName);
            if (!(structField.getDataType() instanceof StringType)) {
                try {
                    throw DeltaErrors.fieldTypeMismatch(fieldName, structField.getDataType(), "string");
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                return getPrimitive(structField);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte[] getBinary(String fieldName) {
            StructField structField = partitionSchema.get(fieldName);
            if (!(structField.getDataType() instanceof BinaryType)) {
                try {
                    throw DeltaErrors.fieldTypeMismatch(fieldName, structField.getDataType(), "binary");
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                return getPrimitive(structField).getBytes();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public BigDecimal getBigDecimal(String fieldName) {
            StructField structField = partitionSchema.get(fieldName);
            if (!(structField.getDataType() instanceof DecimalType)) {
                try {
                    throw DeltaErrors.fieldTypeMismatch(fieldName, structField.getDataType(), "decimal");
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                return new BigDecimal(getPrimitive(structField));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Timestamp getTimestamp(String fieldName) {
            StructField structField = partitionSchema.get(fieldName);
            if (!(structField.getDataType() instanceof TimestampType)) {
                try {
                    throw DeltaErrors.fieldTypeMismatch(fieldName, structField.getDataType(), "timestamp");
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                //TODO 判断null
                return Timestamp.valueOf(getPrimitive(structField));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Date getDate(String fieldName) {
            StructField structField = partitionSchema.get(fieldName);
            if (!(structField.getDataType() instanceof DateType)) {
                try {
                    throw DeltaErrors.fieldTypeMismatch(fieldName, structField.getDataType(), "date");
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                //TODO 判断null
                return Date.valueOf(getPrimitive(structField));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public RowRecord getRecord(String fieldName) {
            throw new UnsupportedOperationException(
                    "Struct is not a supported partition type.");
        }

        @Override
        public <T> List<T> getList(String fieldName) {
            throw new UnsupportedOperationException(
                    "Array is not a supported partition type.");
        }

        @Override
        public <K, V> Map<K, V> getMap(String fieldName) {
            throw new UnsupportedOperationException(
                    "Map is not a supported partition type.");
        }
    }
}
