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

package com.starrocks.connector.iceberg;

import com.google.common.base.Preconditions;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.sql.common.DmlException;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergPartitionData implements StructLike {
    private final Object[] values;

    public IcebergPartitionData(int size) {
        this.values = new Object[size];
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        Object value = values[pos];
        if (javaClass == ByteBuffer.class && value instanceof byte[]) {
            value = ByteBuffer.wrap((byte[]) value);
        }
        return javaClass.cast(value);
    }

    @Override
    public <T> void set(int pos, T value) {
        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) value;
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            values[pos] = bytes;
        } else {
            values[pos] = value;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        IcebergPartitionData that = (IcebergPartitionData) other;
        return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    public static IcebergPartitionData partitionDataFromPath(String relativePartitionPath, PartitionSpec spec) {
        Set<String> partitionNames = spec.fields().stream().map(PartitionField::name).collect(Collectors.toSet());
        Set<String> validPathPartitionNames = new HashSet<>();
        String[] partitions = relativePartitionPath.split("/", -1);
        StringBuilder sb = new StringBuilder();
        for (String part : partitions) {
            String[] parts = part.split("=", 2);
            Preconditions.checkArgument(parts.length == 2, "Invalid partition: %s", part);
            if (!partitionNames.contains(parts[0])) {
                throw new StarRocksConnectorException("Partition column %s not found in iceberg partition columns", parts[0]);
            } else if (parts[1].equals(HiveMetaClient.PARTITION_NULL_VALUE)) {
                sb.append('1');
            } else {
                sb.append('0');
            }
            validPathPartitionNames.add(parts[0]);
        }
        partitionNames.forEach(name -> {
                if (!validPathPartitionNames.contains(name)) {
                    throw new StarRocksConnectorException("Partition column %s not found in path %s",
                            name, relativePartitionPath);
                }
            }
        );
        return partitionDataFromPath(relativePartitionPath, sb.toString(), spec);
    }

    public static IcebergPartitionData partitionDataFromPath(String relativePartitionPath,
                                                             String partitionNullFingerprint, PartitionSpec spec) {
        IcebergPartitionData data = new IcebergPartitionData(spec.fields().size());
        String[] partitions = relativePartitionPath.split("/", -1);
        List<PartitionField> partitionFields = spec.fields();
        if (partitions.length != partitionNullFingerprint.length()) {
            throw new InternalError("Invalid partition and fingerprint size, partition:" + relativePartitionPath +
                    " partition size:" + partitions.length + " fingerprint:" + partitionNullFingerprint);
        }
        for (int i = 0; i < partitions.length; i++) {
            PartitionField field = partitionFields.get(i);
            String[] parts = partitions[i].split("=", 2);
            Preconditions.checkArgument(parts.length == 2 && parts[0] != null &&
                    field.name().equals(parts[0]), "Invalid partition: %s", partitions[i]);
            org.apache.iceberg.types.Type resultType = spec.partitionType().fields().get(i).type();
            // org.apache.iceberg.types.Type sourceType = table.schema().findType(field.sourceId());
            // NOTICE:
            // The behavior here should match the make_partition_path method in be,
            // and revert the String path value to the origin value and type of transform expr for the metastore.
            // otherwise, if we use the api of iceberg to filter the scan files, the result may be incorrect!
            if (partitionNullFingerprint.charAt(i) == '0') { //'0' means not null, '1' means null
                // apply date decoding for date type
                String transform = field.transform().toString();
                if (transform.equals("year") || transform.equals("month")
                        || transform.equals("day") || transform.equals("hour")) {
                    int year = org.apache.iceberg.util.DateTimeUtil.EPOCH.getYear();
                    int month = org.apache.iceberg.util.DateTimeUtil.EPOCH.getMonthValue();
                    int day = org.apache.iceberg.util.DateTimeUtil.EPOCH.getDayOfMonth();
                    int hour = org.apache.iceberg.util.DateTimeUtil.EPOCH.getHour();
                    String[] dateParts = parts[1].split("-");
                    if (dateParts.length > 0) {
                        year = Integer.parseInt(dateParts[0]);
                    }
                    if (dateParts.length > 1) {
                        month = Integer.parseInt(dateParts[1]);
                    }
                    if (dateParts.length > 2) {
                        day = Integer.parseInt(dateParts[2]);
                    }
                    if (dateParts.length > 3) {
                        hour = Integer.parseInt(dateParts[3]);
                    }
                    LocalDateTime target = LocalDateTime.of(year, month, day, hour, 0);
                    if (transform.equals("year")) {
                        //iceberg stores the result of transform as metadata.
                        parts[1] = String.valueOf(
                                ChronoUnit.YEARS.between(org.apache.iceberg.util.DateTimeUtil.EPOCH_DAY, target));
                    } else if (transform.equals("month")) {
                        parts[1] = String.valueOf(
                                ChronoUnit.MONTHS.between(org.apache.iceberg.util.DateTimeUtil.EPOCH_DAY, target));
                    } else if (transform.equals("day")) {
                        //The reuslt of day transform is a date type.
                        //It is diffrent from other date transform exprs, however other's result is a integer.
                        //do nothing
                    } else if (transform.equals("hour")) {
                        parts[1] = String.valueOf(
                                ChronoUnit.HOURS.between(org.apache.iceberg.util.DateTimeUtil.EPOCH_DAY.atTime(0, 0), target));
                    }
                } else if (transform.startsWith("truncate")) {
                    //the result type of truncate is the same as the truncate column
                    if (parts[1].isEmpty()) {
                        //do nothing
                    } else if (resultType.typeId() == Type.TypeID.STRING || resultType.typeId() == Type.TypeID.FIXED) {
                        parts[1] = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                    } else if (resultType.typeId() == Type.TypeID.BINARY) {
                        parts[1] = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                        //Do not convert the byte array to utf-8, because some byte is not valid in utf-8.
                        //like 0xE6 is not valid in utf8. If the convert failed, utf-8 will transfer the byte to 0xFFFD as default
                        //we should just read and store the byte in latin, and thus not change the byte array value.
                        parts[1] = new String(Base64.getDecoder().decode(parts[1]), StandardCharsets.ISO_8859_1);
                    }
                } else if (transform.startsWith("bucket")) {
                    //the result type of bucket is integer.
                    //do nothing
                } else if (transform.equals("identity")) {
                    if (parts[1].isEmpty()) {
                        //do nothing
                    } else if (resultType.typeId() == Type.TypeID.STRING || resultType.typeId() == Type.TypeID.FIXED) {
                        parts[1] = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                    } else if (resultType.typeId() == Type.TypeID.BINARY) {
                        parts[1] = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                        parts[1] = new String(Base64.getDecoder().decode(parts[1]), StandardCharsets.ISO_8859_1);
                    } else if (resultType.typeId() == Type.TypeID.TIMESTAMP) {
                        parts[1] = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                        parts[1] = parts[1].replace(' ', 'T');
                    }
                } else {
                    throw new DmlException("Unsupported partition transform: %s", transform);
                }
            }

            if (partitionNullFingerprint.charAt(i) == '1') {
                data.set(i, null);
            } else if (resultType.typeId() == Type.TypeID.BINARY) {
                data.set(i, parts[1].getBytes(StandardCharsets.ISO_8859_1));
            } else if (resultType.typeId() == Type.TypeID.TIMESTAMP) {
                data.set(i, Literal.of(parts[1]).to(Types.TimestampType.withoutZone()).value());
            } else {
                data.set(i, Conversions.fromPartitionString(resultType, parts[1]));
            }
        }
        return data;
    }
}
