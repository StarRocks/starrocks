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

import com.starrocks.connector.share.iceberg.IcebergPartitionUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Converts a file's partition tuple to a struct over the partition fields of all the table's specs, the
// layout of the partition struct column in the metadata tables. A field the file's spec lacks is null.
public class IcebergPartitionValueReader {
    private final List<PartitionField> partitionFields;
    private final GenericRecord reusedRecord;

    public IcebergPartitionValueReader(Table table) {
        this(IcebergPartitionUtils.getAllPartitionFields(table), table.schema());
    }

    IcebergPartitionValueReader(List<PartitionField> partitionFields, Schema schema) {
        this.partitionFields = partitionFields;
        this.reusedRecord = GenericRecord.create(getResultType(partitionFields, schema));
    }

    private static Types.StructType getResultType(List<PartitionField> partitionFields, Schema schema) {
        List<Types.NestedField> fields = new ArrayList<>();
        for (PartitionField partitionField : partitionFields) {
            int id = partitionField.fieldId();
            String name = partitionField.name();
            Type type = partitionField.transform().getResultType(schema.findType(partitionField.sourceId()));
            Types.NestedField nestedField = Types.NestedField.optional(id, name, type);
            fields.add(nestedField);
        }
        return Types.StructType.of(fields);
    }

    // Empty when no spec of the table has a partition field; the metadata tables omit the partition column then.
    public Types.StructType type() {
        return reusedRecord.struct();
    }

    // The returned record is reused by the next call.
    public GenericRecord read(PartitionData partitionData) {
        List<Types.NestedField> fileFields = partitionData.getPartitionType().fields();
        Map<Integer, Integer> fieldIdToPos = new HashMap<>();
        for (int i = 0; i < fileFields.size(); i++) {
            fieldIdToPos.put(fileFields.get(i).fieldId(), i);
        }

        for (PartitionField partitionField : partitionFields) {
            Integer fieldId = partitionField.fieldId();
            String name = partitionField.name();
            if (fieldIdToPos.containsKey(fieldId)) {
                int pos = fieldIdToPos.get(fieldId);
                Object partitionValue = partitionData.get(pos);
                reusedRecord.setField(name,
                        partitionValue == null ? null : toColumnValue(partitionData.getType(pos), partitionValue));
            } else {
                reusedRecord.setField(name, null);
            }
        }
        return reusedRecord;
    }

    // Converts a partition value to what IcebergMetadataColumnValue reads for the column type the FE gives it.
    private static Object toColumnValue(Type type, Object value) {
        switch (type.typeId()) {
            case TIMESTAMP:
                long micros = (long) value;
                // DATETIME takes epoch millis, shown in the session time zone, or a wall-clock time
                return ((Types.TimestampType) type).shouldAdjustToUTC() ?
                        micros / 1000 : DateTimeUtil.timestampFromMicros(micros);
            case BINARY:
                return ByteBuffers.toByteArray((ByteBuffer) value);
            case UUID:
                return value instanceof ByteBuffer ? UUIDUtil.convert((ByteBuffer) value).toString() : value.toString();
            default:
                return value;
        }
    }
}
