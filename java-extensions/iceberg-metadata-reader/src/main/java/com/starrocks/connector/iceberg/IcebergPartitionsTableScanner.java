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

import com.google.common.collect.ImmutableList;
import com.starrocks.connector.share.iceberg.IcebergPartitionUtils;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iceberg.util.SerializationUtil.deserializeFromBase64;

public class IcebergPartitionsTableScanner extends AbstractIcebergMetadataScanner {
    protected static final List<String> SCAN_COLUMNS =
            ImmutableList.of("content", "partition", "file_size_in_bytes", "record_count");

    private final String manifestBean;
    private ManifestFile manifestFile;
    private CloseableIterator<? extends ContentFile<?>> reader;
    private List<PartitionField> partitionFields;
    private Long lastUpdateTime;
    private Integer spedId;
    private Schema schema;
    private GenericRecord reusedRecord;

    public IcebergPartitionsTableScanner(int fetchSize, Map<String, String> params) {
        super(fetchSize, params);
        this.manifestBean = params.get("split_info");
    }

    @Override
    public void doOpen() {
        this.manifestFile = deserializeFromBase64(manifestBean);
        this.schema = table.schema();
        this.spedId = manifestFile.partitionSpecId();
        this.lastUpdateTime = table.snapshot(manifestFile.snapshotId()) != null ?
                table.snapshot(manifestFile.snapshotId()).timestampMillis() : null;
        this.partitionFields = IcebergPartitionUtils.getAllPartitionFields(table);
        this.reusedRecord = GenericRecord.create(getResultType());
    }

    @Override
    public int doGetNext() {
        int numRows = 0;
        for (; numRows < getTableSize(); numRows++) {
            if (!reader.hasNext()) {
                break;
            }
            ContentFile<?> file = reader.next();
            for (int i = 0; i < requiredFields.length; i++) {
                Object fieldData = get(requiredFields[i], file);
                if (fieldData == null) {
                    appendData(i, null);
                } else {
                    ColumnValue fieldValue = new IcebergMetadataColumnValue(fieldData, timezone);
                    appendData(i, fieldValue);
                }
            }
        }
        return numRows;
    }

    @Override
    public void doClose() throws IOException {
        if (reader != null) {
            reader.close();
        }
        reusedRecord = null;

    }

    @Override
    protected void initReader() {
        Map<Integer, PartitionSpec> specs = table.specs();
        if (manifestFile.content() == ManifestContent.DATA) {
            reader = ManifestFiles.read(manifestFile, table.io(), specs)
                    .select(SCAN_COLUMNS)
                    .caseSensitive(false)
                    .iterator();
        } else {
            reader = ManifestFiles.readDeleteManifest(manifestFile, table.io(), specs)
                    .select(SCAN_COLUMNS)
                    .caseSensitive(false)
                    .iterator();
        }
    }

    private Types.StructType getResultType() {
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

    private Object get(String columnName, ContentFile<?> file) {
        FileContent content = file.content();
        switch (columnName) {
            case "partition_value":
                return getPartitionValues((PartitionData) file.partition());
            case "spec_id":
                return spedId;
            case "record_count":
                return content == FileContent.DATA ? file.recordCount() : 0;
            case "file_count":
                return content == FileContent.DATA ? 1L : 0L;
            case "total_data_file_size_in_bytes":
                return content == FileContent.DATA ? file.fileSizeInBytes() : 0;
            case "position_delete_record_count":
                return content == FileContent.POSITION_DELETES ? file.recordCount() : 0;
            case "position_delete_file_count":
                return content == FileContent.POSITION_DELETES ? 1L : 0L;
            case "equality_delete_record_count":
                return content == FileContent.EQUALITY_DELETES ? file.recordCount() : 0;
            case "equality_delete_file_count":
                return content == FileContent.EQUALITY_DELETES ? 1L : 0L;
            case "last_updated_at":
                return lastUpdateTime;
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }

    private Object getPartitionValues(PartitionData partitionData) {
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
                Type fieldType = partitionData.getType(pos);
                Object partitionValue = partitionData.get(pos);
                if (partitionField.transform().isIdentity() && Types.TimestampType.withZone().equals(fieldType)) {
                    partitionValue = ((long) partitionValue) / 1000;
                }
                reusedRecord.setField(name, partitionValue);
            } else {
                reusedRecord.setField(name, null);
            }
        }
        return reusedRecord;
    }
}
