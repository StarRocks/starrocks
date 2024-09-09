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

import com.google.common.collect.ImmutableMap;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.starrocks.connector.iceberg.IcebergMetadataScanner.DELETE_SCAN_COLUMNS;
import static com.starrocks.connector.iceberg.IcebergMetadataScanner.DELETE_SCAN_WITH_STATS_COLUMNS;
import static com.starrocks.connector.iceberg.IcebergMetadataScanner.SCAN_COLUMNS;
import static com.starrocks.connector.iceberg.IcebergMetadataScanner.SCAN_WITH_STATS_COLUMNS;
import static org.apache.iceberg.util.ByteBuffers.toByteArray;
import static org.apache.iceberg.util.SerializationUtil.deserializeFromBase64;

public class IcebergFilesTableScanner extends AbstractIcebergMetadataScanner {
    private final String manifestBean;
    private final boolean loadColumnStats;
    private ManifestFile manifestFile;
    private CloseableIterator<? extends ContentFile<?>> reader;
    private Map<Integer, Type> idToTypeMapping;

    public IcebergFilesTableScanner(int fetchSize, Map<String, String> params) {
        super(fetchSize, params);
        this.manifestBean = params.get("split_info");
        this.loadColumnStats = Boolean.parseBoolean(params.get("load_column_stats"));
    }

    @Override
    public void doOpen() {
        this.manifestFile = deserializeFromBase64(manifestBean);
        this.idToTypeMapping = getIcebergIdToTypeMapping(table.schema());
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
    }

    @Override
    protected void initReader() {
        Map<Integer, PartitionSpec> specs = table.specs();
        List<String> scanColumns;
        if (manifestFile.content() == ManifestContent.DATA) {
            scanColumns = loadColumnStats ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS;
            reader = ManifestFiles.read(manifestFile, table.io(), specs)
                    .select(scanColumns)
                    .caseSensitive(false)
                    .iterator();
        } else {
            scanColumns = loadColumnStats ? DELETE_SCAN_WITH_STATS_COLUMNS : DELETE_SCAN_COLUMNS;
            reader = ManifestFiles.readDeleteManifest(manifestFile, table.io(), specs)
                    .select(scanColumns)
                    .caseSensitive(false)
                    .iterator();
        }
    }

    private Object get(String columnName, ContentFile<?> file) {
        switch (columnName) {
            case "content":
                return file.content().id();
            case "file_path":
                return file.path().toString();
            case "file_format":
                return file.format().toString();
            case "spec_id":
                return file.specId();
            case "record_count":
                return file.recordCount();
            case "file_size_in_bytes":
                return file.fileSizeInBytes();
            case "column_sizes":
                return file.columnSizes();
            case "value_counts":
                return file.valueCounts();
            case "null_value_counts":
                return file.nullValueCounts();
            case "nan_value_counts":
                return file.nanValueCounts();
            case "lower_bounds":
                return getIntegerStringMap(file.lowerBounds());
            case "upper_bounds":
                return getIntegerStringMap(file.upperBounds());
            case "split_offsets":
                return file.splitOffsets();
            case "sort_id":
                return file.sortOrderId();
            case "equality_ids":
                return file.equalityFieldIds() != null ? file.equalityFieldIds() : null;
            case "key_metadata":
                return file.keyMetadata() == null ? null : toByteArray(file.keyMetadata());
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }

    private Map<Integer, String> getIntegerStringMap(Map<Integer, ByteBuffer> value) {
        if (value == null) {
            return null;
        }
        return value.entrySet().stream()
                .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                .collect(toImmutableMap(
                        Map.Entry<Integer, ByteBuffer>::getKey,
                        entry -> Transforms.identity().toHumanString(
                                idToTypeMapping.get(entry.getKey()),
                                Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue()))));
    }

    private static Map<Integer, Type> getIcebergIdToTypeMapping(Schema schema) {
        ImmutableMap.Builder<Integer, Type> icebergIdToTypeMapping = ImmutableMap.builder();
        for (Types.NestedField field : schema.columns()) {
            fillIcebergIdToTypeMapping(field, icebergIdToTypeMapping);
        }
        return icebergIdToTypeMapping.build();
    }

    private static void fillIcebergIdToTypeMapping(Types.NestedField field,
                                                   ImmutableMap.Builder<Integer, Type> icebergIdToTypeMapping) {
        Type type = field.type();
        icebergIdToTypeMapping.put(field.fieldId(), type);
        if (type instanceof Type.NestedType) {
            type.asNestedType().fields().forEach(child -> fillIcebergIdToTypeMapping(child, icebergIdToTypeMapping));
        }
    }
}
