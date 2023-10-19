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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.ExternalMetadataTable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TGetIcebergFilesResponse;
import com.starrocks.thrift.TIcebergFile;
import com.starrocks.thrift.TSchemaTableType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class FilesTable extends SystemTable implements ExternalMetadataTable {
    private final IcebergTable icebergTable;
    private static final List<Column> METADATA_SCHEMAS = Lists.newArrayList(
            new Column("content", ScalarType.INT),
            new Column("file_path", ScalarType.VARCHAR),
            new Column("file_format", ScalarType.VARCHAR),
            new Column("spec_id", ScalarType.INT),
            new Column("record_count", ScalarType.BIGINT),
            new Column("file_size_in_bytes", ScalarType.BIGINT),
            new Column("column_sizes", ScalarType.VARCHAR, true),
            new Column("value_counts", ScalarType.VARCHAR, true),
            new Column("null_value_counts", ScalarType.VARCHAR, true),
            new Column("nan_value_counts", ScalarType.VARCHAR, true),
            new Column("lower_bounds", ScalarType.VARCHAR, true),
            new Column("upper_bounds", ScalarType.VARCHAR, true),
            new Column("split_offsets", ScalarType.VARCHAR, true),
            new Column("equality_ids", ScalarType.VARCHAR, true)
    );

    public FilesTable(String catalogName, IcebergTable icebergTable) {
        super(catalogName,
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                icebergTable.getRemoteTableName(),
                TableType.SCHEMA,
                METADATA_SCHEMAS,
                TSchemaTableType.SCH_ICEBERG_FILES);
        this.icebergTable = requireNonNull(icebergTable, "iceberg table is null");
    }

    public String getOriginDbName() {
        return icebergTable.getRemoteDbName();
    }

    public String getOriginTableName() {
        return icebergTable.getRemoteTableName();
    }

    public TGetIcebergFilesResponse getFiles(long limit) {
        TGetIcebergFilesResponse res = new TGetIcebergFilesResponse();
        List<TIcebergFile> tFiles = new ArrayList<>();
        res.setIceberg_files(tFiles);
        if (!icebergTable.getSnapshot().isPresent()) {
            return res;
        }

        TableScan scan = icebergTable.getNativeTable().newScan().includeColumnStats();
        CloseableIterator<FileScanTask> fileScanTaskIterator = scan.planFiles().iterator();
        Iterator<FileScanTask> fileScanTasks = Iterators.limit(fileScanTaskIterator, (int) limit);
        Map<Integer, Type> idToTypeMapping = getColIdToTypeMapping(icebergTable.getNativeTable().schema());
        while (fileScanTasks.hasNext()) {
            TIcebergFile tFile = new TIcebergFile();
            FileScanTask scanTask = fileScanTaskIterator.next();
            DataFile dataFile = scanTask.file();
            tFile.setContent(dataFile.content().id());
            tFile.setFile_path(dataFile.path().toString());
            tFile.setFile_format(dataFile.format().name());
            tFile.setSpec_id(dataFile.specId());
            tFile.setRecord_count(dataFile.recordCount());
            tFile.setFile_size_in_bytes(dataFile.fileSizeInBytes());
            if (dataFile.columnSizes() != null) {
                tFile.setColumn_sizes(GsonUtils.GSON.toJson(dataFile.columnSizes()));
            }
            if (dataFile.valueCounts() != null) {
                tFile.setValue_counts(GsonUtils.GSON.toJson(dataFile.valueCounts()));
            }
            if (dataFile.nullValueCounts() != null) {
                tFile.setNull_value_counts(GsonUtils.GSON.toJson(dataFile.nullValueCounts()));
            }
            if (dataFile.nanValueCounts() != null) {
                tFile.setNan_value_counts(GsonUtils.GSON.toJson(dataFile.nanValueCounts()));
            }
            if (dataFile.lowerBounds() != null) {
                tFile.setLower_bounds(getLowerBounds(dataFile, idToTypeMapping));
            }
            if (dataFile.upperBounds() != null) {
                tFile.setUpper_bounds(getUpperBounds(dataFile, idToTypeMapping));
            }
            if (dataFile.splitOffsets() != null) {
                tFile.setSplit_offsets(GsonUtils.GSON.toJson(dataFile.splitOffsets()));
            }
            if (dataFile.equalityFieldIds() != null) {
                tFile.setEquality_ids(GsonUtils.GSON.toJson(dataFile.equalityFieldIds()));
            }
            tFiles.add(tFile);
        }

        try {
            fileScanTaskIterator.close();
        } catch (IOException e) {
            // Ignored
        }

        return res;
    }

    private Map<Integer, Type> getColIdToTypeMapping(Schema schema) {
        Map<Integer, Type> colIdToTypeMapping = new HashMap<>();
        for (Types.NestedField field : schema.columns()) {
            walkColIdToTypeMapping(field, colIdToTypeMapping);
        }
        return colIdToTypeMapping;
    }

    private void walkColIdToTypeMapping(Types.NestedField field, Map<Integer, Type> idToTypeMapping) {
        Type type = field.type();
        idToTypeMapping.put(field.fieldId(), type);
        if (type instanceof Type.NestedType) {
            type.asNestedType().fields().forEach(child -> walkColIdToTypeMapping(child, idToTypeMapping));
        }
    }

    private String getLowerBounds(DataFile dataFile, Map<Integer, Type> idToTypeMapping) {
        Map<Integer, String> values = dataFile.lowerBounds().entrySet().stream()
                .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                .collect(toImmutableMap(
                        Map.Entry<Integer, ByteBuffer>::getKey,
                        entry -> Transforms.identity().toHumanString(
                                idToTypeMapping.get(entry.getKey()),
                                Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue()))));
        return GsonUtils.GSON.toJson(values);
    }

    private String getUpperBounds(DataFile dataFile, Map<Integer, Type> idToTypeMapping) {
        Map<Integer, String> values = dataFile.upperBounds().entrySet().stream()
                .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                .collect(toImmutableMap(
                        Map.Entry<Integer, ByteBuffer>::getKey,
                        entry -> Transforms.identity().toHumanString(
                                idToTypeMapping.get(entry.getKey()),
                                Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue()))));
        return GsonUtils.GSON.toJson(values);
    }
}
