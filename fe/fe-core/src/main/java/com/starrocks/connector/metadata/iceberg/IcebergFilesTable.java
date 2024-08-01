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

package com.starrocks.connector.metadata.iceberg;

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.List;

import static com.starrocks.catalog.ScalarType.createType;
import static com.starrocks.catalog.ScalarType.createVarcharType;
import static com.starrocks.catalog.Type.ARRAY_BIGINT;
import static com.starrocks.catalog.Type.ARRAY_INT;
import static com.starrocks.connector.metadata.TableMetaMetadata.METADATA_DB_NAME;

public class IcebergFilesTable extends MetadataTable {
    public static final String TABLE_NAME = "iceberg_files_table";

    public IcebergFilesTable(String catalogName, long id, String name, TableType type,
                             List<Column> baseSchema, String originDb, String originTable,
                             MetadataTableType metadataTableType) {
        super(catalogName, id, name, type, baseSchema, originDb, originTable, metadataTableType);
    }

    public static IcebergFilesTable create(String catalogName, String originDb, String originTable) {
        return new IcebergFilesTable(catalogName,
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                TABLE_NAME,
                Table.TableType.METADATA,
                builder()
                        .column("content", createType(PrimitiveType.INT))
                        .column("file_path", createVarcharType())
                        .column("file_format", createVarcharType())
                        .column("spec_id", createType(PrimitiveType.INT))
                        .column("record_count", createType(PrimitiveType.BIGINT))
                        .column("file_size_in_bytes", createType(PrimitiveType.BIGINT))
                        .column("column_sizes", new MapType(createType(PrimitiveType.INT), createType(PrimitiveType.BIGINT)))
                        .column("value_counts", new MapType(createType(PrimitiveType.INT), createType(PrimitiveType.BIGINT)))
                        .column("null_value_counts", new MapType(
                                createType(PrimitiveType.INT), createType(PrimitiveType.BIGINT)))
                        .column("nan_value_counts", new MapType(createType(PrimitiveType.INT), createType(PrimitiveType.BIGINT)))
                        .column("lower_bounds", new MapType(createType(PrimitiveType.INT), createVarcharType()))
                        .column("upper_bounds", new MapType(createType(PrimitiveType.INT), createVarcharType()))
                        .column("split_offsets", ARRAY_BIGINT)
                        .column("sort_id", createType(PrimitiveType.INT))
                        .column("equality_ids", ARRAY_INT)
                        .column("key_metadata", createType(PrimitiveType.VARBINARY))
                        .build(),
                originDb,
                originTable,
                MetadataTableType.FILES);
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ICEBERG_FILES_TABLE,
                fullSchema.size(), 0, getName(), METADATA_DB_NAME);
        THdfsTable hdfsTable = buildThriftTable(fullSchema);
        tTableDescriptor.setHdfsTable(hdfsTable);
        return tTableDescriptor;
    }

    @Override
    public boolean isTemporal() {
        return true;
    }

    @Override
    public boolean supportBuildPlan() {
        return true;
    }
}
