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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.type.VarcharType;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.connector.metadata.TableMetaMetadata.METADATA_DB_NAME;
import static com.starrocks.type.ArrayType.ARRAY_BIGINT;
import static com.starrocks.type.ArrayType.ARRAY_INT;
import static com.starrocks.type.IntegerType.BIGINT;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.VarbinaryType.VARBINARY;

public class LogicalIcebergMetadataTable extends MetadataTable {
    public static final String TABLE_NAME = "starrocks_connector_iceberg_logical_metadata_table";

    public LogicalIcebergMetadataTable(String catalogName, long id, String name, TableType type,
                                       List<Column> baseSchema, String originDb, String originTable,
                                       MetadataTableType metadataTableType) {
        super(catalogName, id, name, type, baseSchema, originDb, originTable, metadataTableType);
    }

    public static LogicalIcebergMetadataTable create(String catalogName, String originDb, String originTable) {
        return new LogicalIcebergMetadataTable(catalogName,
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                TABLE_NAME,
                Table.TableType.METADATA,
                builder()
                        .columns(PLACEHOLDER_COLUMNS)
                        .column("content", INT)
                        .column("file_path", VarcharType.VARCHAR)
                        .column("file_format", VarcharType.VARCHAR)
                        .column("spec_id", INT)
                        .column("partition_data", VARBINARY)
                        .column("record_count", BIGINT)
                        .column("file_size_in_bytes", BIGINT)
                        .column("split_offsets", ARRAY_BIGINT)
                        .column("sort_id", INT)
                        .column("equality_ids", ARRAY_INT)
                        .column("file_sequence_number", BIGINT)
                        .column("data_sequence_number", BIGINT)
                        .column("column_stats", VARBINARY)
                        .column("key_metadata", VARBINARY)
                        .build(),
                originDb,
                originTable,
                MetadataTableType.LOGICAL_ICEBERG_METADATA);
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.LOGICAL_ICEBERG_METADATA_TABLE,
                fullSchema.size() - PLACEHOLDER_COLUMNS.size(), 0, getName(), METADATA_DB_NAME);
        List<Column> columns = fullSchema.stream()
                .filter(c -> !PLACEHOLDER_COLUMNS.contains(c))
                .collect(Collectors.toList());

        THdfsTable hdfsTable = buildThriftTable(columns);
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
