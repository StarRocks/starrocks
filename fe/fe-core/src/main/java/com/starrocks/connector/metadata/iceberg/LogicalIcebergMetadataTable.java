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
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;

import java.util.List;

import static com.starrocks.catalog.Type.ARRAY_BIGINT;
import static com.starrocks.catalog.Type.ARRAY_INT;

public class LogicalIcebergMetadataTable extends MetadataTable {
    public static final String ICEBERG_LOGICAL_METADATA_TABLE_NAME = "starrocks_connector_iceberg_logical_metadata_table";

    public LogicalIcebergMetadataTable(String catalogName, long id, String name, TableType type,
                                       List<Column> baseSchema, String originDb, String originTable,
                                       MetadataTableType metadataTableType) {
        super(catalogName, id, name, type, baseSchema, originDb, originTable, metadataTableType);
    }

    public static LogicalIcebergMetadataTable create(String catalogName, String originDb, String originTable) {
        return new LogicalIcebergMetadataTable(catalogName,
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                ICEBERG_LOGICAL_METADATA_TABLE_NAME,
                Table.TableType.METADATA,
                builder()
                        .columns(PLACEHOLDER_COLUMNS)
                        .column("content", ScalarType.createType(PrimitiveType.INT))
                        .column("file_path", ScalarType.createVarcharType())
                        .column("file_format", ScalarType.createVarcharType())
                        .column("spec_id", ScalarType.createType(PrimitiveType.INT))
                        .column("partition_data", ScalarType.createType(PrimitiveType.VARBINARY))
                        .column("record_count", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("file_size_in_bytes", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("split_offsets", ARRAY_BIGINT)
                        .column("sort_id", ScalarType.createType(PrimitiveType.INT))
                        .column("equality_ids", ARRAY_INT)
                        .column("file_sequence_number", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("data_sequence_number", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("column_stats", ScalarType.createType(PrimitiveType.VARBINARY))
                        .build(),
                originDb,
                originTable,
                MetadataTableType.LOGICAL_ICEBERG_METADATA);
    }
}
