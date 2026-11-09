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
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.VarcharType;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.connector.metadata.TableMetaMetadata.METADATA_DB_NAME;
import static com.starrocks.type.ArrayType.ARRAY_BIGINT;
import static com.starrocks.type.ArrayType.ARRAY_INT;
import static com.starrocks.type.IntegerType.BIGINT;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.VarbinaryType.VARBINARY;

// The manifest entries of a snapshot, live and DELETED, like Trino's and Spark's $entries.
public class IcebergEntriesTable extends MetadataTable {
    public static final String TABLE_NAME = "iceberg_entries_table";

    public IcebergEntriesTable(String catalogName, long id, String name, Table.TableType type,
                               List<Column> baseSchema, String originDb, String originTable,
                               MetadataTableType metadataTableType) {
        super(catalogName, id, name, type, baseSchema, originDb, originTable, metadataTableType);
    }

    // ENTRIES lists the entries of a snapshot's manifests; ALL_ENTRIES those of every manifest reachable from any
    // snapshot
    public static IcebergEntriesTable create(ConnectContext context, String catalogName, String originDb,
                                             String originTable, MetadataTableType metadataTableType) {
        StructType partitionType = IcebergFilesTable.getPartitionType(context, catalogName, originDb, originTable);
        return new IcebergEntriesTable(catalogName,
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asLong(),
                TABLE_NAME,
                Table.TableType.METADATA,
                builder()
                        // 0: EXISTING, 1: ADDED, 2: DELETED
                        .column("status", INT)
                        .column("snapshot_id", BIGINT)
                        .column("sequence_number", BIGINT)
                        .column("file_sequence_number", BIGINT)
                        .column("data_file", getDataFileType(partitionType))
                        .build(),
                originDb,
                originTable,
                metadataTableType);
    }

    // The $files columns, with Trino's and Spark's sort_order_id for $files' sort_id. Keep the field order in sync
    // with IcebergEntriesTableScanner.
    private static StructType getDataFileType(StructType partitionType) {
        List<StructField> fields = new ArrayList<>();
        fields.add(new StructField("content", INT));
        fields.add(new StructField("file_path", VarcharType.VARCHAR));
        fields.add(new StructField("file_format", VarcharType.VARCHAR));
        fields.add(new StructField("spec_id", INT));
        if (partitionType != null) {
            fields.add(new StructField("partition", partitionType));
        }
        fields.add(new StructField("record_count", BIGINT));
        fields.add(new StructField("file_size_in_bytes", BIGINT));
        fields.add(new StructField("column_sizes", new MapType(INT, BIGINT)));
        fields.add(new StructField("value_counts", new MapType(INT, BIGINT)));
        fields.add(new StructField("null_value_counts", new MapType(INT, BIGINT)));
        fields.add(new StructField("nan_value_counts", new MapType(INT, BIGINT)));
        fields.add(new StructField("lower_bounds", new MapType(INT, VarcharType.VARCHAR)));
        fields.add(new StructField("upper_bounds", new MapType(INT, VarcharType.VARCHAR)));
        fields.add(new StructField("key_metadata", VARBINARY));
        fields.add(new StructField("split_offsets", ARRAY_BIGINT));
        fields.add(new StructField("equality_ids", ARRAY_INT));
        fields.add(new StructField("sort_order_id", INT));
        fields.add(new StructField("first_row_id", BIGINT));
        fields.add(new StructField("referenced_data_file", VarcharType.VARCHAR));
        fields.add(new StructField("content_offset", BIGINT));
        fields.add(new StructField("content_size_in_bytes", BIGINT));
        return new StructType(fields, true);
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        // BE handles every iceberg metadata table the same way and picks the scanner by the metadata table type
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ICEBERG_FILES_TABLE,
                fullSchema.size(), 0, getName(), METADATA_DB_NAME);
        THdfsTable hdfsTable = buildThriftTable(fullSchema);
        tTableDescriptor.setHdfsTable(hdfsTable);
        return tTableDescriptor;
    }

    @Override
    public boolean isTemporal() {
        return getMetadataTableType() == MetadataTableType.ENTRIES;
    }

    @Override
    public boolean supportBuildPlan() {
        return true;
    }
}
