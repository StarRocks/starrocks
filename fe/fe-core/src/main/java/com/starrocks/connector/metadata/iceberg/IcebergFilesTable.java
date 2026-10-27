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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.connector.share.iceberg.IcebergPartitionUtils;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.PartitionField;

import java.util.List;

import static com.starrocks.connector.metadata.TableMetaMetadata.METADATA_DB_NAME;
import static com.starrocks.type.ArrayType.ARRAY_BIGINT;
import static com.starrocks.type.ArrayType.ARRAY_INT;
import static com.starrocks.type.IntegerType.BIGINT;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.VarbinaryType.VARBINARY;

public class IcebergFilesTable extends MetadataTable {
    public static final String TABLE_NAME = "iceberg_files_table";

    public IcebergFilesTable(String catalogName, long id, String name, TableType type,
                             List<Column> baseSchema, String originDb, String originTable,
                             MetadataTableType metadataTableType) {
        super(catalogName, id, name, type, baseSchema, originDb, originTable, metadataTableType);
    }

    // FILES lists the live files of a snapshot; ALL_DATA_FILES and ALL_DELETE_FILES list the live files of every
    // manifest reachable from any snapshot, so a file can appear more than once
    public static IcebergFilesTable create(ConnectContext context, String catalogName, String originDb,
                                           String originTable, MetadataTableType metadataTableType) {
        Builder builder = builder()
                .column("content", INT)
                .column("file_path", VarcharType.VARCHAR)
                .column("file_format", VarcharType.VARCHAR)
                .column("spec_id", INT);
        // the partition values over the partition fields of all specs, absent for a never-partitioned table
        StructType partitionType = getPartitionType(context, catalogName, originDb, originTable);
        if (partitionType != null) {
            builder.column("partition", partitionType);
        }
        return new IcebergFilesTable(catalogName,
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asLong(),
                TABLE_NAME,
                Table.TableType.METADATA,
                builder.column("record_count", BIGINT)
                        .column("file_size_in_bytes", BIGINT)
                        .column("column_sizes", new MapType(INT, BIGINT))
                        .column("value_counts", new MapType(INT, BIGINT))
                        .column("null_value_counts", new MapType(INT, BIGINT))
                        .column("nan_value_counts", new MapType(INT, BIGINT))
                        .column("lower_bounds", new MapType(INT, VarcharType.VARCHAR))
                        .column("upper_bounds", new MapType(INT, VarcharType.VARCHAR))
                        .column("split_offsets", ARRAY_BIGINT)
                        .column("sort_id", INT)
                        .column("equality_ids", ARRAY_INT)
                        .column("key_metadata", VARBINARY)
                        // format v3: the first row id of a data file, and the data file and blob range a deletion
                        // vector applies to
                        .column("first_row_id", BIGINT)
                        .column("referenced_data_file", VarcharType.VARCHAR)
                        .column("content_offset", BIGINT)
                        .column("content_size_in_bytes", BIGINT)
                        .build(),
                originDb,
                originTable,
                metadataTableType);
    }

    static StructType getPartitionType(ConnectContext context, String catalogName, String originDb,
                                       String originTable) {
        Table icebergTable = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(context, catalogName, originDb, originTable);
        if (icebergTable == null) {
            throw new StarRocksConnectorException("table [%s.%s.%s] does not exist", catalogName, originDb, originTable);
        }
        if (!icebergTable.isIcebergTable()) {
            throw new StarRocksConnectorException("table [%s.%s.%s] isn't an iceberg table", catalogName, originDb,
                    originTable);
        }
        org.apache.iceberg.Table table = ((IcebergTable) icebergTable).getNativeTable();
        List<PartitionField> partitionFields = IcebergPartitionUtils.getAllPartitionFields(table);
        if (partitionFields.isEmpty()) {
            return null;
        }
        return new StructType(IcebergApiConverter.getPartitionColumns(partitionFields, table.schema()), true);
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
        return getMetadataTableType() == MetadataTableType.FILES;
    }

    @Override
    public boolean supportBuildPlan() {
        return true;
    }
}
