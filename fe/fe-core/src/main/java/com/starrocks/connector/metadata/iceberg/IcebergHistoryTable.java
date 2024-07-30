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

import com.google.common.base.Joiner;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.connector.metadata.TableMetaMetadata.METADATA_DB_NAME;

public class IcebergHistoryTable extends MetadataTable {
    public static final String ICEBERG_REFS_TABLE_NAME = "iceberg_history_table";

    public IcebergHistoryTable(String catalogName, long id, String name, TableType type, List<Column> baseSchema,
                            String originDb, String originTable, MetadataTableType metadataTableType) {
        super(catalogName, id, name, type, baseSchema, originDb, originTable, metadataTableType);
    }

    public static IcebergHistoryTable create(String catalogName, String originDb, String originTable) {
        return new IcebergHistoryTable(catalogName,
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                ICEBERG_REFS_TABLE_NAME,
                Table.TableType.METADATA,
                builder()
                        .column("made_current_at", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("snapshot_id", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("parent_id", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("is_current_ancestor", ScalarType.createType(PrimitiveType.VARCHAR))
                        .build(),
                originDb,
                originTable,
                MetadataTableType.HISTORY);
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ICEBERG_HISTORY_TABLE,
                fullSchema.size(), 0, getName(), METADATA_DB_NAME);
        THdfsTable hdfsTable = new THdfsTable();

        hdfsTable.setColumns(fullSchema.stream().map(Column::toThrift).collect(Collectors.toList()));
        hdfsTable.setPartition_columnsIsSet(false);

        String columnNames = Joiner.on(',').join(fullSchema.stream()
                .map(Column::getName)
                .collect(Collectors.toList()));
        hdfsTable.setHive_column_names(columnNames);

        String columnTypes = Joiner.on(',').join(fullSchema.stream()
                .map(x -> ColumnTypeConverter.toHiveType(x.getType()))
                .collect(Collectors.toList()));
        hdfsTable.setHive_column_types(columnTypes);
        hdfsTable.setTime_zone(TimeUtils.getSessionTimeZone());
        tTableDescriptor.setHdfsTable(hdfsTable);
        return tTableDescriptor;
    }

    @Override
    public boolean supportBuildPlan() {
        return true;
    }
}
