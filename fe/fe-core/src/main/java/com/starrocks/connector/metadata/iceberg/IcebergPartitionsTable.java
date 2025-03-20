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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.connector.share.iceberg.IcebergPartitionUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.iceberg.PartitionField;

import java.util.List;

import static com.starrocks.catalog.ScalarType.createType;
import static com.starrocks.connector.metadata.TableMetaMetadata.METADATA_DB_NAME;

public class IcebergPartitionsTable extends MetadataTable {
    public static final String TABLE_NAME = "iceberg_partitions_table";

    public IcebergPartitionsTable(String catalogName, long id, String name, Table.TableType type,
                             List<Column> baseSchema, String originDb, String originTable,
                             MetadataTableType metadataTableType) {
        super(catalogName, id, name, type, baseSchema, originDb, originTable, metadataTableType);
    }

    public static IcebergPartitionsTable create(ConnectContext context, String catalogName, String originDb, String originTable) {
        Table icebergTable = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(context, catalogName, originDb, originTable);
        if (icebergTable == null) {
            throw new StarRocksConnectorException("table [%s.%s.%s] does not exist", catalogName, originDb, originTable);
        }
        if (!icebergTable.isIcebergTable()) {
            throw new StarRocksConnectorException("table [%s.%s.%s] isn't an iceberg table", catalogName, originDb, originTable);
        }
        org.apache.iceberg.Table table = ((IcebergTable) icebergTable).getNativeTable();
        Builder builder = builder();
        if (table.spec().isPartitioned()) {
            List<PartitionField> partitionFields = IcebergPartitionUtils.getAllPartitionFields(table);
            List<StructField> partitionStructFields = IcebergApiConverter.getPartitionColumns(partitionFields, table.schema());
            StructType partitionType = new StructType(partitionStructFields, true);
            builder.column("partition_value", partitionType);
            builder.column("spec_id", createType(PrimitiveType.INT));
        }

        return new IcebergPartitionsTable(catalogName,
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                TABLE_NAME,
                Table.TableType.METADATA,
                builder.column("record_count", createType(PrimitiveType.BIGINT))
                        .column("file_count", createType(PrimitiveType.BIGINT))
                        .column("total_data_file_size_in_bytes", createType(PrimitiveType.BIGINT))
                        .column("position_delete_record_count", createType(PrimitiveType.BIGINT))
                        .column("position_delete_file_count", createType(PrimitiveType.BIGINT))
                        .column("equality_delete_record_count", createType(PrimitiveType.BIGINT))
                        .column("equality_delete_file_count", createType(PrimitiveType.BIGINT))
                        .column("last_updated_at", createType(PrimitiveType.DATETIME))
                        .build(),
                originDb,
                originTable,
                MetadataTableType.PARTITIONS);
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ICEBERG_PARTITIONS_TABLE,
                fullSchema.size(), 0, getName(), METADATA_DB_NAME);
        THdfsTable hdfsTable = buildThriftTable(fullSchema);
        tTableDescriptor.setHdfsTable(hdfsTable);
        return tTableDescriptor;
    }

    @Override
    public boolean supportBuildPlan() {
        return true;
    }
}
