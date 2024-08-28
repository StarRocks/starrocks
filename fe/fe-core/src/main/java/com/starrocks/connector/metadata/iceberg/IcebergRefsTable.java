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
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.List;

import static com.starrocks.connector.metadata.TableMetaMetadata.METADATA_DB_NAME;

public class IcebergRefsTable extends MetadataTable {
    public static final String TABLE_NAME = "iceberg_refs_table";

    public IcebergRefsTable(String catalogName, long id, String name, TableType type, List<Column> baseSchema,
                            String originDb, String originTable, MetadataTableType metadataTableType) {
        super(catalogName, id, name, type, baseSchema, originDb, originTable, metadataTableType);
    }

    public static IcebergRefsTable create(String catalogName, String originDb, String originTable) {
        return new IcebergRefsTable(catalogName,
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                TABLE_NAME,
                Table.TableType.METADATA,
                builder()
                        .column("name", ScalarType.createVarcharType())
                        .column("type", ScalarType.createVarcharType())
                        .column("snapshot_id", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("max_reference_age_in_ms", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("min_snapshots_to_keep", ScalarType.createType(PrimitiveType.INT))
                        .column("max_snapshot_age_in_ms", ScalarType.createType(PrimitiveType.BIGINT))
                        .build(),
                originDb,
                originTable,
                MetadataTableType.REFS);
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ICEBERG_REFS_TABLE,
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
