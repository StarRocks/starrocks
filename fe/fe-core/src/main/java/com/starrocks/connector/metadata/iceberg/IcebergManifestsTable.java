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

import com.google.common.collect.Lists;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.List;

import static com.starrocks.connector.metadata.TableMetaMetadata.METADATA_DB_NAME;

public class IcebergManifestsTable extends MetadataTable {
    public static final String TABLE_NAME = "iceberg_manifests_table";

    public IcebergManifestsTable(String catalogName, long id, String name, TableType type, List<Column> baseSchema,
                               String originDb, String originTable, MetadataTableType metadataTableType) {
        super(catalogName, id, name, type, baseSchema, originDb, originTable, metadataTableType);
    }

    public static IcebergManifestsTable create(String catalogName, String originDb, String originTable) {
        return new IcebergManifestsTable(catalogName,
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                TABLE_NAME,
                Table.TableType.METADATA,
                builder()
                        .column("path", ScalarType.createType(PrimitiveType.VARCHAR))
                        .column("length", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("partition_spec_id", ScalarType.createType(PrimitiveType.INT))
                        .column("added_snapshot_id", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("added_data_files_count", ScalarType.createType(PrimitiveType.INT))
                        .column("added_rows_count", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("existing_data_files_count", ScalarType.createType(PrimitiveType.INT))
                        .column("existing_rows_count", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("deleted_data_files_count", ScalarType.createType(PrimitiveType.INT))
                        .column("deleted_rows_count", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("partitions", new ArrayType(
                                new StructType(Lists.newArrayList(
                                        new StructField("contains_null", ScalarType.createType(PrimitiveType.VARCHAR)),
                                        new StructField("contains_nan", ScalarType.createType(PrimitiveType.VARCHAR)),
                                        new StructField("lower_bound", ScalarType.createType(PrimitiveType.VARCHAR)),
                                        new StructField("upper_bound", ScalarType.createType(PrimitiveType.VARCHAR)))))
                        )
                        .build(),
                originDb,
                originTable,
                MetadataTableType.MANIFESTS);
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ICEBERG_MANIFESTS_TABLE,
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
