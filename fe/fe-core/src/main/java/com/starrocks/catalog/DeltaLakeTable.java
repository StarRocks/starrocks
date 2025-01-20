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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.connector.delta.DeltaUtils;
import com.starrocks.server.CatalogMgr;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TDeltaLakeTable;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsPartitionLocation;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DeltaLakeTable extends Table {
    private String catalogName;
    private String dbName;
    private String tableName;
    private List<String> partColumnNames;
    private SnapshotImpl deltaSnapshot;
    private String tableLocation;
    private Engine deltaEngine;

    public static final String PARTITION_NULL_VALUE = "null";

    public DeltaLakeTable() {
        super(TableType.DELTALAKE);
    }

    public DeltaLakeTable(long id, String catalogName, String dbName, String tableName, List<Column> schema,
                          List<String> partitionNames, SnapshotImpl deltaSnapshot, String tableLocation,
                          Engine deltaEngine, long createTime) {
        super(id, tableName, TableType.DELTALAKE, schema);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.partColumnNames = partitionNames;
        this.deltaSnapshot = deltaSnapshot;
        this.tableLocation = tableLocation;
        this.deltaEngine = deltaEngine;
        this.createTime = createTime;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public String getTableLocation() {
        return tableLocation;
    }

    public Metadata getDeltaMetadata() {
        return deltaSnapshot.getMetadata();
    }

    public Snapshot getDeltaSnapshot() {
        return deltaSnapshot;
    }

    public Engine getDeltaEngine() {
        return deltaEngine;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return dbName;
    }

    @Override
    public String getCatalogTableName() {
        return tableName;
    }

    @Override
    public String getUUID() {
        if (CatalogMgr.isExternalCatalog(catalogName)) {
            return String.join(".", catalogName, dbName, tableName, Long.toString(createTime));
        } else {
            return Long.toString(id);
        }
    }

    @Override
    public List<Column> getPartitionColumns() {
        return partColumnNames.stream()
                .map(name -> nameToColumn.get(name))
                .collect(Collectors.toList());
    }

    public List<String> getPartitionColumnNames() {
        return partColumnNames;
    }

    public boolean isUnPartitioned() {
        return partColumnNames.isEmpty();
    }

    public THdfsPartition toHdfsPartition(DescriptorTable.ReferencedPartitionInfo info) {
        Metadata deltaMetadata = getDeltaMetadata();
        PartitionKey key = info.getKey();
        THdfsPartition tPartition = new THdfsPartition();
        tPartition.setFile_format(DeltaUtils.getRemoteFileFormat(deltaMetadata.getFormat().getProvider()).toThrift());

        List<LiteralExpr> keys = key.getKeys();
        tPartition.setPartition_key_exprs(keys.stream().map(Expr::treeToThrift).collect(Collectors.toList()));

        THdfsPartitionLocation tPartitionLocation = new THdfsPartitionLocation();
        tPartitionLocation.setPrefix_index(-1);
        tPartitionLocation.setSuffix(info.getPath());
        tPartition.setLocation(tPartitionLocation);
        return tPartition;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        Preconditions.checkNotNull(partitions);

        TDeltaLakeTable tDeltaLakeTable = new TDeltaLakeTable();
        tDeltaLakeTable.setLocation(getTableLocation());

        Set<String> partitionColumnNames = Sets.newHashSet();
        List<TColumn> tPartitionColumns = Lists.newArrayList();
        List<TColumn> tColumns = Lists.newArrayList();

        for (Column column : getPartitionColumns()) {
            tPartitionColumns.add(column.toThrift());
            partitionColumnNames.add(column.getName());
        }

        for (Column column : getBaseSchema()) {
            if (partitionColumnNames.contains(column.getName())) {
                continue;
            }
            tColumns.add(column.toThrift());
        }
        tDeltaLakeTable.setColumns(tColumns);
        if (!tPartitionColumns.isEmpty()) {
            tDeltaLakeTable.setPartition_columns(tPartitionColumns);
        }

        for (DescriptorTable.ReferencedPartitionInfo info : partitions) {
            long partitionId = info.getId();
            THdfsPartition tPartition = toHdfsPartition(info);
            tDeltaLakeTable.putToPartitions(partitionId, tPartition);
        }

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.DELTALAKE_TABLE,
                fullSchema.size(), 0, tableName, dbName);
        tTableDescriptor.setDeltaLakeTable(tDeltaLakeTable);
        return tTableDescriptor;
    }
}
