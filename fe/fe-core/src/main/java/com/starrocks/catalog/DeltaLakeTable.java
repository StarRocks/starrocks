// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.external.delta.DeltaUtils;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TDeltaLakeTable;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsPartitionLocation;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.Metadata;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DeltaLakeTable extends Table {
    private String catalogName;
    private String dbName;
    private String tableName;
    private List<String> partColumnNames;
    private DeltaLog deltaLog;

    public DeltaLakeTable(long id, String catalogName, String dbName, String tableName, List<Column> schema,
                          List<String> partitionNames, DeltaLog deltaLog) {
        super(id, tableName, TableType.DELTALAKE, schema);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.partColumnNames = partitionNames;
        this.deltaLog = deltaLog;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    public DeltaLog getDeltaLog() {
        return deltaLog;
    }

    public String getTableLocation() {
        return deltaLog.getPath().toString();
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Column> getPartitionColumns() {
        return partColumnNames.stream()
                .map(name -> nameToColumn.get(name))
                .collect(Collectors.toList());
    }

    public boolean isUnPartitioned() {
        return partColumnNames.size() == 0;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        Preconditions.checkNotNull(partitions);
        Metadata metadata = deltaLog.snapshot().getMetadata();

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
            PartitionKey key = info.getKey();
            long partitionId = info.getId();

            THdfsPartition tPartition = new THdfsPartition();
            tPartition.setFile_format(DeltaUtils.getHdfsFileFormat(metadata.getFormat().getProvider()).toThrift());

            List<LiteralExpr> keys = key.getKeys();
            tPartition.setPartition_key_exprs(keys.stream().map(Expr::treeToThrift).collect(Collectors.toList()));

            THdfsPartitionLocation tPartitionLocation = new THdfsPartitionLocation();
            tPartitionLocation.setPrefix_index(-1);
            tPartitionLocation.setSuffix(info.getPath());
            tPartition.setLocation(tPartitionLocation);
            tDeltaLakeTable.putToPartitions(partitionId, tPartition);
        }

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.DELTALAKE_TABLE,
                fullSchema.size(), 0, tableName, dbName);
        tTableDescriptor.setDeltaLakeTable(tDeltaLakeTable);
        return tTableDescriptor;
    }
}
