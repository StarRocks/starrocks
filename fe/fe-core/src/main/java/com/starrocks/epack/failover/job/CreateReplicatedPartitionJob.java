// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.util.Util;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

public class CreateReplicatedPartitionJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(CreateReplicatedPartitionJob.class);

    private final Database remoteDatabase;
    private final OlapTable remoteTable;
    private final Partition remotePartition;
    private final Database localDatabase;
    private final OlapTable localTable;
    private final boolean isIncludeObject;

    public CreateReplicatedPartitionJob(FailoverGroup failoverGroup, Database remoteDatabase,
            OlapTable remoteTable, Partition remotePartition, Database localDatabase,
            OlapTable localTable, boolean isIncludeObject) {
        super(failoverGroup);
        this.remoteDatabase = remoteDatabase;
        this.remoteTable = remoteTable;
        this.remotePartition = remotePartition;
        this.localDatabase = localDatabase;
        this.localTable = localTable;
        this.isIncludeObject = isIncludeObject;
    }

    @Override
    public void execute() {
        LOG.info("Creating partition {}.{}.{} in failover group {}", localDatabase.getFullName(), localTable.getName(),
                remotePartition.getName(), failoverGroup.getName());

        AddPartitionClause addPartitionClause = getAddPartitionClause(remoteTable, remotePartition);
        try {
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            ConnectContext ctx = Util.getOrCreateInnerContext();
            ctx.setCurrentWarehouse(warehouseManager.getBackgroundWarehouse().getName());
            GlobalStateMgr.getServingState().getLocalMetastore().addPartitions(ctx, localDatabase, localTable.getName(),
                    addPartitionClause);
        } catch (Exception e) {
            failoverGroup.addErrorMessage("Failed to create partition " + localDatabase.getFullName() + "." +
                    localTable.getName() + "." + remotePartition.getName() + ", error: " + e.getMessage());
            LOG.warn("Failed to create partition {}.{}.{} in failover group {}, ", localDatabase.getFullName(),
                    localTable.getName(), remotePartition.getName(), failoverGroup.getName(), e);
            return;
        }

        CheckReplicatedTableJob job = new CheckReplicatedTableJob(failoverGroup,
                remoteDatabase, remoteTable, localDatabase, isIncludeObject);
        job.execute();
    }

    private static AddPartitionClause getAddPartitionClause(OlapTable table, Partition partition) {
        PartitionDesc partitionDesc = null;
        List<PartitionDesc> resolvedPartitionDescList = null;
        switch (table.getPartitionInfo().getType()) {
            case UNPARTITIONED:
                break;
            case LIST:
                ListPartitionDesc listPartitionDesc = getListPartitionDesc(table, partition);
                partitionDesc = listPartitionDesc;
                resolvedPartitionDescList = listPartitionDesc.getPartitionDescs();
                break;
            case RANGE:
            case EXPR_RANGE:
            case EXPR_RANGE_V2:
                RangePartitionDesc rangePartitionDesc = getRangePartitionDesc(table, partition);
                partitionDesc = rangePartitionDesc;
                resolvedPartitionDescList = Lists.newArrayList(rangePartitionDesc.getSingleRangePartitionDescs());
                break;
            default:
                LOG.warn("Invalid partition type {} of table {}", table.getPartitionInfo().getType(), table.getName());
                break;
        }
        DistributionDesc distributionDesc = partition.getDistributionInfo().toDistributionDesc(table.getIdToColumn());
        AddPartitionClause clause = new AddPartitionClause(partitionDesc, distributionDesc, null, false);
        clause.setResolvedPartitionDescList(resolvedPartitionDescList);
        return clause;
    }

    private static RangePartitionDesc getRangePartitionDesc(OlapTable table, Partition partition) {
        long partitionId = partition.getId();
        String partitionName = partition.getName();
        Preconditions.checkState(!partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX));

        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();

        List<String> partitionColumnNames = rangePartitionInfo.getPartitionColumns(table.getIdToColumn()).stream()
                .map(Column::getName).collect(Collectors.toList());

        Range<PartitionKey> partitionRange = rangePartitionInfo.getRange(partition.getId());
        List<PartitionValue> lowerValues = partitionRange.lowerEndpoint().getKeys().stream()
                .map(v -> new PartitionValue(v.getStringValue())).collect(Collectors.toList());
        List<PartitionValue> upperValues = partitionRange.upperEndpoint().getKeys().stream()
                .map(v -> new PartitionValue(v.getStringValue())).collect(Collectors.toList());
        PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(lowerValues, upperValues);
        SingleRangePartitionDesc singleRangePartitionDesc = new SingleRangePartitionDesc(
                true, partitionName,
                rangePartitionInfo.getReplicationNum(partitionId),
                rangePartitionInfo.getDataProperty(partitionId),
                rangePartitionInfo.getTabletType(partitionId),
                Partition.PARTITION_INIT_VERSION,
                rangePartitionInfo.getIsInMemory(partitionId),
                rangePartitionInfo.getDataCacheInfo(partitionId),
                partitionKeyDesc);
        singleRangePartitionDesc.setSystem(true);

        RangePartitionDesc rangePartitionDesc = new RangePartitionDesc(partitionColumnNames,
                Lists.newArrayList(singleRangePartitionDesc));
        rangePartitionDesc.setAutoPartitionTable(rangePartitionInfo.isAutomaticPartition());
        rangePartitionDesc.setSystem(true);
        return rangePartitionDesc;
    }

    private static ListPartitionDesc getListPartitionDesc(OlapTable table, Partition partition) {
        long partitionId = partition.getId();
        String partitionName = partition.getName();
        Preconditions.checkState(!partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX));

        ListPartitionInfo listPartitionInfo = (ListPartitionInfo) table.getPartitionInfo();

        List<String> partitionColumnNames = listPartitionInfo.getPartitionColumns(table.getIdToColumn()).stream()
                .map(Column::getName).collect(Collectors.toList());
        List<ColumnDef> partitionColumnDefs = listPartitionInfo.getPartitionColumns(table.getIdToColumn()).stream()
                .map(column -> column.toColumnDef(table)).collect(Collectors.toList());

        List<String> partitionValues = listPartitionInfo.getIdToValues().get(partitionId);
        List<List<String>> partitionMultiValues = listPartitionInfo.getIdToMultiValues().get(partitionId);

        List<PartitionDesc> partitionDescs = Lists.newArrayListWithCapacity(1);
        if (partitionValues != null) {
            SingleItemListPartitionDesc singleItemListPartitionDesc = new SingleItemListPartitionDesc(
                    true, partitionName,
                    listPartitionInfo.getReplicationNum(partitionId),
                    listPartitionInfo.getDataProperty(partitionId),
                    listPartitionInfo.getTabletType(partitionId),
                    Partition.PARTITION_INIT_VERSION,
                    listPartitionInfo.getIsInMemory(partitionId),
                    listPartitionInfo.getDataCacheInfo(partitionId),
                    partitionValues,
                    partitionColumnDefs);
            singleItemListPartitionDesc.setSystem(true);
            partitionDescs.add(singleItemListPartitionDesc);
        }

        if (partitionMultiValues != null) {
            MultiItemListPartitionDesc multiItemListPartitionDesc = new MultiItemListPartitionDesc(
                    true, partitionName,
                    listPartitionInfo.getReplicationNum(partitionId),
                    listPartitionInfo.getDataProperty(partitionId),
                    listPartitionInfo.getTabletType(partitionId),
                    Partition.PARTITION_INIT_VERSION,
                    listPartitionInfo.getIsInMemory(partitionId),
                    listPartitionInfo.getDataCacheInfo(partitionId),
                    partitionMultiValues,
                    partitionColumnDefs);
            multiItemListPartitionDesc.setSystem(true);
            partitionDescs.add(multiItemListPartitionDesc);
        }

        ListPartitionDesc listPartitionDesc = new ListPartitionDesc(partitionColumnNames, partitionDescs);
        listPartitionDesc.setAutoPartitionTable(listPartitionInfo.isAutomaticPartition());
        listPartitionDesc.setSystem(true);
        return listPartitionDesc;
    }
}
