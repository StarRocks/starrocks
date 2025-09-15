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
package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.common.ListPartitionDiffer;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * `PCTTableSnapshotInfo` represents a snapshot of the base table of materialized view.
 */
public class PCTTableSnapshotInfo extends BaseTableSnapshotInfo {

    private static final Logger LOG = LogManager.getLogger(PCTTableSnapshotInfo.class);

    // partition's base info to be used in `updateMeta`
    private Map<String, MaterializedView.BasePartitionInfo> refreshedPartitionInfos = Maps.newHashMap();

    public PCTTableSnapshotInfo(BaseTableInfo baseTableInfo, Table baseTable) {
        super(baseTableInfo, baseTable);
    }

    public Map<String, MaterializedView.BasePartitionInfo> getRefreshedPartitionInfos() {
        return refreshedPartitionInfos;
    }

    @Override
    public String toString() {
        return "baseTable=" + baseTable.getName() +
                ", refreshedPartitionInfos=" + refreshedPartitionInfos;
    }

    /**
     * Check if the base table's partition has changed since the last snapshot.
     * @param mv : materialized view to check
     * @return true if the base table's partition has changed, false otherwise.
     */
    public boolean hasBaseTableChanged(MaterializedView mv) {
        try {
            return checkBaseTableChanged(mv);
        } catch (Exception e) {
            LOG.warn("Materialized view compute partition change failed:", e);
            return true;
        }
    }

    /**
     * Update the partition infos of the base table.
     * @param refreshedPartitionNames : the names of the partitions that have been refreshed.
     */
    public void updatePartitionInfos(List<String> refreshedPartitionNames) {
        Preconditions.checkNotNull(baseTableInfo, "baseTableInfo should not be null");
        Preconditions.checkNotNull(baseTable, "baseTable should not be null");
        if (baseTable.isNativeTableOrMaterializedView()) {
            OlapTable olapTable = (OlapTable) baseTable;
            updatePCTOlapPartitionInfos(olapTable, refreshedPartitionNames);
        } else if (MVPCTRefreshPartitioner.isPartitionRefreshSupported(baseTable)) {
            updatePCTExternalPartitionInfos(baseTable, refreshedPartitionNames);
        } else {
            // FIXME: base table does not support partition-level refresh and does not update the meta
            //  in materialized view.
            LOG.warn("refresh mv with non-supported-partition-level refresh base table {}", baseTable.getName());
        }
    }

    private boolean checkBaseTableChanged(MaterializedView mv) throws StarRocksException {
        Optional<Table> optTable = MvUtils.getTableWithIdentifier(baseTableInfo);
        if (optTable.isEmpty()) {
            return true;
        }
        Table table = optTable.get();
        if (baseTable.isOlapOrCloudNativeTable()) {
            OlapTable snapShotOlapTable = (OlapTable) baseTable;
            PartitionInfo snapshotPartitionInfo = snapShotOlapTable.getPartitionInfo();
            if (snapshotPartitionInfo.isUnPartitioned()) {
                Set<String> partitionNames = ((OlapTable) table).getVisiblePartitionNames();
                // if the partition names are not equal, it means the partition has changed.
                return !snapShotOlapTable.getVisiblePartitionNames().equals(partitionNames);
            } else if (snapshotPartitionInfo.isListPartition()) {
                OlapTable snapshotOlapTable = (OlapTable) baseTable;
                Map<String, PListCell> snapshotPartitionMap = snapshotOlapTable.getListPartitionItems();
                Map<String, PListCell> currentPartitionMap = snapshotOlapTable.getListPartitionItems();
                return ListPartitionDiffer.hasListPartitionChanged(snapshotPartitionMap, currentPartitionMap);
            } else {
                Map<String, Range<PartitionKey>> snapshotPartitionMap = snapShotOlapTable.getRangePartitionMap();
                Map<String, Range<PartitionKey>> currentPartitionMap = ((OlapTable) table).getRangePartitionMap();
                return SyncPartitionUtils.hasRangePartitionChanged(snapshotPartitionMap, currentPartitionMap);
            }
        } else if (ConnectorPartitionTraits.isSupported(baseTable.getType())) {
            if (baseTable.isUnPartitioned()) {
                return false;
            }
            PartitionInfo mvPartitionInfo = mv.getPartitionInfo();
            // TODO: Support list partition later.
            // do not need to check base partition table changed when mv is not partitioned
            if (!(mvPartitionInfo.isRangePartition())) {
                return false;
            }
            Map<Table, List<Column>> partitionTableAndColumns = mv.getRefBaseTablePartitionColumns();
            // For Non-partition based base table, it's not necessary to check the partition changed.
            if (!partitionTableAndColumns.containsKey(baseTable)) {
                return false;
            }
            List<Column> partitionColumns = partitionTableAndColumns.get(baseTable);
            Preconditions.checkArgument(partitionColumns.size() == 1,
                    "Only support one partition column in range partition");
            Column partitionColumn = partitionColumns.get(0);
            Optional<Expr> rangePartitionExprOpt = mv.getRangePartitionFirstExpr();
            if (rangePartitionExprOpt.isEmpty()) {
                return false;
            }
            Expr rangePartitionExpr = rangePartitionExprOpt.get();
            Map<String, Range<PartitionKey>> snapshotPartitionMap = PartitionUtil.getPartitionKeyRange(
                    baseTable, partitionColumn, rangePartitionExpr);
            Map<String, Range<PartitionKey>> currentPartitionMap = PartitionUtil.getPartitionKeyRange(
                    table, partitionColumn, rangePartitionExpr);
            return SyncPartitionUtils.hasRangePartitionChanged(snapshotPartitionMap, currentPartitionMap);
        } else {
            return false;
        }
    }

    private static MaterializedView.BasePartitionInfo toBasePartitionInfo(Partition partition) {
        return new MaterializedView.BasePartitionInfo(partition.getId(),
                partition.getDefaultPhysicalPartition().getVisibleVersion(),
                partition.getDefaultPhysicalPartition().getVisibleVersionTime());
    }

    private void updatePCTOlapPartitionInfos(OlapTable olapTable,
                                             List<String> refreshedPartitionNames) {
        for (String partitionName : refreshedPartitionNames) {
            Partition partition = olapTable.getPartition(partitionName);
            // it's ok to skip because only existed partitions are updated in the version map.
            if (partition == null) {
                LOG.warn("partition {} not found in base table {}, refreshedPartitionNames:{}",
                        partitionName, olapTable.getName(), refreshedPartitionNames);
                continue;
            }
            MaterializedView.BasePartitionInfo basePartitionInfo = toBasePartitionInfo(partition);
            refreshedPartitionInfos.put(partition.getName(), basePartitionInfo);
        }
    }

    /**
     * @param table                  : input table to collect refresh partition infos
     * @param refreshedPartitionNames : input table refreshed partition names
     * @return : return the given table's refresh partition infos
     */
    private void updatePCTExternalPartitionInfos(Table table,
                                                 List<String> refreshedPartitionNames) {
        // sort selectedPartitionNames before the for loop, otherwise the order of partition names may be
        // different in selectedPartitionNames and partitions and will lead to infinite partition refresh.
        Collections.sort(refreshedPartitionNames);

        List<com.starrocks.connector.PartitionInfo> partitions = GlobalStateMgr.getCurrentState()
                .getMetadataMgr()
                .getPartitions(baseTableInfo.getCatalogName(), table, refreshedPartitionNames);
        if (partitions.size() != refreshedPartitionNames.size()) {
            LOG.warn("Partition names size {} does not match partitions size {} for table {}",
                    refreshedPartitionNames.size(), partitions.size(), table.getName());
            return;
        }
        for (int index = 0; index < refreshedPartitionNames.size(); ++index) {
            long modifiedTime = partitions.get(index).getModifiedTime();
            String partitionName = refreshedPartitionNames.get(index);
            Preconditions.checkArgument(partitionName != null, "name should not be null");

            MaterializedView.BasePartitionInfo basePartitionInfo = new MaterializedView.BasePartitionInfo(
                    -1, modifiedTime, modifiedTime);
            if (Config.enable_mv_automatic_repairing_for_broken_base_tables) {
                MVPCTMetaRepairer.collectTableRepairInfo(table, partitionName, basePartitionInfo);
            }
            refreshedPartitionInfos.put(partitionName, basePartitionInfo);
        }
    }
}