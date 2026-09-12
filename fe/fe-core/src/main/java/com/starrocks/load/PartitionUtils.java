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

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.ListPartitionPersistInfo;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.RangePartitionPersistInfo;
import com.starrocks.persist.SinglePartitionPersistInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionUtils {
    private static final Logger LOG = LogManager.getLogger(PartitionUtils.class);

    // Keep a job error message bounded when many loads are running concurrently.
    private static final int MAX_REPORTED_CONFLICTING_TXNS = 5;

    /**
     * Find the running transactions that may write to the given partition.
     * <p>
     * Jobs that replace a partition with a rewritten one (optimize, merge partition, insert overwrite)
     * must call this while holding the table write lock, right before the replacement: replacing a
     * partition that has concurrent ingestion silently discards the loaded rows, because the tablets of
     * the source partition are force deleted while the load transaction still reports success.
     *
     * @return ids of the conflicting transactions, empty if this partition has no concurrent ingestion
     */
    public static List<Long> getConflictingIngestionTxnIds(long dbId, long tableId, Partition partition) {
        Set<Long> physicalPartitionIds = partition.getSubPartitions().stream()
                .map(PhysicalPartition::getId).collect(Collectors.toSet());
        return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getConflictingTxnIds(dbId, tableId, physicalPartitionIds);
    }

    /**
     * Whether the given partition has a transaction that is committed but not published yet.
     * <p>
     * Used to gate the rewritten temp partition before it takes over: swapping it in while its own rewrite
     * is not published yet would briefly expose a partition without the rewritten rows. Unlike a source
     * partition, a temp partition survives the swap, so a pending publish still lands on it and no row is
     * lost, which is why in flight transactions are not considered here.
     */
    public static boolean hasCommittedNotVisibleTxn(long dbId, long tableId, Partition partition) {
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        return partition.getSubPartitions().stream()
                .anyMatch(physicalPartition -> transactionMgr.existCommittedTxns(dbId, tableId, physicalPartition.getId()));
    }

    /**
     * Render conflicting transaction ids for a job error message, so that the blocking loads are visible
     * in `SHOW ALTER TABLE OPTIMIZE` without having to dig through the FE log. The list is truncated to
     * keep the persisted error message bounded when many loads run concurrently.
     */
    public static String formatConflictingTxnIds(List<Long> conflictingTxnIds) {
        if (conflictingTxnIds.isEmpty()) {
            return "";
        }
        String txnIds = conflictingTxnIds.stream().limit(MAX_REPORTED_CONFLICTING_TXNS)
                .map(String::valueOf).collect(Collectors.joining(", "));
        String suffix = conflictingTxnIds.size() > MAX_REPORTED_CONFLICTING_TXNS
                ? ", ... " + conflictingTxnIds.size() + " in total" : "";
        return " (txn: " + txnIds + suffix + ")";
    }

    public static void createAndAddTempPartitionsForTable(Database db, OlapTable targetTable,
                                                          String postfix, List<Long> sourcePartitionIds,
                                                          List<Long> tmpPartitionIds,
                                                          DistributionDesc distributionDesc,
                                                          ComputeResource computeResource) throws DdlException {
        List<Partition> newTempPartitions = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .createTempPartitionsFromPartitions(db, targetTable, postfix, sourcePartitionIds,
                        tmpPartitionIds, distributionDesc, computeResource);
        Locker locker = new Locker();
        if (!locker.lockTableAndCheckDbExist(db, targetTable.getId(), LockType.WRITE)) {
            throw new DdlException("create and add partition failed. database:{}" + db.getFullName() + " not exist");
        }
        boolean success = false;
        try {
            // should check whether targetTable exists
            Table tmpTable = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), targetTable.getId());
            if (tmpTable == null) {
                throw new DdlException("create partition failed because target table does not exist");
            }
            if (sourcePartitionIds.stream().anyMatch(id -> targetTable.getPartition(id) == null)) {
                throw new DdlException("create partition failed because src partitions changed");
            }
            List<Partition> sourcePartitions = sourcePartitionIds.stream()
                    .map(targetTable::getPartition).toList();
            PartitionInfo partitionInfo = targetTable.getPartitionInfo();
            List<PartitionPersistInfoV2> partitionInfoV2List = Lists.newArrayListWithCapacity(newTempPartitions.size());
            for (int i = 0; i < newTempPartitions.size(); i++) {
                long sourcePartitionId = sourcePartitions.get(i).getId();
                Partition partition = newTempPartitions.get(i);

                PartitionPersistInfoV2 info;
                if (partitionInfo.isRangePartition()) {
                    RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                    Range<PartitionKey> range = rangePartitionInfo.getRange(sourcePartitionId);

                    info = new RangePartitionPersistInfo(db.getId(), targetTable.getId(),
                            partition, partitionInfo.getDataProperty(sourcePartitionId),
                            partitionInfo.getReplicationNum(sourcePartitionId),
                            true, range, partitionInfo.getDataCacheInfo(sourcePartitionId));
                } else if (partitionInfo.isUnPartitioned()) {
                    info = new SinglePartitionPersistInfo(db.getId(), targetTable.getId(),
                            partition, partitionInfo.getDataProperty(sourcePartitionId),
                            partitionInfo.getReplicationNum(sourcePartitionId),
                            true, partitionInfo.getDataCacheInfo(sourcePartitionId));
                } else if (partitionInfo.isListPartition()) {
                    ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
                    List<String> values = listPartitionInfo.getIdToValues().get(sourcePartitionId);
                    List<List<String>> multiValues = listPartitionInfo.getIdToMultiValues().get(sourcePartitionId);
                    info = new ListPartitionPersistInfo(db.getId(), targetTable.getId(),
                            partition, partitionInfo.getDataProperty(sourcePartitionId),
                            partitionInfo.getReplicationNum(sourcePartitionId),
                            true, values, multiValues, partitionInfo.getDataCacheInfo(sourcePartitionId));
                } else {
                    throw new DdlException("Unsupported partition persist info.");
                }
                partitionInfoV2List.add(info);
            }

            AddPartitionsInfoV2 infos = new AddPartitionsInfoV2(partitionInfoV2List);
            GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(infos, wal -> {
                for (int i = 0; i < newTempPartitions.size(); i++) {
                    Partition partition = newTempPartitions.get(i);
                    long sourcePartitionId = sourcePartitions.get(i).getId();
                    targetTable.addTempPartition(partition);
                    partitionInfo.addPartition(partition.getId(),
                            partitionInfo.getDataProperty(sourcePartitionId),
                            partitionInfo.getReplicationNum(sourcePartitionId),
                            partitionInfo.getDataCacheInfo(sourcePartitionId));

                    if (partitionInfo.isRangePartition()) {
                        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                        rangePartitionInfo.setRange(partition.getId(), true,
                                rangePartitionInfo.getRange(sourcePartitionId));
                    } else if (partitionInfo.isListPartition()) {
                        ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
                        listPartitionInfo.setIdToIsTempPartition(partition.getId(), true);
                        List<String> values = listPartitionInfo.getIdToValues().get(sourcePartitionId);
                        if (values != null) {
                            listPartitionInfo.setValues(partition.getId(), values);
                            List<LiteralExpr> literalExprs =
                                    listPartitionInfo.getLiteralExprValues().get(sourcePartitionId);
                            listPartitionInfo.setDirectLiteralExprValues(partition.getId(), literalExprs);
                        }

                        List<List<String>> multiValues = listPartitionInfo.getIdToMultiValues().get(sourcePartitionId);
                        if (multiValues != null) {
                            listPartitionInfo.setMultiValues(partition.getId(), multiValues);
                            List<List<LiteralExpr>> multiLiteralExprs =
                                    listPartitionInfo.getMultiLiteralExprValues().get(sourcePartitionId);
                            listPartitionInfo.setDirectMultiLiteralExprValues(partition.getId(), multiLiteralExprs);
                        }
                    }
                }
            });

            success = true;
        } finally {
            if (!success) {
                try {
                    clearTabletsFromInvertedIndex(newTempPartitions);
                } catch (Throwable t) {
                    LOG.warn("clear tablets from inverted index failed", t);
                }
            }
            locker.unLockTableWithIntensiveDbLock(db.getId(), targetTable.getId(), LockType.WRITE);
        }
    }

    public static void clearTabletsFromInvertedIndex(List<Partition> partitions) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Partition partition : partitions) {
            for (PhysicalPartition subPartition : partition.getSubPartitions()) {
                for (MaterializedIndex materializedIndex : subPartition.getAllMaterializedIndices(
                            MaterializedIndex.IndexExtState.ALL)) {
                    for (Tablet tablet : materializedIndex.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }
        }
    }

    public static RangePartitionBoundary calRangePartitionBoundary(Range<PartitionKey> range) {
        boolean isMaxPartition = range.upperEndpoint().isMaxValue();
        boolean isMinPartition = range.lowerEndpoint().isMinValue();

        // start keys
        List<LiteralExpr> rangeKeyExprs;
        List<Object> startKeys = new ArrayList<>();
        if (!isMinPartition) {
            rangeKeyExprs = range.lowerEndpoint().getKeys();
            for (LiteralExpr literalExpr : rangeKeyExprs) {
                Object keyValue;
                if (literalExpr instanceof DateLiteral) {
                    keyValue = convertDateLiteralToNumber((DateLiteral) literalExpr);
                } else {
                    keyValue = literalExpr.getRealObjectValue();
                }

                startKeys.add(keyValue);
            }
        }

        // end keys
        // is empty list when max partition
        List<Object> endKeys = new ArrayList<>();
        if (!isMaxPartition) {
            rangeKeyExprs = range.upperEndpoint().getKeys();
            for (LiteralExpr literalExpr : rangeKeyExprs) {
                Object keyValue;
                if (literalExpr instanceof DateLiteral) {
                    keyValue = convertDateLiteralToNumber((DateLiteral) literalExpr);
                } else {
                    keyValue = literalExpr.getRealObjectValue();
                }
                endKeys.add(keyValue);
            }
        }

        return new RangePartitionBoundary(isMinPartition, isMaxPartition, startKeys, endKeys);
    }

    public static List<List<Object>> calListPartitionKeys(List<List<LiteralExpr>> multiLiteralExprs,
                                                          List<LiteralExpr> literalExprs) {
        List<List<Object>> keys = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(multiLiteralExprs)) {
            for (List<LiteralExpr> exprs : multiLiteralExprs) {
                keys.add(initItemOfInKeys(exprs));
            }
        }
        if (CollectionUtils.isNotEmpty(literalExprs)) {
            for (LiteralExpr expr : literalExprs) {
                keys.add(initItemOfInKeys(Collections.singletonList(expr)));
            }
        }
        return keys;
    }

    private static List<Object> initItemOfInKeys(List<LiteralExpr> exprs) {
        return exprs.stream()
                .filter(Objects::nonNull)
                .map(PartitionUtils::exprValue)
                .collect(Collectors.toList());
    }

    private static Object exprValue(LiteralExpr expr) {
        return expr instanceof DateLiteral
                ? convertDateLiteralToNumber((DateLiteral) expr) : expr.getRealObjectValue();
    }

    // This is to be compatible with Spark Load Job formats for Date type.
    // Because the historical version is serialized and deserialized with a special hash number for DateLiteral,
    // special processing is also done here for DateLiteral to keep the historical version compatible.
    // The deserialized code is in "SparkDpp.createPartitionRangeKeys"
    public static Object convertDateLiteralToNumber(DateLiteral dateLiteral) {
        if (dateLiteral.getType().isDate()) {
            return (dateLiteral.getYear() * 16 * 32L
                    + dateLiteral.getMonth() * 32
                    + dateLiteral.getDay());
        } else if (dateLiteral.getType().isDatetime()) {
            return dateLiteral.getLongValue();
        } else {
            throw new StarRocksPlannerException("Invalid date type: " + dateLiteral.getType(), ErrorType.INTERNAL_ERROR);
        }
    }

    public static class RangePartitionBoundary {

        private final boolean minPartition;

        private final boolean maxPartition;

        private final List<Object> startKeys;

        private final List<Object> endKeys;

        public RangePartitionBoundary(boolean minPartition,
                                      boolean maxPartition,
                                      List<Object> startKeys,
                                      List<Object> endKeys) {
            this.minPartition = minPartition;
            this.maxPartition = maxPartition;
            this.startKeys = startKeys;
            this.endKeys = endKeys;
        }

        public boolean isMinPartition() {
            return minPartition;
        }

        public boolean isMaxPartition() {
            return maxPartition;
        }

        public List<Object> getStartKeys() {
            return startKeys;
        }

        public List<Object> getEndKeys() {
            return endKeys;
        }
    }
}
