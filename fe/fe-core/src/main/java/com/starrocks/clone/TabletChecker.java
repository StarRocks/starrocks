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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/clone/TabletChecker.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.clone;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table.Cell;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.LocalTablet.TabletStatus;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Partition.PartitionState;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AdminStmtAnalyzer;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * This checker is responsible for checking all unhealthy tablets.
 * It does not responsible for any scheduler of tablet repairing or balance
 */
public class TabletChecker extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletChecker.class);

    private final TabletScheduler tabletScheduler;
    private final TabletSchedulerStat stat;

    // db id -> (tbl id -> PrioPart)
    // priority of replicas of partitions in this table will be set to VERY_HIGH if unhealthy
    private com.google.common.collect.Table<Long, Long, Set<PrioPart>> urgentTable = HashBasedTable.create();

    // represent a partition which need to be repaired preferentially
    public static class PrioPart {
        public long partId;
        public long addTime;
        public long timeoutMs;

        public PrioPart(long partId, long addTime, long timeoutMs) {
            this.partId = partId;
            this.addTime = addTime;
            this.timeoutMs = timeoutMs;
        }

        public boolean isTimeout() {
            return System.currentTimeMillis() - addTime > timeoutMs;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PrioPart)) {
                return false;
            }
            return partId == ((PrioPart) obj).partId;
        }

        @Override
        public int hashCode() {
            return Long.valueOf(partId).hashCode();
        }
    }

    public static class RepairTabletInfo {
        public long dbId;
        public long tblId;
        public List<Long> partIds;

        public RepairTabletInfo(Long dbId, Long tblId, List<Long> partIds) {
            this.dbId = dbId;
            this.tblId = tblId;
            this.partIds = partIds;
        }
    }

    public TabletChecker(TabletScheduler tabletScheduler, TabletSchedulerStat stat) {
        super("tablet checker", Config.tablet_sched_checker_interval_seconds * 1000L);
        this.tabletScheduler = tabletScheduler;
        this.stat = stat;
    }

    private void addToUrgentTable(RepairTabletInfo repairTabletInfo, long timeoutMs) {
        Preconditions.checkArgument(!repairTabletInfo.partIds.isEmpty());
        long currentTime = System.currentTimeMillis();
        synchronized (urgentTable) {
            Set<PrioPart> parts = urgentTable.get(repairTabletInfo.dbId, repairTabletInfo.tblId);
            if (parts == null) {
                parts = Sets.newHashSet();
                urgentTable.put(repairTabletInfo.dbId, repairTabletInfo.tblId, parts);
            }

            for (long partId : repairTabletInfo.partIds) {
                PrioPart prioPart = new PrioPart(partId, currentTime, timeoutMs);
                parts.add(prioPart);
            }
        }

        // we also need to change the priority of tablets which are already in
        tabletScheduler.changeTabletsPriorityToVeryHigh(repairTabletInfo.dbId, repairTabletInfo.tblId,
                repairTabletInfo.partIds);
    }

    /**
     * When setting a tablet to `bad` status manually, call this method to put the corresponding partition into
     * the `urgentTable` so that the bad tablet can be repaired ASAP.
     *
     * @param dbId database id
     * @param tableId table id
     * @param partitionId partition to which the tablet belongs
     */
    public void setTabletForUrgentRepair(long dbId, long tableId, long partitionId) {
        RepairTabletInfo repairTabletInfo = new RepairTabletInfo(dbId, tableId, Collections.singletonList(partitionId));
        addToUrgentTable(repairTabletInfo, AdminStmtAnalyzer.DEFAULT_PRIORITY_REPAIR_TIMEOUT_SEC);
    }

    public void removeFromUrgentTable(RepairTabletInfo repairTabletInfo) {
        Preconditions.checkArgument(!repairTabletInfo.partIds.isEmpty());
        synchronized (urgentTable) {
            Map<Long, Set<PrioPart>> tblMap = urgentTable.row(repairTabletInfo.dbId);
            if (tblMap == null) {
                return;
            }
            Set<PrioPart> parts = tblMap.get(repairTabletInfo.tblId);
            if (parts == null) {
                return;
            }
            for (long partId : repairTabletInfo.partIds) {
                parts.remove(new PrioPart(partId, -1, -1));
            }
            if (parts.isEmpty()) {
                tblMap.remove(repairTabletInfo.tblId);
            }
        }

    }

    /*
     * For each cycle, TabletChecker will check all OlapTable's tablet.
     * If a tablet is not healthy, a TabletInfo will be created and sent to TabletScheduler for repairing.
     */
    @Override
    protected void runAfterCatalogReady() {
        if (RunMode.isSharedDataMode()) {
            return;
        }
        int pendingNum = tabletScheduler.getPendingNum();
        int runningNum = tabletScheduler.getRunningNum();
        if (pendingNum > Config.tablet_sched_max_scheduling_tablets
                || runningNum > Config.tablet_sched_max_scheduling_tablets) {
            LOG.info("too many tablets are being scheduled. pending: {}, running: {}, limit: {}. skip check",
                    pendingNum, runningNum, Config.tablet_sched_max_scheduling_tablets);
            return;
        }

        checkAllTablets();

        cleanInvalidUrgentTable();

        stat.counterTabletCheckRound.incrementAndGet();
        LOG.info(stat.incrementalBrief());
    }

    /**
     * Check the manually repaired table/partition first,
     * so that they can be scheduled for repair at first place.
     */
    private void checkAllTablets() {
        checkUrgentTablets();
        checkNonUrgentTablets();
    }

    private void checkUrgentTablets() {
        doCheck(true);
    }

    private void checkNonUrgentTablets() {
        doCheck(false);
    }

    /**
     * In order to avoid meaningless repair schedule, for task that need to
     * choose a source replica to clone from, we check that whether we can
     * find a healthy source replica before send it to pending queue.
     * <p>
     * If we don't do this check, the task will go into the pending queue and
     * get deleted when no healthy source replica found, and then it will be added
     * to the queue again in the next `TabletChecker` round.
     */
    private boolean tryChooseSrcBeforeSchedule(TabletSchedCtx tabletCtx) {
        if (tabletCtx.needCloneFromSource()) {
            if (tabletCtx.getReplicas().size() == 1 && Config.recover_with_empty_tablet) {
                // in this case, we need to forcefully create an empty replica to recover
                return true;
            } else {
                return tabletCtx.getHealthyReplicas().size() != 0;
            }
        } else {
            return true;
        }
    }

    private void doCheck(boolean isUrgent) {
        long start = System.nanoTime();
        long totalTabletNum = 0;
        long unhealthyTabletNum = 0;
        long addToSchedulerTabletNum = 0;
        long tabletInScheduler = 0;
        long tabletNotReady = 0;

        long lockTotalTime = 0;
        long waitTotalTime = 0;
        long lockStart;
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIdsIncludeRecycleBin();
        DATABASE:
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDbIncludeRecycleBin(dbId);
            if (db == null) {
                continue;
            }

            if (db.isSystemDatabase()) {
                continue;
            }

            // set the config to a local variable to avoid config params changed.
            int partitionBatchNum = Config.tablet_checker_partition_batch_num;
            int partitionChecked = 0;
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            lockStart = System.nanoTime();
            try {
                List<Long> aliveBeIdsInCluster = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true);
                TABLE:
                for (Table table : GlobalStateMgr.getCurrentState().getTablesIncludeRecycleBin(db)) {
                    if (!table.needSchedule(false)) {
                        continue;
                    }
                    if (table.isCloudNativeTableOrMaterializedView()) {
                        // replicas are managed by StarOS and cloud storage.
                        continue;
                    }

                    if ((isUrgent && !isUrgentTable(dbId, table.getId()))) {
                        continue;
                    }

                    OlapTable olapTbl = (OlapTable) table;
                    for (Partition partition : GlobalStateMgr.getCurrentState().getAllPartitionsIncludeRecycleBin(olapTbl)) {
                        partitionChecked++;

                        boolean isPartitionUrgent = isPartitionUrgent(dbId, table.getId(), partition.getId());
                        boolean isUrgentPartitionHealthy = true;
                        if ((isUrgent && !isPartitionUrgent) || (!isUrgent && isPartitionUrgent)) {
                            continue;
                        }

                        if (partitionChecked % partitionBatchNum == 0) {
                            LOG.debug("partition checked reached batch value, release lock");
                            lockTotalTime += System.nanoTime() - lockStart;
                            // release lock, so that lock can be acquired by other threads.
                            locker.unLockDatabase(db, LockType.READ);
                            locker.lockDatabase(db, LockType.READ);
                            LOG.debug("checker get lock again");
                            lockStart = System.nanoTime();
                            if (GlobalStateMgr.getCurrentState().getDbIncludeRecycleBin(dbId) == null) {
                                continue DATABASE;
                            }
                            if (GlobalStateMgr.getCurrentState().getTableIncludeRecycleBin(db, olapTbl.getId()) == null) {
                                continue TABLE;
                            }
                            if (GlobalStateMgr.getCurrentState()
                                    .getPartitionIncludeRecycleBin(olapTbl, partition.getId()) == null) {
                                continue;
                            }
                        }

                        if (partition.getState() != PartitionState.NORMAL) {
                            // when alter job is in FINISHING state, partition state will be set to NORMAL,
                            // and we can schedule the tablets in it.
                            continue;
                        }

                        short replicaNum = GlobalStateMgr.getCurrentState()
                                .getReplicationNumIncludeRecycleBin(olapTbl.getPartitionInfo(), partition.getId());
                        if (replicaNum == (short) -1) {
                            continue;
                        }

                        /*
                         * Tablet in SHADOW index can not be repaired of balanced
                         */
                        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                            for (MaterializedIndex idx : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                                for (Tablet tablet : idx.getTablets()) {
                                    LocalTablet localTablet = (LocalTablet) tablet;
                                    totalTabletNum++;

                                    if (tabletScheduler.containsTablet(tablet.getId())) {
                                        tabletInScheduler++;
                                        continue;
                                    }

                                    Pair<TabletStatus, TabletSchedCtx.Priority> statusWithPrio =
                                            localTablet.getHealthStatusWithPriority(
                                                    GlobalStateMgr.getCurrentSystemInfo(),
                                                    physicalPartition.getVisibleVersion(),
                                                    replicaNum,
                                                    aliveBeIdsInCluster);

                                    if (statusWithPrio.first == TabletStatus.HEALTHY) {
                                        // Only set last status check time when status is healthy.
                                        localTablet.setLastStatusCheckTime(System.currentTimeMillis());
                                        continue;
                                    } else if (isPartitionUrgent) {
                                        statusWithPrio.second = TabletSchedCtx.Priority.VERY_HIGH;
                                        isUrgentPartitionHealthy = false;
                                    }

                                    unhealthyTabletNum++;

                                    if (!localTablet.readyToBeRepaired(statusWithPrio.first, statusWithPrio.second)) {
                                        tabletNotReady++;
                                        continue;
                                    }

                                    TabletSchedCtx tabletCtx = new TabletSchedCtx(
                                            TabletSchedCtx.Type.REPAIR,
                                            db.getId(), olapTbl.getId(), partition.getId(),
                                            physicalPartition.getId(), idx.getId(), tablet.getId(),
                                            System.currentTimeMillis());
                                    // the tablet status will be set again when being scheduled
                                    tabletCtx.setTabletStatus(statusWithPrio.first);
                                    tabletCtx.setOrigPriority(statusWithPrio.second);
                                    tabletCtx.setTablet(localTablet);
                                    if (!tryChooseSrcBeforeSchedule(tabletCtx)) {
                                        continue;
                                    }

                                    Pair<Boolean, Long> result =
                                            tabletScheduler.blockingAddTabletCtxToScheduler(db, tabletCtx, isPartitionUrgent);
                                    waitTotalTime += result.second;
                                    if (result.first) {
                                        addToSchedulerTabletNum++;
                                    }
                                }
                            } // indices
                        }

                        if (isUrgentPartitionHealthy && isPartitionUrgent) {
                            // if all replicas in this partition are healthy, remove this partition from
                            // priorities.
                            LOG.debug("partition is healthy, remove from urgent table: {}-{}-{}",
                                    db.getId(), olapTbl.getId(), partition.getId());
                            removeFromUrgentTable(new RepairTabletInfo(db.getId(),
                                    olapTbl.getId(), Lists.newArrayList(partition.getId())));
                        }
                    } // partitions
                } // tables
            } finally {
                lockTotalTime += System.nanoTime() - lockStart;
                locker.unLockDatabase(db, LockType.READ);
            }
        } // end for dbs

        long cost = (System.nanoTime() - start) / 1000000;
        lockTotalTime = lockTotalTime / 1000000;

        stat.counterTabletCheckCostMs.addAndGet(cost);
        stat.counterTabletChecked.addAndGet(totalTabletNum);
        stat.counterUnhealthyTabletNum.addAndGet(unhealthyTabletNum);
        stat.counterTabletAddToBeScheduled.addAndGet(addToSchedulerTabletNum);

        LOG.info("finished to check tablets. isUrgent: {}, " +
                        "unhealthy/total/added/in_sched/not_ready: {}/{}/{}/{}/{}, " +
                        "cost: {} ms, in lock time: {} ms, wait time: {}ms",
                isUrgent, unhealthyTabletNum, totalTabletNum, addToSchedulerTabletNum,
                tabletInScheduler, tabletNotReady, cost, lockTotalTime - waitTotalTime, waitTotalTime);
    }

    public boolean isUrgentTable(long dbId, long tblId) {
        synchronized (urgentTable) {
            return urgentTable.contains(dbId, tblId);
        }
    }

    public boolean isPartitionUrgent(long dbId, long tblId, long partId) {
        synchronized (urgentTable) {
            if (isUrgentTable(dbId, tblId)) {
                return Objects.requireNonNull(urgentTable.get(dbId, tblId)).contains(new PrioPart(partId, -1, -1));
            }
            return false;
        }
    }

    // remove partition from urgent table if:
    // 1. timeout
    // 2. meta not found
    private void cleanInvalidUrgentTable() {
        com.google.common.collect.Table<Long, Long, Set<PrioPart>> copiedUrgentTable;
        synchronized (urgentTable) {
            copiedUrgentTable = HashBasedTable.create(urgentTable);
        }
        List<Pair<Long, Long>> deletedUrgentTable = Lists.newArrayList();
        Iterator<Map.Entry<Long, Map<Long, Set<PrioPart>>>> iter = copiedUrgentTable.rowMap().entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, Map<Long, Set<PrioPart>>> dbEntry = iter.next();
            long dbId = dbEntry.getKey();
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                iter.remove();
                continue;
            }

            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                for (Map.Entry<Long, Set<PrioPart>> tblEntry : dbEntry.getValue().entrySet()) {
                    long tblId = tblEntry.getKey();
                    OlapTable tbl = (OlapTable) db.getTable(tblId);
                    if (tbl == null) {
                        deletedUrgentTable.add(Pair.create(dbId, tblId));
                        continue;
                    }

                    Set<PrioPart> parts = tblEntry.getValue();
                    parts = parts.stream().filter(p -> (tbl.getPartition(p.partId) != null && !p.isTimeout())).collect(
                            Collectors.toSet());
                    if (parts.isEmpty()) {
                        deletedUrgentTable.add(Pair.create(dbId, tblId));
                    }
                }

                if (dbEntry.getValue().isEmpty()) {
                    iter.remove();
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }
        for (Pair<Long, Long> prio : deletedUrgentTable) {
            copiedUrgentTable.remove(prio.first, prio.second);
        }
        urgentTable = copiedUrgentTable;
    }

    /*
     * handle ADMIN REPAIR TABLE stmt send by user.
     * This operation will add specified tables into 'urgentTable', and tablets of this table will be set VERY_HIGH
     * when being scheduled.
     */
    public void repairTable(AdminRepairTableStmt stmt) throws DdlException {
        RepairTabletInfo repairTabletInfo =
                getRepairTabletInfo(stmt.getDbName(), stmt.getTblName(), stmt.getPartitions());
        addToUrgentTable(repairTabletInfo, stmt.getTimeoutS());
        LOG.info("repair database: {}, table: {}, partition: {}", repairTabletInfo.dbId, repairTabletInfo.tblId,
                repairTabletInfo.partIds);
    }

    /*
     * handle ADMIN CANCEL REPAIR TABLE stmt send by user.
     * This operation will remove the specified partitions from 'urgentTable'
     */
    public void cancelRepairTable(AdminCancelRepairTableStmt stmt) throws DdlException {
        RepairTabletInfo repairTabletInfo =
                getRepairTabletInfo(stmt.getDbName(), stmt.getTblName(), stmt.getPartitions());
        removeFromUrgentTable(repairTabletInfo);
        LOG.info("cancel repair database: {}, table: {}, partition: {}", repairTabletInfo.dbId, repairTabletInfo.tblId,
                repairTabletInfo.partIds);
    }

    public int getPrioPartitionNum() {
        int count = 0;
        synchronized (urgentTable) {
            for (Set<PrioPart> set : urgentTable.values()) {
                count += set.size();
            }
        }
        return count;
    }

    public List<List<String>> getUrgentTableInfo() {
        List<List<String>> infos = Lists.newArrayList();
        synchronized (urgentTable) {
            for (Cell<Long, Long, Set<PrioPart>> cell : urgentTable.cellSet()) {
                for (PrioPart part : Objects.requireNonNull(cell.getValue())) {
                    List<String> row = Lists.newArrayList();
                    row.add(Objects.requireNonNull(cell.getRowKey()).toString());
                    row.add(Objects.requireNonNull(cell.getColumnKey()).toString());
                    row.add(String.valueOf(part.partId));
                    row.add(String.valueOf(part.timeoutMs - (System.currentTimeMillis() - part.addTime)));
                    infos.add(row);
                }
            }
        }
        return infos;
    }

    public static RepairTabletInfo getRepairTabletInfo(String dbName, String tblName, List<String> partitions)
            throws DdlException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        long dbId = db.getId();
        long tblId;
        List<Long> partIds = Lists.newArrayList();
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Table tbl = db.getTable(tblName);
            if (tbl == null || tbl.getType() != TableType.OLAP) {
                throw new DdlException("Table does not exist or is not OLAP table: " + tblName);
            }

            tblId = tbl.getId();
            OlapTable olapTable = (OlapTable) tbl;

            if (partitions == null || partitions.isEmpty()) {
                partIds = olapTable.getPartitions().stream().map(Partition::getId).collect(Collectors.toList());
            } else {
                for (String partName : partitions) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        throw new DdlException("Partition does not exist: " + partName);
                    }
                    partIds.add(partition.getId());
                }
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        Preconditions.checkState(tblId != -1);

        return new RepairTabletInfo(dbId, tblId, partIds);
    }
}
