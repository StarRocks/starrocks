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
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table.Cell;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.LocalTablet.TabletHealthStatus;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Partition.PartitionState;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.CloseableLock;
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
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.NodeSelector;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
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
     * @param dbId        database id
     * @param tableId     table id
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
                return !tabletCtx.getHealthyReplicas().isEmpty();
            }
        } else {
            return true;
        }
    }

    private void doCheck(boolean isUrgent) {
        long start = System.nanoTime();
        TabletCheckerStat totStat = new TabletCheckerStat();

        long lockTotalTime = 0;
        long waitTotalTime = 0;
        long lockStart;
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIdsIncludeRecycleBin();
        DATABASE:
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIncludeRecycleBin(dbId);
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
                List<Long> aliveBeIdsInCluster =
                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true);
                TABLE:
                for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTablesIncludeRecycleBin(db)) {
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
                    for (Partition partition : GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getAllPartitionsIncludeRecycleBin(olapTbl)) {
                        partitionChecked++;

                        boolean isPartitionUrgent = isPartitionUrgent(dbId, table.getId(), partition.getId());
                        totStat.isUrgentPartitionHealthy = true;
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
                            if (GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIncludeRecycleBin(dbId) == null) {
                                continue DATABASE;
                            }
                            if (GlobalStateMgr.getCurrentState().getLocalMetastore()
                                    .getTableIncludeRecycleBin(db, olapTbl.getId()) == null) {
                                continue TABLE;
                            }
                            if (GlobalStateMgr.getCurrentState()
                                    .getLocalMetastore().getPartitionIncludeRecycleBin(olapTbl, partition.getId()) == null) {
                                continue;
                            }
                        }

                        if (partition.getState() != PartitionState.NORMAL) {
                            // when alter job is in FINISHING state, partition state will be set to NORMAL,
                            // and we can schedule the tablets in it.
                            continue;
                        }

                        short replicaNum = GlobalStateMgr.getCurrentState()
                                .getLocalMetastore()
                                .getReplicationNumIncludeRecycleBin(olapTbl.getPartitionInfo(), partition.getId());
                        if (replicaNum == (short) -1) {
                            continue;
                        }

                        TabletCheckerStat partitionTabletCheckerStat = doCheckOnePartition(db, olapTbl, partition,
                                replicaNum, aliveBeIdsInCluster, isPartitionUrgent);
                        totStat.accumulateStat(partitionTabletCheckerStat);

                        if (totStat.isUrgentPartitionHealthy && isPartitionUrgent) {
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
        stat.counterTabletChecked.addAndGet(totStat.totalTabletNum);
        stat.counterUnhealthyTabletNum.addAndGet(totStat.unhealthyTabletNum);
        stat.counterTabletAddToBeScheduled.addAndGet(totStat.addToSchedulerTabletNum);

        LOG.info("finished to check tablets. isUrgent: {}, " +
                        "unhealthy/total/added/in_sched/not_ready: {}/{}/{}/{}/{}, " +
                        "cost: {} ms, in lock time: {} ms, wait time: {}ms",
                isUrgent, totStat.unhealthyTabletNum, totStat.totalTabletNum, totStat.addToSchedulerTabletNum,
                totStat.tabletInScheduler, totStat.tabletNotReady, cost, lockTotalTime - waitTotalTime, waitTotalTime);
    }

    private static class TabletCheckerStat {
        public long totalTabletNum = 0;
        public long unhealthyTabletNum = 0;
        public long addToSchedulerTabletNum = 0;
        public long tabletInScheduler = 0;
        public long tabletNotReady = 0;
        public long waitTotalTime = 0;
        public boolean isUrgentPartitionHealthy = true;

        public void accumulateStat(TabletCheckerStat stat) {
            totalTabletNum += stat.totalTabletNum;
            unhealthyTabletNum += stat.unhealthyTabletNum;
            addToSchedulerTabletNum += stat.addToSchedulerTabletNum;
            tabletInScheduler += stat.tabletInScheduler;
            tabletNotReady += stat.tabletNotReady;
            waitTotalTime += stat.waitTotalTime;
            isUrgentPartitionHealthy = stat.isUrgentPartitionHealthy;
        }
    }

    private TabletCheckerStat doCheckOnePartition(Database db, OlapTable olapTbl, Partition partition,
                                                  int replicaNum, List<Long> aliveBeIdsInCluster,
                                                  boolean isPartitionUrgent) {
        TabletCheckerStat partitionTabletCheckerStat = new TabletCheckerStat();
        // Tablet in SHADOW index can not be repaired or balanced
        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
            for (MaterializedIndex idx : physicalPartition.getMaterializedIndices(
                    IndexExtState.VISIBLE)) {
                for (Tablet tablet : idx.getTablets()) {
                    LocalTablet localTablet = (LocalTablet) tablet;
                    partitionTabletCheckerStat.totalTabletNum++;

                    if (tabletScheduler.containsTablet(tablet.getId())) {
                        partitionTabletCheckerStat.tabletInScheduler++;
                        continue;
                    }

                    SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                    Pair<TabletHealthStatus, TabletSchedCtx.Priority> statusWithPrio =
                            TabletChecker.getTabletHealthStatusWithPriority(
                                    localTablet,
                                    systemInfoService,
                                    physicalPartition.getVisibleVersion(),
                                    replicaNum,
                                    aliveBeIdsInCluster,
                                    olapTbl.getLocation());

                    if (statusWithPrio.first == TabletHealthStatus.HEALTHY) {
                        // Only set last status check time when status is healthy.
                        localTablet.setLastStatusCheckTime(System.currentTimeMillis());
                        continue;
                    } else if (isPartitionUrgent) {
                        statusWithPrio.second = TabletSchedCtx.Priority.VERY_HIGH;
                        partitionTabletCheckerStat.isUrgentPartitionHealthy = false;
                    }

                    partitionTabletCheckerStat.unhealthyTabletNum++;

                    if (!localTablet.readyToBeRepaired(statusWithPrio.first, statusWithPrio.second)) {
                        partitionTabletCheckerStat.tabletNotReady++;
                        continue;
                    }

                    if (statusWithPrio.first == TabletHealthStatus.LOCATION_MISMATCH &&
                            !preCheckEnoughLocationMatchedBackends(olapTbl.getLocation(), replicaNum)) {
                        continue;
                    }

                    TabletSchedCtx tabletSchedCtx = new TabletSchedCtx(
                            TabletSchedCtx.Type.REPAIR,
                            db.getId(), olapTbl.getId(), partition.getId(),
                            physicalPartition.getId(), idx.getId(), tablet.getId(),
                            System.currentTimeMillis());
                    // the tablet status will be set again when being scheduled
                    tabletSchedCtx.setTabletStatus(statusWithPrio.first);
                    tabletSchedCtx.setOrigPriority(statusWithPrio.second);
                    tabletSchedCtx.setTablet(localTablet);
                    tabletSchedCtx.setRequiredLocation(olapTbl.getLocation());
                    tabletSchedCtx.setReplicaNum(replicaNum);
                    if (!tryChooseSrcBeforeSchedule(tabletSchedCtx)) {
                        continue;
                    }

                    Pair<Boolean, Long> result =
                            tabletScheduler.blockingAddTabletCtxToScheduler(db, tabletSchedCtx,
                                    isPartitionUrgent);
                    partitionTabletCheckerStat.waitTotalTime += result.second;
                    if (result.first) {
                        partitionTabletCheckerStat.addToSchedulerTabletNum++;
                    }
                }
            } // indices
        }

        return partitionTabletCheckerStat;
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

    private boolean preCheckEnoughLocationMatchedBackends(Multimap<String, String> requiredLocation, int replicaNum) {
        List<List<Long>> locBackendIdList = new ArrayList<>();
        List<ComputeNode> availableBackends = Lists.newArrayList();
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        availableBackends.addAll(systemInfoService.getAvailableBackends());
        return NodeSelector.getLocationMatchedBackendIdList(locBackendIdList, availableBackends,
                requiredLocation, systemInfoService) >= replicaNum;
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

    private static class LocalTabletHealthStats {
        /**
         * Number of replicas that are alive, i.e. corresponding backend hasn't dropped and is alive,
         * the replica state is normal.
         */
        public int aliveCnt = 0;
        /**
         * Number of replicas that are alive and version complete, i.e. don't have missing versions.
         */
        public int aliveAndVersionCompleteCnt = 0;
        /**
         * Number of replicas that are alive, version complete and backend is stable,
         * i.e. backend is not decommissioned.
         */
        public int backendStableCnt = 0;
        /**
         * Number of replicas that are alive, version complete, backend is stable and disk is also stable,
         * i.e. the disk of the backend where the replica resides on is not decommissioned
         */
        public int diskStableCnt = 0;
        /**
         * Number of replicas that are alive, version complete, backend is stable, disk is also stable, and
         * the replica matches the location requirement of table.
         */
        public int locationMatchCnt = 0;

        /**
         * Replica which is alive, but need further repair, i.e. need to incrementally clone missing versions first.
         * See comments for {@link Replica#needFurtherRepair()} for more details.
         */
        Replica needFurtherRepairReplica = null;

        // all getters and setters
        public int getAliveCnt() {
            return aliveCnt;
        }

        public void incrAliveCnt() {
            aliveCnt++;
        }

        public int getAliveAndVersionCompleteCnt() {
            return aliveAndVersionCompleteCnt;
        }

        public void incrAliveAndVersionCompleteCnt() {
            aliveAndVersionCompleteCnt++;
        }

        public int getBackendStableCnt() {
            return backendStableCnt;
        }

        public void incrBackendStableCnt() {
            backendStableCnt++;
        }

        public int getDiskStableCnt() {
            return diskStableCnt;
        }

        public void incrDiskStableCnt() {
            diskStableCnt++;
        }

        public int getLocationMatchCnt() {
            return locationMatchCnt;
        }

        public void incrLocationMatchCnt() {
            locationMatchCnt++;
        }

        public Replica getNeedFurtherRepairReplica() {
            return needFurtherRepairReplica;
        }

        public void setNeedFurtherRepairReplica(Replica needFurtherRepairReplica) {
            this.needFurtherRepairReplica = needFurtherRepairReplica;
        }
    }

    private static Pair<TabletHealthStatus, TabletSchedCtx.Priority> createRedundantSchedCtx(
            TabletHealthStatus status, TabletSchedCtx.Priority prio, Replica needFurtherRepairReplica) {
        if (needFurtherRepairReplica != null) {
            return Pair.create(TabletHealthStatus.NEED_FURTHER_REPAIR, TabletSchedCtx.Priority.HIGH);
        }
        return Pair.create(status, prio);
    }

    /**
     * For certain deployment, like k8s pods + pvc, the replica is not lost even the
     * corresponding backend is detected as dead, because the replica data is persisted
     * on a pvc which is backed by a remote storage service, such as AWS EBS. And later,
     * k8s control place will schedule a new pod and attach the pvc to it which will
     * restore the replica to a {@link Replica.ReplicaState#NORMAL} state immediately. But normally
     * the {@link com.starrocks.clone.TabletScheduler} of Starrocks will start to schedule
     * {@link TabletHealthStatus#REPLICA_MISSING} tasks and create new replicas in a short time.
     * After new pod scheduling is completed, {@link com.starrocks.clone.TabletScheduler} has
     * to delete the redundant healthy replica which cause resource waste and may also affect
     * the loading process.
     *
     * <p>This method checks whether the corresponding backend of tablet replica is dead or not.
     * Only when the backend has been dead for {@link Config#tablet_sched_be_down_tolerate_time_s}
     * seconds, will this method returns true.
     */
    private static boolean isReplicaBackendDead(Backend backend) {
        long currentTimeMs = System.currentTimeMillis();
        assert backend != null;
        return !backend.isAlive() &&
                (currentTimeMs - backend.getLastUpdateMs() > Config.tablet_sched_be_down_tolerate_time_s * 1000);
    }

    private static boolean isReplicaBackendDropped(Backend backend) {
        return backend == null;
    }

    private static boolean isReplicaStateAbnormal(Replica replica, Backend backend, Set<String> replicaHostSet) {
        assert backend != null && replica != null;
        return replica.getState() == Replica.ReplicaState.CLONE
                || replica.getState() == Replica.ReplicaState.DECOMMISSION
                || replica.isBad()
                || !replicaHostSet.add(backend.getHost());
    }

    private static boolean needRecoverWithEmptyTablet(LocalTablet localTablet, SystemInfoService systemInfoService) {
        List<Replica> replicas = localTablet.getImmutableReplicas();
        try (CloseableLock ignored = CloseableLock.lock(localTablet.getReadLock())) {
            if (Config.recover_with_empty_tablet && replicas.size() > 1) {
                int numReplicaLostForever = 0;
                int numReplicaRecoverable = 0;
                for (Replica replica : replicas) {
                    if (replica.isBad() || systemInfoService.getBackend(replica.getBackendId()) == null) {
                        numReplicaLostForever++;
                    } else {
                        numReplicaRecoverable++;
                    }
                }

                return numReplicaLostForever > 0 && numReplicaRecoverable == 0;
            }
        }

        return false;
    }

    public static Pair<TabletHealthStatus, TabletSchedCtx.Priority> getTabletHealthStatusWithPriority(
            LocalTablet tablet,
            SystemInfoService systemInfoService,
            long visibleVersion, int replicationNum,
            List<Long> aliveBeIdsInCluster, Multimap<String, String> requiredLocation) {
        try (CloseableLock ignored = CloseableLock.lock(tablet.getReadLock())) {
            return getTabletHealthStatusWithPriorityUnlocked(tablet, systemInfoService, visibleVersion,
                    replicationNum, aliveBeIdsInCluster, requiredLocation);
        }
    }

    public static boolean isLocationMatch(long backendId, Multimap<String, String> requiredLocation) {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        return isLocationMatch(backendId, requiredLocation, systemInfoService);
    }

    public static boolean isLocationMatch(String backendLocKey,
                                          String backendLocVal,
                                          Multimap<String, String> requiredLocation) {
        if (requiredLocation == null || requiredLocation.isEmpty()) {
            return true;
        } else {
            return requiredLocation.keySet().contains("*") ||
                    (requiredLocation.keySet().contains(backendLocKey) &&
                            (requiredLocation.get(backendLocKey).contains("*") ||
                                    requiredLocation.get(backendLocKey).contains(backendLocVal)));
        }
    }

    private static boolean isLocationMatch(long backendId,
                                           Multimap<String, String> requiredLocation,
                                           SystemInfoService systemInfoService) {
        if (requiredLocation == null || requiredLocation.isEmpty()) {
            return true;
        }

        Backend backend = systemInfoService.getBackend(backendId);
        if (backend == null) {
            return false;
        }

        Pair<String, String> backendLocKV = backend.getSingleLevelLocationKV();
        return isLocationMatch(requiredLocation, backendLocKV);
    }

    public static boolean isLocationMatch(Multimap<String, String> requiredLocation,
                                          Pair<String, String> backendLocKV) {
        if (backendLocKV == null) {
            return false;
        } else {
            return isLocationMatch(backendLocKV.first, backendLocKV.second, requiredLocation);
        }
    }

    private static LocalTabletHealthStats collectLocalTabletHealthStats(LocalTablet localTablet,
                                                                        SystemInfoService systemInfoService,
                                                                        long visibleVersion,
                                                                        Multimap<String, String> requiredLocation) {
        LocalTabletHealthStats stats = new LocalTabletHealthStats();
        Set<Pair<String, String>> uniqueReplicaLocations = Sets.newHashSet();
        Set<String> hosts = Sets.newHashSet();
        List<Replica> replicas = localTablet.getImmutableReplicas();
        for (Replica replica : replicas) {
            Backend backend = systemInfoService.getBackend(replica.getBackendId());
            if (isReplicaBackendDropped(backend)
                    || isReplicaBackendDead(backend)
                    || isReplicaStateAbnormal(replica, backend, hosts)) {
                // this replica is not alive,
                // or if this replica is on same host with another replica, we also treat it as 'dead',
                // so that Tablet Scheduler will create a new replica on different host.
                // ATTN: Replicas on same host is a bug of previous StarRocks version, so we fix it by this way.
                continue;
            }
            stats.incrAliveCnt();

            if (replica.needFurtherRepair() && stats.getNeedFurtherRepairReplica() == null) {
                stats.setNeedFurtherRepairReplica(replica);
            }

            if (replica.getLastFailedVersion() > 0 || replica.getVersion() < visibleVersion) {
                // this replica is alive but version incomplete
                continue;
            }
            stats.incrAliveAndVersionCompleteCnt();

            if (backend.isDecommissioned()) {
                // this replica is alive, version complete, but backend is not available
                continue;
            }
            stats.incrBackendStableCnt();

            if (backend.isDiskDecommissioned(replica.getPathHash())) {
                // disk in decommission state
                continue;
            }
            stats.incrDiskStableCnt();

            if (requiredLocation == null || requiredLocation.isEmpty()) {
                stats.incrLocationMatchCnt();
            } else {
                Pair<String, String> backendLocKV = backend.getSingleLevelLocationKV();
                if (backendLocKV != null) {
                    if (!uniqueReplicaLocations.contains(backendLocKV) &&
                            isLocationMatch(backendLocKV.first, backendLocKV.second, requiredLocation)) {
                        stats.incrLocationMatchCnt();
                        uniqueReplicaLocations.add(backendLocKV);
                    }
                }
            }
        }

        return stats;
    }

    private static Pair<TabletHealthStatus, TabletSchedCtx.Priority> getStatusWhenNotEnoughAliveReplicas(
            LocalTablet localTablet,
            LocalTabletHealthStats stats,
            int replicationNum,
            List<Long> aliveBeIdsInCluster,
            SystemInfoService systemInfoService,
            List<Replica> replicas) {
        int aliveBackendsNum = aliveBeIdsInCluster.size();
        // check whether we need to forcefully recover with an empty tablet first
        // we use a FORCE_REDUNDANT task to drop the invalid replica first and
        // then REPLICA_MISSING task will try to create that empty tablet
        if (needRecoverWithEmptyTablet(localTablet, systemInfoService)) {
            LOG.info("need to forcefully recover with empty tablet for {}, replica info:{}",
                    localTablet.getId(), localTablet.getReplicaInfos());
            return createRedundantSchedCtx(TabletHealthStatus.FORCE_REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH,
                    stats.getNeedFurtherRepairReplica());
        }

        if (stats.getAliveCnt() < replicationNum && replicas.size() >= aliveBackendsNum
                && aliveBackendsNum >= replicationNum && replicationNum > 1) {
            // there is no enough backend for us to create a new replica, so we have to delete an existing replica,
            // so there can be available backend for us to create a new replica.
            // And if there is only one replica, we will not handle it(maybe need human interference)
            // condition explain:
            // 1. alive < replicationNum: replica is missing or bad
            // 2. replicas.size() >= aliveBackendsNum: the existing replicas occupies all available backends
            // 3. aliveBackendsNum >= replicationNum: make sure after deletion, there will be
            //    at least one backend for new replica.
            // 4. replicationNum > 1: if replication num is set to 1, do not delete any replica, for safety reason
            // For example: 3 replica, 3 be, one set bad, we need to forcefully delete one first
            return createRedundantSchedCtx(TabletHealthStatus.FORCE_REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH,
                    stats.getNeedFurtherRepairReplica());
        } else {
            List<Long> availableBEs = systemInfoService.getAvailableBackendIds();
            // We create `REPLICA_MISSING` type task only when there exists enough available BEs which
            // we can choose to clone data to, if not we should check if we can create `VERSION_INCOMPLETE` task,
            // so that repair of replica with incomplete version won't be blocked and hence version publish process
            // of load task won't be blocked either.
            if (availableBEs.size() > stats.getAliveCnt()) {
                if (stats.getAliveCnt() < (replicationNum / 2) + 1) {
                    return Pair.create(TabletHealthStatus.REPLICA_MISSING, TabletSchedCtx.Priority.HIGH);
                } else if (stats.getAliveCnt() < replicationNum) {
                    return Pair.create(TabletHealthStatus.REPLICA_MISSING, TabletSchedCtx.Priority.NORMAL);
                }
            }
        }

        return null;
    }

    private static Pair<TabletHealthStatus, TabletSchedCtx.Priority> getStatusWhenNotEnoughVerCompleteReplicas(
            LocalTabletHealthStats stats,
            int replicationNum) {
        if (stats.getAliveAndVersionCompleteCnt() < (replicationNum / 2) + 1) {
            return Pair.create(TabletHealthStatus.VERSION_INCOMPLETE, TabletSchedCtx.Priority.HIGH);
        } else if (stats.getAliveAndVersionCompleteCnt() < replicationNum) {
            return Pair.create(TabletHealthStatus.VERSION_INCOMPLETE, TabletSchedCtx.Priority.NORMAL);
        } else if (stats.getAliveAndVersionCompleteCnt() > replicationNum) {
            // we set REDUNDANT as VERY_HIGH, because delete redundant replicas can free the space quickly.
            return createRedundantSchedCtx(TabletHealthStatus.REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH,
                    stats.getNeedFurtherRepairReplica());
        }

        return null;
    }

    private static Pair<TabletHealthStatus, TabletSchedCtx.Priority> getStatusWhenBackendDecommissioned(
            LocalTabletHealthStats stats,
            int replicationNum,
            List<Long> aliveBeIdsInCluster,
            SystemInfoService systemInfoService,
            List<Replica> replicas) {
        if (stats.getBackendStableCnt() < replicationNum) {
            Set<Long> replicaBeIds = replicas.stream()
                    .map(Replica::getBackendId).collect(Collectors.toSet());
            List<Long> availableBeIds = aliveBeIdsInCluster.stream()
                    .filter(systemInfoService::checkBackendAvailable)
                    .collect(Collectors.toList());
            if (replicaBeIds.containsAll(availableBeIds)
                    && availableBeIds.size() >= replicationNum
                    && replicationNum > 1) { // Doesn't have any BE that can be chosen to create a new replica
                return createRedundantSchedCtx(TabletHealthStatus.FORCE_REDUNDANT,
                        stats.getBackendStableCnt() < (replicationNum / 2) + 1 ? TabletSchedCtx.Priority.NORMAL :
                                TabletSchedCtx.Priority.LOW, stats.getNeedFurtherRepairReplica());
            }
            if (stats.getBackendStableCnt() < (replicationNum / 2) + 1) {
                return Pair.create(TabletHealthStatus.REPLICA_RELOCATING, TabletSchedCtx.Priority.NORMAL);
            } else {
                return Pair.create(TabletHealthStatus.REPLICA_RELOCATING, TabletSchedCtx.Priority.LOW);
            }
        }

        return null;
    }

    /**
     * A replica is healthy only if
     * 1. the backend is available
     * 2. the disk is not decommissioned
     * 3. the replica state is normal, e.g. not in BAD state
     * 4. replica version is caught up, and last failed version is -1
     * <p>
     * A tablet is healthy only if
     * 1. healthy replica num is equal to replicationNum
     * 2. all healthy replicas are in right location (if specified)
     */
    private static Pair<TabletHealthStatus, TabletSchedCtx.Priority> getTabletHealthStatusWithPriorityUnlocked(
            LocalTablet localTablet,
            SystemInfoService systemInfoService,
            long visibleVersion, int replicationNum,
            List<Long> aliveBeIdsInCluster,
            Multimap<String, String> requiredLocation) {
        List<Replica> replicas = localTablet.getImmutableReplicas();
        LocalTabletHealthStats stats = collectLocalTabletHealthStats(localTablet, systemInfoService,
                visibleVersion, requiredLocation);

        // The priority of handling different unhealthy situations should be:
        // FORCE_REDUNDANT > REPLICA_MISSING > VERSION_INCOMPLETE >
        // REPLICA_RELOCATING > DISK_MIGRATION > LOCATION_MISMATCH > REDUNDANT.
        // And the counter should also be counted based on this priority.
        // Handling the lower priority means that the higher priority situation doesn't exist,
        // we don't to care about it.
        // The reason why FORCE_REDUNDANT is at top priority is that we need to
        // drop some replica first in order to clone new replica in some situation.
        // 1. alive replicas are not enough
        Pair<TabletHealthStatus, TabletSchedCtx.Priority> result = getStatusWhenNotEnoughAliveReplicas(
                localTablet, stats, replicationNum, aliveBeIdsInCluster, systemInfoService, replicas);
        if (result != null) {
            return result;
        }

        // 2. version complete replicas are not enough
        result = getStatusWhenNotEnoughVerCompleteReplicas(stats, replicationNum);
        if (result != null) {
            return result;
        }

        // 3. replica is under relocation
        result = getStatusWhenBackendDecommissioned(
                stats, replicationNum, aliveBeIdsInCluster, systemInfoService, replicas);
        if (result != null) {
            return result;
        }

        // 4. disk decommission
        if (stats.getDiskStableCnt() < replicationNum) {
            return Pair.create(TabletHealthStatus.DISK_MIGRATION, TabletSchedCtx.Priority.NORMAL);
        }

        // 5. replica redundant
        if (replicas.size() > replicationNum) {
            // we set REDUNDANT as VERY_HIGH, because delete redundant replicas can free the space quickly.
            return createRedundantSchedCtx(TabletHealthStatus.REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH,
                    stats.getNeedFurtherRepairReplica());
        }

        // 6. replica location doesn't match the location requirement of table
        // should be after REDUNDANT, because if we cannot find enough location matched backends,
        // the deletion of redundant replica will be blocked constantly.
        if (stats.getLocationMatchCnt() < replicationNum) {
            return Pair.create(TabletHealthStatus.LOCATION_MISMATCH, TabletSchedCtx.Priority.NORMAL);
        }

        // 7. healthy
        return Pair.create(TabletHealthStatus.HEALTHY, TabletSchedCtx.Priority.NORMAL);
    }

    public static TabletHealthStatus getColocateTabletHealthStatus(LocalTablet localTablet, long visibleVersion,
                                                                   int replicationNum, Set<Long> backendsSet) {
        try (CloseableLock ignored = CloseableLock.lock(localTablet.getReadLock())) {
            return getColocateTabletHealthStatusUnlocked(localTablet, visibleVersion, replicationNum, backendsSet);
        }
    }

    /**
     * Check colocate table's tablet health
     * <p>
     * 1. Mismatch:<p>
     * backends set:       1,2,3<p>
     * tablet replicas:    1,2,5
     * <p>
     * backends set:       1,2,3<p>
     * tablet replicas:    1,2
     * <p>
     * backends set:       1,2,3<p>
     * tablet replicas:    1,2,4,5
     * <p>
     * 2. Version incomplete:<p>
     * backend matched, but some replica's version is incomplete
     * <p>
     * 3. Redundant:<p>
     * backends set:       1,2,3<p>
     * tablet replicas:    1,2,3,4
     * <p>
     * 4. Replica bad:<p>
     * If a replica is marked bad, we need to migrate it and all the other replicas corresponding to the same bucket
     * index in the colocate group to another backend, but the backend where the bad replica sits on may still be
     * available, so the backend set won't be changed by ColocateBalancer, so we need to learn this state and update
     * the backend set to replace the backend that has the bad replica. In order to be in consistent with the current
     * logic, we return COLOCATE_MISMATCH not REPLICA_MISSING.
     * <p></p>
     * No need to check if backend is available. We consider all backends in 'backendsSet' are available,
     * If not, unavailable backends will be relocated by ColocateTableBalancer first.
     */
    private static TabletHealthStatus getColocateTabletHealthStatusUnlocked(LocalTablet localTablet,
                                                                            long visibleVersion,
                                                                            int replicationNum, Set<Long> backendsSet) {
        // 1. check if replicas' backends are mismatch
        Set<Long> replicaBackendIds = localTablet.getBackendIds();
        for (Long backendId : backendsSet) {
            if (!replicaBackendIds.contains(backendId)
                    && containsAnyHighPrioBackend(replicaBackendIds,
                    Config.tablet_sched_colocate_balance_high_prio_backends)) {
                return TabletHealthStatus.COLOCATE_MISMATCH;
            }
        }

        int diskStableCnt = 0;
        List<Replica> replicas = localTablet.getImmutableReplicas();
        // 2. check version completeness
        for (Replica replica : replicas) {
            // do not check the replica that is not in the colocate backend set,
            // this kind of replica should be dropped.
            if (!backendsSet.contains(replica.getBackendId())) {
                continue;
            }

            if (replica.isBad()) {
                LOG.debug("colocate tablet {} has bad replica, need to drop-then-repair, " +
                                "current backend set: {}, visible version: {}", localTablet.getId(), backendsSet,
                        visibleVersion);
                // we use `TabletScheduler#handleColocateRedundant()` to drop bad replica forcefully.
                return TabletHealthStatus.COLOCATE_REDUNDANT;
            }

            if (replica.getLastFailedVersion() > 0 || replica.getVersion() < visibleVersion) {
                // this replica is alive but version incomplete
                return TabletHealthStatus.VERSION_INCOMPLETE;
            }

            SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            Backend backend = systemInfoService.getBackend(replica.getBackendId());
            if (backend != null && !backend.isDiskDecommissioned(replica.getPathHash())) {
                diskStableCnt++;
            }
        }

        // 3. check disk decommission
        if (diskStableCnt < replicationNum) {
            return TabletHealthStatus.DISK_MIGRATION;
        }

        // 4. check redundant
        if (replicas.size() > replicationNum) {
            return TabletHealthStatus.COLOCATE_REDUNDANT;
        }

        return TabletHealthStatus.HEALTHY;
    }

    private static boolean containsAnyHighPrioBackend(Set<Long> backendIds, long[] highPriorityBackendIds) {
        if (highPriorityBackendIds == null || highPriorityBackendIds.length == 0) {
            return true;
        }

        for (long beId : highPriorityBackendIds) {
            if (backendIds.contains(beId)) {
                return true;
            }
        }

        return false;
    }
}
