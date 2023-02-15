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
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.Tablet;
import com.starrocks.clone.TabletScheduler.AddResult;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * This checker is responsible for checking all unhealthy tablets.
 * It does not responsible for any scheduler of tablet repairing or balance
 */
public class TabletChecker extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletChecker.class);

    private GlobalStateMgr globalStateMgr;
    private SystemInfoService infoService;
    private TabletScheduler tabletScheduler;
    private TabletSchedulerStat stat;

    // db id -> (tbl id -> PrioPart)
    // priority of replicas of partitions in this table will be set to VERY_HIGH if not healthy
    private com.google.common.collect.Table<Long, Long, Set<PrioPart>> prios = HashBasedTable.create();

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

    public TabletChecker(GlobalStateMgr globalStateMgr, SystemInfoService infoService, TabletScheduler tabletScheduler,
                         TabletSchedulerStat stat) {
        super("tablet checker", Config.tablet_sched_checker_interval_seconds * 1000L);
        this.globalStateMgr = globalStateMgr;
        this.infoService = infoService;
        this.tabletScheduler = tabletScheduler;
        this.stat = stat;
    }

    private void addPrios(RepairTabletInfo repairTabletInfo, long timeoutMs) {
        Preconditions.checkArgument(!repairTabletInfo.partIds.isEmpty());
        long currentTime = System.currentTimeMillis();
        synchronized (prios) {
            Set<PrioPart> parts = prios.get(repairTabletInfo.dbId, repairTabletInfo.tblId);
            if (parts == null) {
                parts = Sets.newHashSet();
                prios.put(repairTabletInfo.dbId, repairTabletInfo.tblId, parts);
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

    private void removePrios(RepairTabletInfo repairTabletInfo) {
        Preconditions.checkArgument(!repairTabletInfo.partIds.isEmpty());
        synchronized (prios) {
            Map<Long, Set<PrioPart>> tblMap = prios.row(repairTabletInfo.dbId);
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
        int pendingNum = tabletScheduler.getPendingNum();
        int runningNum = tabletScheduler.getRunningNum();
        if (pendingNum > Config.tablet_sched_max_scheduling_tablets
                || runningNum > Config.tablet_sched_max_scheduling_tablets) {
            LOG.info("too many tablets are being scheduled. pending: {}, running: {}, limit: {}. skip check",
                    pendingNum, runningNum, Config.tablet_sched_max_scheduling_tablets);
            return;
        }

        checkAllTablets();

        removePriosIfNecessary();

        stat.counterTabletCheckRound.incrementAndGet();
        LOG.info(stat.incrementalBrief());
    }

    /**
     * Check the manually repaired table/partition first,
     * so that they can be scheduled for repair at first place.
     */
    private void checkAllTablets() {
        checkTabletsOnlyInPrios();
        checkTabletsNotInPrios();
    }

    private void checkTabletsOnlyInPrios() {
        doCheck(true);
    }

    private void checkTabletsNotInPrios() {
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

    private void doCheck(boolean checkInPrios) {
        long start = System.nanoTime();
        long totalTabletNum = 0;
        long unhealthyTabletNum = 0;
        long addToSchedulerTabletNum = 0;
        long tabletInScheduler = 0;
        long tabletNotReady = 0;

        long lockTotalTime = 0;
        long lockStart;
        List<Long> dbIds = globalStateMgr.getDbIdsIncludeRecycleBin();
        DATABASE:
        for (Long dbId : dbIds) {
            Database db = globalStateMgr.getDbIncludeRecycleBin(dbId);
            if (db == null) {
                continue;
            }

            if (db.isInfoSchemaDb()) {
                continue;
            }

            // set the config to a local variable to avoid config params changed.
            int partitionBatchNum = Config.tablet_checker_partition_batch_num;
            int partitionChecked = 0;
            db.readLock();
            lockStart = System.nanoTime();
            try {
                List<Long> aliveBeIdsInCluster = infoService.getBackendIds(true);
                TABLE:
                for (Table table : globalStateMgr.getTablesIncludeRecycleBin(db)) {
                    if (!table.needSchedule(false)) {
                        continue;
                    }
                    if (table.isCloudNativeTable()) {
                        // replicas are managed by StarOS and cloud storage.
                        continue;
                    }

                    if ((checkInPrios && !isTableInPrios(dbId, table.getId())) ||
                            (!checkInPrios && isTableInPrios(dbId, table.getId()))) {
                        continue;
                    }

                    OlapTable olapTbl = (OlapTable) table;
                    for (Partition partition : globalStateMgr.getAllPartitionsIncludeRecycleBin(olapTbl)) {
                        partitionChecked++;
                        if (partitionChecked % partitionBatchNum == 0) {
                            LOG.debug("partition checked reached batch value, release lock");
                            lockTotalTime += System.nanoTime() - lockStart;
                            // release lock, so that lock can be acquired by other threads.
                            db.readUnlock();
                            db.readLock();
                            LOG.debug("checker get lock again");
                            lockStart = System.nanoTime();
                            if (globalStateMgr.getDbIncludeRecycleBin(dbId) == null) {
                                continue DATABASE;
                            }
                            if (globalStateMgr.getTableIncludeRecycleBin(db, olapTbl.getId()) == null) {
                                continue TABLE;
                            }
                            if (globalStateMgr.getPartitionIncludeRecycleBin(olapTbl, partition.getId()) == null) {
                                continue;
                            }
                        }
                        if (partition.getState() != PartitionState.NORMAL) {
                            // when alter job is in FINISHING state, partition state will be set to NORMAL,
                            // and we can schedule the tablets in it.
                            continue;
                        }

                        short replicaNum = globalStateMgr.getReplicationNumIncludeRecycleBin(olapTbl.getPartitionInfo(),
                                partition.getId());
                        if (replicaNum == (short) -1) {
                            continue;
                        }

                        boolean isPartitionInPrios = isPartitionInPrios(dbId, table.getId(), partition.getId());
                        boolean prioPartIsHealthy = true;
                        if ((checkInPrios && !isPartitionInPrios) || (!checkInPrios && isPartitionInPrios)) {
                            continue;
                        }

                        /*
                         * Tablet in SHADOW index can not be repaired of balanced
                         */
                        for (MaterializedIndex idx : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Tablet tablet : idx.getTablets()) {
                                LocalTablet localTablet = (LocalTablet) tablet;
                                totalTabletNum++;

                                if (tabletScheduler.containsTablet(tablet.getId())) {
                                    tabletInScheduler++;
                                    continue;
                                }

                                Pair<TabletStatus, TabletSchedCtx.Priority> statusWithPrio =
                                        localTablet.getHealthStatusWithPriority(
                                                infoService,
                                                partition.getVisibleVersion(),
                                                replicaNum,
                                                aliveBeIdsInCluster);

                                if (statusWithPrio.first == TabletStatus.HEALTHY) {
                                    // Only set last status check time when status is healthy.
                                    localTablet.setLastStatusCheckTime(System.currentTimeMillis());
                                    continue;
                                } else if (isPartitionInPrios) {
                                    statusWithPrio.second = TabletSchedCtx.Priority.VERY_HIGH;
                                    prioPartIsHealthy = false;
                                }

                                unhealthyTabletNum++;

                                if (!localTablet.readyToBeRepaired(statusWithPrio.first, statusWithPrio.second)) {
                                    tabletNotReady++;
                                    continue;
                                }

                                TabletSchedCtx tabletCtx = new TabletSchedCtx(
                                        TabletSchedCtx.Type.REPAIR,
                                        db.getId(), olapTbl.getId(),
                                        partition.getId(), idx.getId(), tablet.getId(),
                                        System.currentTimeMillis());
                                // the tablet status will be set again when being scheduled
                                tabletCtx.setTabletStatus(statusWithPrio.first);
                                tabletCtx.setOrigPriority(statusWithPrio.second);
                                tabletCtx.setTablet(localTablet);
                                if (!tryChooseSrcBeforeSchedule(tabletCtx)) {
                                    continue;
                                }

                                AddResult res = tabletScheduler.addTablet(tabletCtx, false /* not force */);
                                if (res == AddResult.LIMIT_EXCEED) {
                                    LOG.info("number of scheduling tablets in tablet scheduler"
                                            + " exceed to limit. stop tablet checker");
                                    break DATABASE;
                                } else if (res == AddResult.ADDED) {
                                    addToSchedulerTabletNum++;
                                }
                            }
                        } // indices

                        if (prioPartIsHealthy && isPartitionInPrios) {
                            // if all replicas in this partition are healthy, remove this partition from
                            // priorities.
                            LOG.debug("partition is healthy, remove from prios: {}-{}-{}",
                                    db.getId(), olapTbl.getId(), partition.getId());
                            removePrios(new RepairTabletInfo(db.getId(),
                                    olapTbl.getId(), Lists.newArrayList(partition.getId())));
                        }
                    } // partitions
                } // tables
            } finally {
                lockTotalTime += System.nanoTime() - lockStart;
                db.readUnlock();
            }
        } // end for dbs

        long cost = (System.nanoTime() - start) / 1000000;
        lockTotalTime = lockTotalTime / 1000000;

        stat.counterTabletCheckCostMs.addAndGet(cost);
        stat.counterTabletChecked.addAndGet(totalTabletNum);
        stat.counterUnhealthyTabletNum.addAndGet(unhealthyTabletNum);
        stat.counterTabletAddToBeScheduled.addAndGet(addToSchedulerTabletNum);

        LOG.info("finished to check tablets. checkInPrios: {}, " +
                        "unhealthy/total/added/in_sched/not_ready: {}/{}/{}/{}/{}, " +
                        "cost: {} ms, in lock time: {} ms",
                checkInPrios, unhealthyTabletNum, totalTabletNum, addToSchedulerTabletNum,
                tabletInScheduler, tabletNotReady, cost, lockTotalTime);
    }

    private boolean isTableInPrios(long dbId, long tblId) {
        synchronized (prios) {
            return prios.contains(dbId, tblId);
        }
    }

    private boolean isPartitionInPrios(long dbId, long tblId, long partId) {
        synchronized (prios) {
            if (isTableInPrios(dbId, tblId)) {
                return prios.get(dbId, tblId).contains(new PrioPart(partId, -1, -1));
            }
            return false;
        }
    }

    // remove partition from prios if:
    // 1. timeout
    // 2. meta not found
    private void removePriosIfNecessary() {
        com.google.common.collect.Table<Long, Long, Set<PrioPart>> copiedPrios = null;
        synchronized (prios) {
            copiedPrios = HashBasedTable.create(prios);
        }
        List<Pair<Long, Long>> deletedPrios = Lists.newArrayList();
        Iterator<Map.Entry<Long, Map<Long, Set<PrioPart>>>> iter = copiedPrios.rowMap().entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, Map<Long, Set<PrioPart>>> dbEntry = iter.next();
            long dbId = dbEntry.getKey();
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                iter.remove();
                continue;
            }

            db.readLock();
            try {
                Iterator<Map.Entry<Long, Set<PrioPart>>> jter = dbEntry.getValue().entrySet().iterator();
                while (jter.hasNext()) {
                    Map.Entry<Long, Set<PrioPart>> tblEntry = jter.next();
                    long tblId = tblEntry.getKey();
                    OlapTable tbl = (OlapTable) db.getTable(tblId);
                    if (tbl == null) {
                        deletedPrios.add(Pair.create(dbId, tblId));
                        continue;
                    }

                    Set<PrioPart> parts = tblEntry.getValue();
                    parts = parts.stream().filter(p -> (tbl.getPartition(p.partId) != null && !p.isTimeout())).collect(
                            Collectors.toSet());
                    if (parts.isEmpty()) {
                        deletedPrios.add(Pair.create(dbId, tblId));
                    }
                }

                if (dbEntry.getValue().isEmpty()) {
                    iter.remove();
                }
            } finally {
                db.readUnlock();
            }
        }
        for (Pair<Long, Long> prio : deletedPrios) {
            copiedPrios.remove(prio.first, prio.second);
        }
        prios = copiedPrios;
    }

    /*
     * handle ADMIN REPAIR TABLE stmt send by user.
     * This operation will add specified tables into 'prios', and tablets of this table will be set VERY_HIGH
     * when being scheduled.
     */
    public void repairTable(AdminRepairTableStmt stmt) throws DdlException {
        RepairTabletInfo repairTabletInfo =
                getRepairTabletInfo(stmt.getDbName(), stmt.getTblName(), stmt.getPartitions());
        addPrios(repairTabletInfo, stmt.getTimeoutS());
        LOG.info("repair database: {}, table: {}, partition: {}", repairTabletInfo.dbId, repairTabletInfo.tblId,
                repairTabletInfo.partIds);
    }

    /*
     * handle ADMIN CANCEL REPAIR TABLE stmt send by user.
     * This operation will remove the specified partitions from 'prios'
     */
    public void cancelRepairTable(AdminCancelRepairTableStmt stmt) throws DdlException {
        RepairTabletInfo repairTabletInfo =
                getRepairTabletInfo(stmt.getDbName(), stmt.getTblName(), stmt.getPartitions());
        removePrios(repairTabletInfo);
        LOG.info("cancel repair database: {}, table: {}, partition: {}", repairTabletInfo.dbId, repairTabletInfo.tblId,
                repairTabletInfo.partIds);
    }

    public int getPrioPartitionNum() {
        int count = 0;
        synchronized (prios) {
            for (Set<PrioPart> set : prios.values()) {
                count += set.size();
            }
        }
        return count;
    }

    public List<List<String>> getPriosInfo() {
        List<List<String>> infos = Lists.newArrayList();
        synchronized (prios) {
            for (Cell<Long, Long, Set<PrioPart>> cell : prios.cellSet()) {
                for (PrioPart part : cell.getValue()) {
                    List<String> row = Lists.newArrayList();
                    row.add(cell.getRowKey().toString());
                    row.add(cell.getColumnKey().toString());
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
        long tblId = -1;
        List<Long> partIds = Lists.newArrayList();
        db.readLock();
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
            db.readUnlock();
        }

        Preconditions.checkState(tblId != -1);

        return new RepairTabletInfo(dbId, tblId, partIds);
    }
}
