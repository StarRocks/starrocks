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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/consistency/ConsistencyChecker.java

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

package com.starrocks.consistency;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MetaObject;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.consistency.CheckConsistencyJob.JobState;
import com.starrocks.persist.ConsistencyCheckInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.task.CheckConsistencyTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConsistencyChecker extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(ConsistencyChecker.class);

    private static final int MAX_JOB_NUM = 100;
    private static final Comparator<MetaObject> COMPARATOR =
            (first, second) -> Long.signum(first.getLastCheckTime() - second.getLastCheckTime());

    // tabletId -> job
    private final Map<Long, CheckConsistencyJob> jobs;

    /*
     * ATTN:
     *      lock order is:
     *       jobs lock
     *       CheckConsistencyJob's synchronized
     *       db lock
     *
     * if reversal is inevitable. use db.tryLock() instead to avoid deadlock
     */
    private final ReentrantReadWriteLock jobsLock;

    private int startTime;
    private int endTime;
    private long lastTabletMetaCheckTime = 0;

    // Record the id of the table being created and ignore the check of the tablet of the table being created
    // to avoid deleting its tablets from TabletInvertedIndex by mistake.
    private final Map<Long, Integer> creatingTableCounters = new ConcurrentHashMap<>();

    public ConsistencyChecker() {
        super("consistency-checker");

        jobs = Maps.newHashMap();
        jobsLock = new ReentrantReadWriteLock();

        if (!initWorkTime()) {
            LOG.error("failed to init time in ConsistencyChecker. exit");
            System.exit(-1);
        }
    }

    private boolean initWorkTime() {
        // Using system time zone.
        SimpleDateFormat hourFormat = new SimpleDateFormat("HH");
        Date startDate;
        Date endDate;
        try {
            startDate = hourFormat.parse(Config.consistency_check_start_time);
            endDate = hourFormat.parse(Config.consistency_check_end_time);
            LOG.info("parsed startDate: {}, endDate: {}", startDate, endDate);
        } catch (ParseException e) {
            LOG.error("failed to parse start/end time: {}, {}", Config.consistency_check_start_time,
                    Config.consistency_check_end_time, e);
            return false;
        }

        if (startDate == null || endDate == null) {
            return false;
        }

        Calendar calendar = Calendar.getInstance();

        calendar.setTime(startDate);
        startTime = calendar.get(Calendar.HOUR_OF_DAY);

        calendar.setTime(endDate);
        endTime = calendar.get(Calendar.HOUR_OF_DAY);
        return true;
    }

    /**
     * Check the consistency between {@link com.starrocks.catalog.TabletInvertedIndex} and
     * {@link com.starrocks.server.LocalMetastore}.
     * <p>
     * If we find an invalid tablet, i.e. it's neither in current catalog nor in recycle bin,
     * we will remove it from {@link com.starrocks.catalog.TabletInvertedIndex} directly.
     * And this process will also output a report in `fe.log`, including valid number of
     * tablet and number of tablet in recycle bin for each backend.
     */
    private void checkTabletMetaConsistency(Map<Long, Integer> creatingTableIds) {
        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        TabletInvertedIndex tabletInvertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();

        Set<Long> invalidTablets = new HashSet<>();
        // backend id -> <num of currently existed tablet, num of tablet in recycle bin>
        Map<Long, Pair<Long, Long>> backendTabletNumReport = new HashMap<>();
        List<Long> backendIds = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds();

        long startTime = System.currentTimeMillis();
        long scannedTabletCount = 0;
        long numIgnoredTabletCausedByAbnormalState = 0;
        List<Long> ignoreTablets = new ArrayList<>();

        for (Long backendId : backendIds) {
            LOG.info("TabletMetaChecker: start to check tablet meta consistency for backend {}", backendId);
            List<Long> tabletIds = tabletInvertedIndex.getTabletIdsByBackendId(backendId);
            backendTabletNumReport.put(backendId, new Pair<>(0L, 0L));

            // TODO: group tablets by (dbId, tableId) before this loop and process each
            // group under a single lockTableWithIntensiveDbLock acquisition. Today we
            // acquire/release the per-table lock once per tablet, plus one getDb +
            // getTable lookup per tablet. With grouping, that drops to once per
            // (dbId, tableId) group: O(tables) instead of O(tablets). For backends
            // reporting tens of thousands of tablets clustered on far fewer tables --
            // the typical case -- this is a 10x-100x reduction in LockManager calls.
            // Even in the pathological 1-tablet-per-table case the cost is essentially
            // the same, so this is strictly beneficial in realistic deployments.
            for (Long tabletId : tabletIds) {
                scannedTabletCount++;
                boolean isInRecycleBin = false;

                TabletMeta tabletMeta = tabletInvertedIndex.getTabletMeta(tabletId);
                if (tabletMeta == null) {
                    deleteTabletByConsistencyChecker(null, tabletId, backendId, "tablet meta is null", invalidTablets);
                    continue;
                }

                // validate database
                long dbId = tabletMeta.getDbId();
                Database db = localMetastore.getDb(dbId);
                if (db == null) {
                    db = recycleBin.getDatabase(dbId);
                    if (db != null) {
                        isInRecycleBin = true;
                    } else {
                        deleteTabletByConsistencyChecker(tabletMeta, tabletId, backendId,
                                "database " + dbId + " doesn't exist", invalidTablets);
                        continue;
                    }
                }

                // Intensive path: IS on DB + READ on this one table. Each iteration
                // looks up exactly one table (by id) plus one partition / index / tablet
                // on that table; we never traverse db.getTables(). DROP TABLE / DROP
                // DATABASE still take DB WRITE which conflicts with IS, so the table
                // existence re-check below still races correctly.
                long tableId = tabletMeta.getTableId();
                Locker locker = new Locker();
                try {
                    locker.lockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);

                    // validate table
                    if (creatingTableIds.containsKey(tableId)) {
                        continue;
                    }
                    com.starrocks.catalog.Table table = db.getTable(tableId);
                    if (table == null) {
                        table = recycleBin.getTable(dbId, tableId);
                        if (table != null) {
                            isInRecycleBin = true;
                        } else {
                            deleteTabletByConsistencyChecker(tabletMeta, tabletId, backendId,
                                    "table " + dbId + "." + tableId + " doesn't exist", invalidTablets);
                            continue;
                        }
                    }

                    // To avoid delete tablet using by restore job, rollup job, schema change job etc.
                    if (table instanceof OlapTable &&
                            ((OlapTable) table).getState() != OlapTable.OlapTableState.NORMAL) {
                        if (ignoreTablets.size() < 20) {
                            ignoreTablets.add(tabletId);
                        }
                        numIgnoredTabletCausedByAbnormalState++;
                        continue;
                    }

                    // validate partition
                    long partitionId = tabletMeta.getPhysicalPartitionId();
                    PhysicalPartition physicalPartition = table.getPhysicalPartition(partitionId);
                    if (physicalPartition == null) {
                        physicalPartition = recycleBin.getPhysicalPartition(partitionId);
                        if (physicalPartition != null) {
                            isInRecycleBin = true;
                        } else {
                            deleteTabletByConsistencyChecker(tabletMeta, tabletId, backendId,
                                    "partition " + dbId + "." + tableId + "." + partitionId + " doesn't exist",
                                    invalidTablets);
                            continue;
                        }
                    }

                    // validate index
                    long indexId = tabletMeta.getIndexId();
                    MaterializedIndex index = physicalPartition.getIndex(indexId);
                    if (index == null) {
                        deleteTabletByConsistencyChecker(tabletMeta, tabletId, backendId,
                                "materialized index " + dbId + "." + tableId + "." +
                                        partitionId + "." + indexId + " doesn't exist",
                                invalidTablets);
                        continue;
                    }

                    if (!table.isCloudNativeTableOrMaterializedView()) {
                        // validate tablet
                        Tablet tablet = index.getTablet(tabletId);
                        if (tablet == null) {
                            deleteTabletByConsistencyChecker(tabletMeta, tabletId, backendId,
                                    "tablet " + dbId + "." + tableId + "." +
                                            partitionId + "." + indexId + "." + tabletId + " doesn't exist",
                                    invalidTablets);
                            continue;
                        }
                    }

                    if (isInRecycleBin) {
                        backendTabletNumReport.get(backendId).second++;
                    } else {
                        backendTabletNumReport.get(backendId).first++;
                    }

                    tabletMeta.resetToBeCleanedTime();
                } finally {
                    locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
                }
            } // end for tabletIds
        } // end for backendIds

        // logging report
        LOG.info("TabletMetaChecker has cleaned {} invalid tablet(s), scanned {} tablet(s) on {} backend(s) in {}ms," +
                        " total tablets in recycle bin: {}, total ignored tablets: {}, up to 20 of ignored tablets: {}, " +
                        "backend tablet count info(format: backend_id=curr_tablet_count:recycle_tablet_count): {}",
                invalidTablets.size(), scannedTabletCount, backendIds.size(),
                System.currentTimeMillis() - startTime,
                backendTabletNumReport.values().stream().mapToLong(p -> p.second).sum(),
                numIgnoredTabletCausedByAbnormalState, ignoreTablets,
                backendTabletNumReport);
    }

    private void deleteTabletByConsistencyChecker(TabletMeta tabletMeta, long tabletId, long backendId,
                                                  String reason, Set<Long> invalidTablets) {
        if (tabletMeta != null) {
            Long toBeCleanedTime = tabletMeta.getToBeCleanedTime();
            if (toBeCleanedTime == null) {
                // init `toBeCleanedTime` and delay the actual deletion to next round of check
                tabletMeta.setToBeCleanedTime(System.currentTimeMillis() +
                        Config.consistency_tablet_meta_check_interval_ms / 2);
                return;
            } else if (System.currentTimeMillis() < toBeCleanedTime) {
                return;
            }
        }

        LOG.info("TabletMetaChecker: delete tablet {} on backend {} from inverted index by" +
                        " consistency checker, because: {}",
                tabletId, backendId, reason);
        TabletInvertedIndex tabletInvertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        tabletInvertedIndex.deleteTablet(tabletId);
        invalidTablets.add(tabletId);
    }

    @Override
    protected void runAfterLeaseValid() {
        if (System.currentTimeMillis() - lastTabletMetaCheckTime > Config.consistency_tablet_meta_check_interval_ms) {
            checkTabletMetaConsistency(creatingTableCounters);
            lastTabletMetaCheckTime = System.currentTimeMillis();
        }

        // for each round. try chose enough new tablets to check
        // only add new job when it's work time
        if (itsTime() && getJobNum() == 0) {
            List<Long> chosenTabletIds = chooseTablets();
            for (Long tabletId : chosenTabletIds) {
                CheckConsistencyJob job = new CheckConsistencyJob(tabletId);
                addJob(job);
            }
        }

        jobsLock.writeLock().lock();
        try {
            // handle all jobs
            Iterator<Map.Entry<Long, CheckConsistencyJob>> iterator = jobs.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, CheckConsistencyJob> entry = iterator.next();
                CheckConsistencyJob oneJob = entry.getValue();

                JobState state = oneJob.getState();
                switch (state) {
                    case PENDING:
                        if (!oneJob.sendTasks()) {
                            clearJob(oneJob);
                            iterator.remove();
                        }
                        break;
                    case RUNNING:
                        int res = oneJob.tryFinishJob();
                        if (res == -1 || res == 1) {
                            // cancelled or finished
                            clearJob(oneJob);
                            iterator.remove();
                        }
                        break;
                    default:
                        break;
                }
            } // end while
        } finally {
            jobsLock.writeLock().unlock();
        }
    }

    @Override
    protected void onStopped() {
        // jobs holds in-flight CheckConsistencyJob instances and creatingTableCounters tracks
        // CREATE TABLE flows that are still running on this leader. Both belong to the current
        // leader session: in-flight create paths will fail when the editlog is sealed, so any
        // counter increments left behind would otherwise leak across sessions and incorrectly
        // mask tablets from future consistency checks. The watermark is reset for the same
        // reason - the next leader should re-issue a tablet-meta scan rather than honor a
        // timestamp captured by a different session.
        jobsLock.writeLock().lock();
        try {
            for (CheckConsistencyJob job : jobs.values()) {
                try {
                    job.clear();
                } catch (Throwable t) {
                    LOG.warn("clear consistency job for tablet {} failed", job.getTabletId(), t);
                }
            }
            jobs.clear();
        } finally {
            jobsLock.writeLock().unlock();
        }
        creatingTableCounters.clear();
        lastTabletMetaCheckTime = 0;
    }

    /*
     * check if time comes
     */
    private boolean itsTime() {
        if (startTime == endTime) {
            return false;
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        int currentTime = calendar.get(Calendar.HOUR_OF_DAY);

        boolean isTime;
        if (startTime < endTime) {
            isTime = currentTime >= startTime && currentTime <= endTime;
        } else {
            // startTime > endTime (across the day)
            isTime = currentTime >= startTime || currentTime <= endTime;
        }

        if (!isTime) {
            LOG.debug("current time is {}:00, waiting to {}:00 to {}:00",
                    currentTime, startTime, endTime);
        }

        return isTime;
    }

    private void clearJob(CheckConsistencyJob job) {
        job.clear();
        LOG.debug("tablet[{}] consistency checking job is cleared", job.getTabletId());
    }

    private boolean addJob(CheckConsistencyJob job) {
        this.jobsLock.writeLock().lock();
        try {
            if (jobs.containsKey(job.getTabletId())) {
                return false;
            } else {
                LOG.debug("add tablet[{}] to check consistency", job.getTabletId());
                jobs.put(job.getTabletId(), job);
                return true;
            }
        } finally {
            this.jobsLock.writeLock().unlock();
        }
    }

    private CheckConsistencyJob getJob(long tabletId) {
        this.jobsLock.readLock().lock();
        try {
            return jobs.get(tabletId);
        } finally {
            this.jobsLock.readLock().unlock();
        }
    }

    private int getJobNum() {
        this.jobsLock.readLock().lock();
        try {
            return jobs.size();
        } finally {
            this.jobsLock.readLock().unlock();
        }
    }

    /**
     * choose a tablet to check whether it's consistent
     * we use a priority queue to sort db/table/partition/index/tablet by 'lastCheckTime'.
     * chose a tablet which has the smallest 'lastCheckTime'.
     */
    protected List<Long> chooseTablets() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MetaObject chosenOne;

        List<Long> chosenTablets = Lists.newArrayList();

        // sort dbs
        List<Long> dbIds = globalStateMgr.getLocalMetastore().getDbIds();
        if (dbIds.isEmpty()) {
            return chosenTablets;
        }
        Queue<MetaObject> dbQueue = new PriorityQueue<>(dbIds.size(), COMPARATOR);
        for (Long dbId : dbIds) {
            if (dbId == 0L) {
                // skip 'information_schema' database
                continue;
            }
            Database db = globalStateMgr.getLocalMetastore().getDb(dbId);
            if (db == null) {
                continue;
            }
            dbQueue.add(db);
        }

        // must lock jobsLock first to obey the lock order rule
        this.jobsLock.readLock().lock();
        try {
            while ((chosenOne = dbQueue.poll()) != null) {
                Database db = (Database) chosenOne;
                Locker locker = new Locker();
                long startTime = System.currentTimeMillis();
                try {
                    // Lock-free snapshot: idToTable is a ConcurrentHashMap, so getTables()
                    // returns a weakly-consistent ArrayList that may miss a concurrent
                    // create or include a concurrent drop. That is fine here - this is a
                    // periodic heuristic, and the per-table re-fetch below is what actually
                    // guards against walking a dropped table or DB. Holding any DB-level
                    // lock just for this snapshot would be theatre: it is released before
                    // the per-table walk, so the snapshot is stale immediately afterwards
                    // regardless.
                    List<Table> tables = globalStateMgr.getLocalMetastore().getTables(db.getId());

                    // sort tables
                    Queue<MetaObject> tableQueue = new PriorityQueue<>(Math.max(tables.size(), 1), COMPARATOR);
                    for (Table table : tables) {
                        // Only check the OLAP table who is in NORMAL state.
                        // Because some tablets of the not NORMAL table may just a temporary presence in memory,
                        // if we check those tablets and log FinishConsistencyCheck to bdb,
                        // it will throw NullPointerException when replaying the log.
                        if (!table.isOlapTableOrMaterializedView() || ((OlapTable) table).getState() != OlapTableState.NORMAL) {
                            continue;
                        }
                        tableQueue.add(table);
                    }

                    while ((chosenOne = tableQueue.poll()) != null) {
                        OlapTable table = (OlapTable) chosenOne;
                        long tableId = table.getId();
                        // Take per-table READ under IS so concurrent schema change /
                        // partition mutation on other tables in the same DB can proceed.
                        locker.lockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
                        try {
                            if (chooseTabletsFromTable(globalStateMgr, db, table, chosenTablets)) {
                                return chosenTablets;
                            }
                        } finally {
                            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
                        }
                    } // end while tableQueue
                } finally {
                    // Since only at most `MAX_JOB_NUM` tablet are chosen, we don't need to release the per-table
                    // read lock from time to time, just log the time cost here.
                    LOG.info("choose tablets from db[{}-{}] took {}ms",
                                db.getFullName(), db.getId(), System.currentTimeMillis() - startTime);
                }
            } // end while dbQueue
        } finally {
            jobsLock.readLock().unlock();
        }

        return chosenTablets;
    }

    /**
     * Walk one table's partitions / indices / tablets and append choosable tablet ids to
     * {@code chosenTablets}. Caller must already hold the per-table intensive READ lock.
     *
     * <p>Re-fetches and re-checks the table under the lock. The pre-filter / snapshot reads
     * happen without a lock, so in the gap between snapshot and per-table lock acquisition
     * the table may have been dropped (DROP TABLE / DROP DATABASE take DB WRITE) or may have
     * transitioned out of NORMAL state. Skipping dropped or non-NORMAL tables here avoids
     * walking a concurrently-mutated structure and avoids enqueueing consistency jobs for
     * tablets already being torn down.
     *
     * @return true iff {@code chosenTablets.size() >= MAX_JOB_NUM}, signalling that the
     *         caller should stop iterating tables. Skips (drop / non-NORMAL) return false.
     */
    private boolean chooseTabletsFromTable(GlobalStateMgr globalStateMgr, Database db, OlapTable table,
                                           List<Long> chosenTablets) {
        if (globalStateMgr.getLocalMetastore().getTable(db.getId(), table.getId()) == null
                || table.getState() != OlapTableState.NORMAL) {
            return false;
        }

        // sort partitions
        Queue<MetaObject> partitionQueue =
                    new PriorityQueue<>(Math.max(table.getAllPhysicalPartitions().size(), 1), COMPARATOR);
        for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
            // check partition's replication num. if 1 replication. skip
            if (table.getPartitionInfo().getReplicationNum(physicalPartition.getParentId()) == (short) 1) {
                LOG.debug("partition[{}]'s replication num is 1. ignore", physicalPartition.getParentId());
                continue;
            }

            // check if this partition has no data
            if (physicalPartition.getVisibleVersion() == Partition.PARTITION_INIT_VERSION) {
                LOG.debug("partition[{}]'s version is {}. ignore", physicalPartition.getId(),
                            Partition.PARTITION_INIT_VERSION);
                continue;
            }
            partitionQueue.add(physicalPartition);
        }

        MetaObject chosenOne;
        while ((chosenOne = partitionQueue.poll()) != null) {
            PhysicalPartition physicalPartition = (PhysicalPartition) chosenOne;

            // sort materializedIndices
            List<MaterializedIndex> visibleIndexes =
                        physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE);
            Queue<MetaObject> indexQueue =
                    new PriorityQueue<>(Math.max(visibleIndexes.size(), 1), COMPARATOR);
            indexQueue.addAll(visibleIndexes);

            while ((chosenOne = indexQueue.poll()) != null) {
                MaterializedIndex index = (MaterializedIndex) chosenOne;

                // sort tablets
                Queue<MetaObject> tabletQueue =
                        new PriorityQueue<>(Math.max(index.getTablets().size(), 1), COMPARATOR);
                long startCheckTime = System.currentTimeMillis();
                long cooldownedTimeMs = startCheckTime - Config.consistency_check_cooldown_time_second * 1000;
                List<Tablet> cooldownedTablets = index.getTablets().stream()
                        .filter(t -> t.getLastCheckTime() < cooldownedTimeMs)
                        .toList();
                tabletQueue.addAll(cooldownedTablets);

                while ((chosenOne = tabletQueue.poll()) != null) {
                    LocalTablet tablet = (LocalTablet) chosenOne;
                    long chosenTabletId = tablet.getId();

                    if (this.jobs.containsKey(chosenTabletId)) {
                        continue;
                    }

                    // check if version has already been checked
                    if (physicalPartition.getVisibleVersion() == tablet.getCheckedVersion()) {
                        if (tablet.isConsistent()) {
                            LOG.debug("tablet[{}]'s version[{}-{}] has been checked. ignore",
                                        chosenTabletId, tablet.getCheckedVersion(),
                                        physicalPartition.getVisibleVersion());
                        }
                    } else {
                        LOG.info("chose tablet[{}-{}-{}-{}-{}] to check consistency", db.getId(),
                                    table.getId(), physicalPartition.getId(), index.getId(), chosenTabletId);

                        chosenTablets.add(chosenTabletId);
                    }
                } // end while tabletQueue
            } // end while indexQueue

            if (chosenTablets.size() >= MAX_JOB_NUM) {
                return true;
            }
        } // end while partitionQueue
        return false;
    }

    public void handleFinishedConsistencyCheck(CheckConsistencyTask task, long checksum) {
        long tabletId = task.getTabletId();
        long backendId = task.getBackendId();

        CheckConsistencyJob job = getJob(tabletId);
        if (job == null) {
            LOG.warn("cannot find {} job[{}]", task.getTaskType().name(), tabletId);
            return;
        }

        job.handleFinishedReplica(backendId, checksum);
    }

    public void replayFinishConsistencyCheck(ConsistencyCheckInfo info, GlobalStateMgr globalStateMgr) {
        Database db = globalStateMgr.getLocalMetastore().getDb(info.getDbId());
        if (db == null) {
            LOG.warn("replay finish consistency check failed, db is null, info: {}", info);
            return;
        }
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getId(), info.getTableId());
        if (table == null) {
            LOG.warn("replay finish consistency check failed, table is null, info: {}", info);
            return;
        }

        try (AutoCloseableLock ignore
                    = new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE)) {
            PhysicalPartition physicalPartition = table.getPhysicalPartition(info.getPhysicalPartitionId());
            if (physicalPartition == null) {
                LOG.warn("replay finish consistency check failed, partition is null, info: {}", info);
                return;
            }
            MaterializedIndex index = physicalPartition.getIndex(info.getIndexId());
            if (index == null) {
                LOG.warn("replay finish consistency check failed, index is null, info: {}", info);
                return;
            }
            LocalTablet tablet = (LocalTablet) index.getTablet(info.getTabletId());
            if (tablet == null) {
                LOG.warn("replay finish consistency check failed, tablet is null, info: {}", info);
                return;
            }

            long lastCheckTime = info.getLastCheckTime();
            db.setLastCheckTime(lastCheckTime);
            table.setLastCheckTime(lastCheckTime);
            if (physicalPartition instanceof MetaObject) {
                ((MetaObject) physicalPartition).setLastCheckTime(lastCheckTime);
            }
            index.setLastCheckTime(lastCheckTime);
            tablet.setLastCheckTime(lastCheckTime);
            tablet.setCheckedVersion(info.getCheckedVersion());

            tablet.setIsConsistent(info.isConsistent());
        }
    }

    // manually adding tablets to check
    public void addTabletsToCheck(List<Long> tabletIds) {
        for (Long tabletId : tabletIds) {
            CheckConsistencyJob job = new CheckConsistencyJob(tabletId);
            addJob(job);
        }
    }

    public void addCreatingTableId(long tableId) {
        creatingTableCounters.compute(tableId, (k, v) -> (v == null) ? 1 : v + 1);
    }

    public void deleteCreatingTableId(long tableId) {
        creatingTableCounters.compute(tableId, (k, v) -> {
            if (v == null || v <= 1) {
                return null;
            } else {
                return v - 1;
            }
        });
    }
}
