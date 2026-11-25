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

package com.starrocks.sql.plan;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCacheStatsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Re-reads, right before the scan ranges are built, the partition versions a lock-free plan's table copies recorded.
 *
 * <p>Scans read the recorded versions ({@link OlapTable#getVisibleVersionForQuery}), one state of each table. Three
 * optimizer decisions read live metadata that may be newer: LIMIT tablet pruning (row counts), the partition-column
 * MIN/MAX rewrite (which partitions hold data) and async MV rewrite (MV freshness). The two rules mark the copies they
 * decided on, and a plan that reads an MV copy counts as marked. So does a plan that scans a published table beside
 * its copies: MV rewrite expands an MV's definition over the live base tables, and scans the live MV where it took no
 * copy of it, and those scans read the live versions when their scan ranges are built. A marked plan compares the live
 * version of every partition it reads off a copy with the record, without a lock; if one moved, it re-reads them all
 * under the tables' READ locks, so its scans are again newer than every decision and about as new as its live scans. A
 * constant answer of the MIN/MAX rewrite leaves no scan of its table, and so no mark to see.
 *
 * <p>The re-read waits for the locks as the planner's own acquisitions do: unbounded, or within the bound of a
 * planner that takes its lock back within one ({@link #boundLockWaits}). A thread that already holds locks re-reads
 * without taking any.
 */
public final class PlanningVersionRecheck {
    private static final String RECHECKED = "PlanningVersionRecheck";
    private static final String REREAD = "PlanningVersionReread";

    // The bound on a re-read's lock waits on this thread, in ms; null for none. See boundLockWaits.
    private static final ThreadLocal<Long> LOCK_WAIT_BOUND_MS = new ThreadLocal<>();

    private PlanningVersionRecheck() {
    }

    /** Closing it puts back the bound that was in place before. */
    public interface LockWaitBound extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * Bounds the lock waits of this thread's re-reads to {@code timeoutMs} until the scope is closed; a negative value
     * keeps the current bound. A re-read that runs out throws {@link PlannerMetaLocker.AcquireTimeoutException}.
     */
    public static LockWaitBound boundLockWaits(long timeoutMs) {
        Long previous = LOCK_WAIT_BOUND_MS.get();
        if (timeoutMs >= 0) {
            LOCK_WAIT_BOUND_MS.set(timeoutMs);
        }
        return () -> {
            if (previous == null) {
                LOCK_WAIT_BOUND_MS.remove();
            } else {
                LOCK_WAIT_BOUND_MS.set(previous);
            }
        };
    }

    /** Re-reads the versions {@code plan} reads if it is marked and one of them moved. */
    public static void recheck(OptExpression plan) {
        List<PhysicalScanOperator> scans = new ArrayList<>();
        boolean scansPublishedTable = collectScans(plan, scans);
        if (scans.isEmpty()
                || (!scansPublishedTable && scans.stream().noneMatch(scan -> isMarked((OlapTable) scan.getTable())))) {
            return;
        }
        Tracers.count(Tracers.Module.BASE, RECHECKED, 1);
        // Every table the plan reads, not only the marked ones: an MV refreshed before its copy was taken does not
        // move again, while the base tables read beside it do.
        if (!anyMoved(scans)) {
            return;
        }
        Tracers.count(Tracers.Module.BASE, REREAD, 1);
        reread(readsOf(scans));
    }

    private static boolean isMarked(OlapTable table) {
        return table.isPlanningVersionsMarkedForRecheck() || table instanceof MaterializedView;
    }

    /**
     * Collects the scans in {@code expr} that read a planning copy's recorded versions. Returns whether {@code expr}
     * also scans a table that records none, which is read at its live versions: a base table of an MV's definition, or
     * an MV that MV rewrite could not lock for the copy or does not copy at all (a DML's query, or
     * skip_whole_phase_lock_mv_limit below 0).
     */
    private static boolean collectScans(OptExpression expr, List<PhysicalScanOperator> scans) {
        boolean scansPublishedTable = false;
        Operator op = expr.getOp();
        if (op instanceof PhysicalOlapScanOperator || op instanceof PhysicalMetaScanOperator
                || op instanceof PhysicalCacheStatsScanOperator) {
            Table table = ((PhysicalScanOperator) op).getTable();
            if (table instanceof OlapTable olapTable) {
                if (olapTable.hasPlanningVisibleVersions()) {
                    scans.add((PhysicalScanOperator) op);
                } else {
                    scansPublishedTable = true;
                }
            }
        }
        for (OptExpression input : expr.getInputs()) {
            scansPublishedTable |= collectScans(input, scans);
        }
        return scansPublishedTable;
    }

    /** Whether a partition {@code scans} read moved since its copy was taken. Collects nothing: most plans end here. */
    private static boolean anyMoved(List<PhysicalScanOperator> scans) {
        for (PhysicalScanOperator scan : scans) {
            OlapTable table = (OlapTable) scan.getTable();
            if (anyReadPartition(scan, table::isPlanningVisibleVersionMoved)) {
                return true;
            }
        }
        return false;
    }

    /** The physical partitions {@code scans} read, by table copy. */
    private static Map<OlapTable, Set<PhysicalPartition>> readsOf(List<PhysicalScanOperator> scans) {
        Map<OlapTable, Set<PhysicalPartition>> reads = new IdentityHashMap<>();
        for (PhysicalScanOperator scan : scans) {
            Set<PhysicalPartition> partitions = reads.computeIfAbsent((OlapTable) scan.getTable(),
                    t -> Collections.newSetFromMap(new IdentityHashMap<>()));
            anyReadPartition(scan, partition -> {
                partitions.add(partition);
                return false;
            });
        }
        return reads;
    }

    /** Whether {@code test} holds for a physical partition {@code scan} reads; stops at the first that does. */
    private static boolean anyReadPartition(PhysicalScanOperator scan, Predicate<PhysicalPartition> test) {
        OlapTable table = (OlapTable) scan.getTable();
        if (scan instanceof PhysicalOlapScanOperator olapScan) {
            if (olapScan.getSelectedPartitionId() == null) {
                return false;
            }
            for (Long partitionId : olapScan.getSelectedPartitionId()) {
                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    if (test.test(physicalPartition)) {
                        return true;
                    }
                }
            }
            return false;
        }
        // Meta and cached-stats scans pick their partitions later; take all of them.
        for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
            if (test.test(physicalPartition)) {
                return true;
            }
        }
        return false;
    }

    private static void reread(Map<OlapTable, Set<PhysicalPartition>> reads) {
        int partitionCount = 0;
        for (Set<PhysicalPartition> partitions : reads.values()) {
            partitionCount += partitions.size();
        }
        // A single version is read atomically on its own. A thread that already holds locks, as a plan built under the
        // statement's locks does, takes no more: waiting for some while holding others can deadlock. The tables it
        // holds cannot move, and the rest, such as a [_SYNC_MV_] relation's base table, are read as main reads them.
        if (partitionCount <= 1 || LockHoldDepth.isUnderLock()) {
            reads.forEach((table, partitions) -> partitions.forEach(table::rereadPlanningVisibleVersion));
            return;
        }

        // The locks the copies were taken under, databases and tables in id order. A dropped table keeps its record.
        TreeMap<Long, Set<Long>> tableIdsByDb = new TreeMap<>();
        List<OlapTable> rereadTables = new ArrayList<>();
        for (OlapTable table : reads.keySet()) {
            Pair<Long, Long> lockTarget = lockTargetOf(table);
            if (lockTarget == null) {
                continue;
            }
            tableIdsByDb.computeIfAbsent(lockTarget.first, k -> new TreeSet<>()).add(lockTarget.second);
            rereadTables.add(table);
        }

        Long waitBoundMs = LOCK_WAIT_BOUND_MS.get();
        Locker locker = new Locker();
        List<Map.Entry<Long, List<Long>>> locked = new ArrayList<>();
        try {
            for (Map.Entry<Long, Set<Long>> entry : tableIdsByDb.entrySet()) {
                List<Long> tableIds = new ArrayList<>(entry.getValue());
                if (waitBoundMs == null) {
                    locker.lockTablesWithIntensiveDbLock(entry.getKey(), tableIds, LockType.READ);
                } else if (!locker.tryLockTablesWithIntensiveDbLock(entry.getKey(), tableIds, LockType.READ,
                        waitBoundMs, TimeUnit.MILLISECONDS)) {
                    throw new PlannerMetaLocker.AcquireTimeoutException(waitBoundMs);
                }
                locked.add(Map.entry(entry.getKey(), tableIds));
            }
            for (OlapTable table : rereadTables) {
                reads.get(table).forEach(table::rereadPlanningVisibleVersion);
            }
        } finally {
            for (int i = locked.size() - 1; i >= 0; i--) {
                locker.unLockTablesWithIntensiveDbLock(locked.get(i).getKey(), locked.get(i).getValue(), LockType.READ);
            }
        }
    }

    /** The lock target of {@code table}'s partitions (a synchronous MV's is its base table), or null once dropped. */
    private static Pair<Long, Long> lockTargetOf(OlapTable table) {
        Pair<Long, Long> target;
        if (table instanceof MaterializedView mv) {
            target = mv.getPartitionLockTarget();
        } else {
            Long dbId = table.mayGetDatabaseId().orElse(null);
            if (dbId == null) {
                return null;
            }
            target = Pair.create(dbId, table.getId());
        }
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(target.first, target.second) == null
                ? null : target;
    }
}
