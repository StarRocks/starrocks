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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/TabletStatMgr.java

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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.alter.reshard.TabletReshardUtils;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTablet;
import com.starrocks.proto.TabletStatRequest;
import com.starrocks.proto.TabletStatRequest.TabletInfo;
import com.starrocks.proto.TabletStatResponse;
import com.starrocks.proto.TabletStatResponse.TabletStat;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TTabletStat;
import com.starrocks.thrift.TTabletStatResult;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/*
 * TabletStatMgr is for collecting tablet(replica) statistics from backends.
 * Each FE will collect by itself.
 */
public class TabletStatMgr extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletStatMgr.class);

    private LocalDateTime lastWorkTimestamp = LocalDateTime.MIN;

    public TabletStatMgr() {
        super("tablet-stat-mgr", Config.tablet_stat_update_interval_second * 1000L);
    }

    // Note this stamp says only that a cycle ENDED, not that it collected anything: it is advanced
    // unconditionally below, including for a cycle whose stat RPCs all failed. Its one remaining
    // consumer, StatisticsCalcUtils, uses it for a cardinality estimate, where being wrong costs a
    // worse plan. The exact-COUNT(*) fold used to consult it too, through workTimeIsMustAfter(), and
    // served stale rows as an exact answer; it now asks the tablets to prove which version their
    // counts cover (see Tablet#getRowCountAtVersion), and that method is gone with its last caller.
    public LocalDateTime getLastWorkTimestamp() {
        return lastWorkTimestamp;
    }

    /**
     * The table-level range-colocate snapshot the merge signal is filtered against.
     *
     * <p>{@code ranges == null} means the table IS range-colocate but cannot be classified right now —
     * an unstable group, a range record not yet replayed, or a failed lookup. That is deliberately
     * distinct from a {@code null} context, which means the table is not range-colocate at all: the
     * first must withhold the merge signal entirely, while the second keeps the ordinary
     * boundary-blind behavior. Collapsing the two is what would let an unstable group keep proposing
     * merges the factory rejects on every scan.
     */
    private record RangeColocateSignalContext(@Nullable List<ColocateRange> ranges, int colocateColumnCount) {
        private static final RangeColocateSignalContext UNUSABLE = new RangeColocateSignalContext(null, 0);

        /**
         * Binds the ranges to one index's sort key, or {@code null} when that cannot be done. Every
         * failure mode here is a metadata race or a schema this signal simply cannot classify (an
         * index meta that vanished, a sort key shorter than the colocate prefix), and the caller
         * treats a null classifier on a range-colocate table as "withhold the merge signal".
         */
        @Nullable
        ColocateRangeUtils.Classifier classifierFor(OlapTable olapTable, long indexMetaId) {
            if (ranges == null) {
                return null;
            }
            try {
                return ColocateRangeUtils.Classifier.of(ranges,
                        MetaUtils.getRangeDistributionColumns(olapTable, indexMetaId), colocateColumnCount);
            } catch (Exception e) {
                LOG.warn("cannot classify index meta {} of table {} against its colocate ranges; "
                        + "withholding its merge signal", indexMetaId, olapTable.getId(), e);
                return null;
            }
        }
    }

    /**
     * The table's range-colocate ranges plus colocate column count; {@code null} when the table is not
     * range-colocate, or {@link RangeColocateSignalContext#UNUSABLE} when it is but cannot be
     * classified (unstable group, ranges not yet replayed, or a failed lookup).
     *
     * <p>Fail-soft on purpose. The scan loop that calls this has no per-table {@code catch} — only a
     * {@code finally} that releases the lock — so an unchecked failure here would skip
     * {@code setRowCount} for every table ordered after this one, on every cycle. The lookups really
     * can fail: {@code getRangeColocateGroupId} and {@code getGroupSchema} take the colocate index
     * lock separately, so a group dropped between them yields a null schema.
     *
     * <p>An UNSTABLE group is withheld rather than signalled: {@code MergeTabletJobFactory} refuses to
     * plan a merge while any peer is unstable, so signalling one would only produce a rejected job and
     * a stack trace in the log on every scan for as long as alignment takes.
     */
    @Nullable
    private static RangeColocateSignalContext resolveRangeColocateSignalContext(long tableId) {
        try {
            ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
            ColocateTableIndex.GroupId groupId = colocateTableIndex.getRangeColocateGroupId(tableId);
            if (groupId == null) {
                return null;
            }
            List<ColocateRange> ranges = colocateTableIndex.getColocateRanges(groupId.grpId);
            ColocateGroupSchema schema = colocateTableIndex.getGroupSchema(groupId);
            // Empty ranges means the group is registered but its range record has not been replayed
            // yet, so there is no topology to filter against.
            if (ranges.isEmpty() || schema == null
                    || colocateTableIndex.isAnyGroupWithSameColocateGroupIdUnstable(groupId.grpId)) {
                return RangeColocateSignalContext.UNUSABLE;
            }
            return new RangeColocateSignalContext(ranges, schema.getColocateColumnCount());
        } catch (Exception e) {
            // Withhold rather than guess: a suppressed merge signal costs one scan, a boundary-blind
            // one costs a rejected job every scan.
            LOG.warn("failed to resolve the range-colocate context of table {}; "
                    + "withholding its merge signal this pass", tableId, e);
            return RangeColocateSignalContext.UNUSABLE;
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        // update interval
        if (getInterval() != Config.tablet_stat_update_interval_second * 1000) {
            setInterval(Config.tablet_stat_update_interval_second * 1000);
        }

        // for testing statistic behavior
        if (!Config.enable_sync_tablet_stats) {
            return;
        }

        acquireBackgroundComputeResource();
        updateLocalTabletStat();
        updateLakeTabletStat();

        // after update replica in all backends, update index row num
        long start = System.currentTimeMillis();
        for (Long dbId : GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds()) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                continue;
            }
            Locker locker = new Locker();
            for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                if (!table.isNativeTableOrMaterializedView()) {
                    continue;
                }

                long totalRowCount = 0L;
                long maxTabletSize = 0L;
                long minAdjacentTabletPairSize = Long.MAX_VALUE;
                long maxAdaptiveSplitTabletSize = 0L;
                Map<Pair<Long, Long>, Long> indexRowCountMap = Maps.newHashMap();
                // NOTE: calculate the row first with read lock, then update the stats with write lock
                OlapTable olapTable = (OlapTable) table;
                // Reshard is leader-only (TabletStatMgr runs on all FEs), and only for cloud-native
                // range-distribution tables. This gates the parallelism-floor lookup (a StarMgr RPC),
                // the adjacency walk, and the reshard trigger — none of which should run on followers.
                boolean reshardEligible = GlobalStateMgr.getCurrentState().isLeader()
                        && olapTable.isCloudNativeTableOrMaterializedView()
                        && olapTable.isRangeDistribution();
                // One resolution per eligible table, feeding both the merge floor and the early bound.
                int computeNodeCount = reshardEligible
                        ? TabletReshardUtils.safeComputeNodeCountForTable(table.getId()) : 0;
                // One sample of the split cap as well as of the node count: both the merge floor and
                // the adaptive bound derive from it, and a change landing between two reads would put
                // the floor above the bound -- the overlap that lets this scan emit a merge signal and
                // an adaptive-split signal for the same index.
                int maxSplitCount = Config.tablet_reshard_max_split_count;
                int parallelismFloor = computeNodeCount == 0 ? 0
                        : TabletReshardUtils.parallelismFloor(computeNodeCount, maxSplitCount);
                int adaptiveBound = TabletReshardUtils.adaptiveSplitBound(computeNodeCount, maxSplitCount);
                // A merge that joined two tablets from different ColocateRanges would produce a tablet
                // no colocate scan can dispatch, so MergeTabletJobFactory refuses that pair. Signalling
                // one here would therefore only schedule a merge plan that comes out empty -- on every
                // scan, since the steady state of a range-colocate table is one tablet per range and
                // every adjacent pair then crosses a boundary. Resolved once per table, and only inside
                // the eligibility gate: a follower, a shared-nothing table and a non-range table pay
                // nothing for it.
                RangeColocateSignalContext colocateContext = reshardEligible
                        ? resolveRangeColocateSignalContext(table.getId()) : null;
                // One Classifier per index meta, not per (physical partition x index): its inputs are
                // the table-level range snapshot and the index meta's sort key, both fixed for this
                // walk, so a table with hundreds of partitions would otherwise rebuild an identical
                // expansion for each one. A null value is cached too, so a table whose sort key cannot
                // be resolved is not retried once per partition.
                Map<Long, ColocateRangeUtils.Classifier> classifierByIndexMeta = new HashMap<>();
                // A single unclassifiable index withholds the WHOLE table's merge signal, not just its
                // own: MergeTabletJobFactory walks every visible index, so it would hit that same index
                // and reject the entire job. Emitting a signal another index earned would just recreate
                // and re-reject the candidate on every scan.
                boolean tableMergeSignalUsable = true;
                locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                            long version = physicalPartition.getVisibleVersion();
                            long visibleVersionTime = physicalPartition.getVisibleVersionTime();
                            for (MaterializedIndex index : physicalPartition.getLatestMaterializedIndices(
                                    IndexExtState.VISIBLE)) {
                                long indexRowCount = 0L;
                                List<Tablet> tablets = index.getTablets();
                                // Only an index above the parallelism floor contributes the merge signal
                                // (minAdjacentTabletPairSize); otherwise auto-merge could shrink it below the
                                // tablet count pre-split established for parallelism (and would churn empty
                                // merge jobs every cycle). Split detection (maxTabletSize) is never gated.
                                // MergeTabletJobFactory's per-index merge budget re-enforces the same floor
                                // inside an admitted job, so the floor holds even for manual size-based merges.
                                boolean eligibleForMerge = tablets.size() > parallelismFloor;
                                // Under-provisioned means what the planner means by it: fewer tablets than
                                // the bound, which is also the headroom the planner spends. Testing it here
                                // is what keeps the adaptive rule and auto-merge disjoint -- the bound sits
                                // at or below the merge floor, so an index is one rule's business or the
                                // other's, never both -- and it is what stops an index parked at its bound
                                // from signalling on every scan for a plan that can only come out empty.
                                // It also keeps the index walk below off every table that cannot reshard at
                                // all: a follower FE, or a shared-nothing table whose tablets take a lock
                                // per size read.
                                boolean underProvisioned = adaptiveBound > 0 && tablets.size() < adaptiveBound;
                                // The index's own target: narrowed while it has less parallelism
                                // than the warehouse can drive, the steady-state target once it does.
                                long indexTarget = underProvisioned
                                        ? TabletReshardUtils.adaptiveTargetSize(index.getDataSize(true),
                                                Config.tablet_reshard_target_size, adaptiveBound)
                                        : 0;
                                // Expanded once per index, since the per-tablet classification below
                                // binary-searches these. Each visible index carries its own sort-key
                                // arity -- a rollup or MV can have a strict prefix of the base index's,
                                // and the base arity would misclassify its tablets -- so resolve it from
                                // the index's own (reshard-stable) meta id. An index that cannot
                                // contribute a pair at all is not classified.
                                // Resolved for EVERY range-colocate index, including one at or below
                                // the parallelism floor. MergeTabletJobFactory walks every visible
                                // index and builds the same classifier before it consults the merge
                                // budget, so an index this producer skipped would still fail there and
                                // reject the whole job -- the signal has to notice the failures its
                                // consumer will hit, not just the ones on indexes it can merge. Only
                                // the per-tablet classification below is skipped when ineligible;
                                // that is where the per-tablet cost is.
                                ColocateRangeUtils.Classifier classifier = null;
                                if (colocateContext != null) {
                                    long indexMetaId = index.getMetaId();
                                    if (classifierByIndexMeta.containsKey(indexMetaId)) {
                                        classifier = classifierByIndexMeta.get(indexMetaId);
                                    } else {
                                        classifier = colocateContext.classifierFor(olapTable, indexMetaId);
                                        classifierByIndexMeta.put(indexMetaId, classifier);
                                    }
                                }
                                // Accumulate this index's merge signal separately so a classification
                                // failure part-way through can discard it without losing the row counts
                                // collected alongside it -- and so a range-colocate index we cannot
                                // classify contributes NO signal rather than falling back to the
                                // boundary-blind pairing, which is what the factory would then reject.
                                long indexMinAdjacentPairSize = Long.MAX_VALUE;
                                // A null classifier on a range-colocate table now means exactly one
                                // thing -- construction failed -- because it is attempted for every
                                // index regardless of eligibility.
                                boolean indexMergeSignalUsable =
                                        colocateContext == null || classifier != null;
                                int prevColocateRangeIndex = -1;
                                long prevFreshTabletSize = -1L;
                                // NOTE: can take a rather long time to iterate lots of tablets
                                for (Tablet tablet : tablets) {
                                    indexRowCount += tablet.getRowCount(version);
                                    long dataSize = tablet.getDataSize(true);
                                    maxTabletSize = Math.max(maxTabletSize, dataSize);
                                    if (underProvisioned
                                            && TabletReshardUtils.adaptiveSplitCount(dataSize, indexTarget) > 1) {
                                        maxAdaptiveSplitTabletSize =
                                                Math.max(maxAdaptiveSplitTabletSize, dataSize);
                                    }
                                    if (!(tablet instanceof LakeTablet)
                                            || ((LakeTablet) tablet).getDataSizeUpdateTime() < visibleVersionTime) {
                                        prevFreshTabletSize = -1L;
                                        continue;
                                    }
                                    if (classifier != null && eligibleForMerge) {
                                        // Break the adjacency chain at every colocate boundary. A -1
                                        // (a tablet already spanning a boundary, or a prefix no range
                                        // covers) breaks it too: such a tablet is already misaligned,
                                        // and merging it can only make it worse.
                                        int colocateRangeIndex;
                                        try {
                                            colocateRangeIndex = classifier.indexOf(tablet);
                                        } catch (Exception e) {
                                            // A malformed or racing TabletRange must not escape into
                                            // the caller's catch-less loop and cost every later table
                                            // its row-count update. Give up on this index's signal and
                                            // keep collecting row counts.
                                            LOG.warn("cannot classify tablet {} of table {} against its "
                                                    + "colocate ranges; withholding the merge signal for "
                                                    + "index {}", tablet.getId(), table.getId(),
                                                    index.getId(), e);
                                            indexMergeSignalUsable = false;
                                            classifier = null;
                                            prevFreshTabletSize = dataSize;
                                            continue;
                                        }
                                        if (colocateRangeIndex < 0
                                                || colocateRangeIndex != prevColocateRangeIndex) {
                                            prevFreshTabletSize = -1L;
                                        }
                                        prevColocateRangeIndex = colocateRangeIndex;
                                    }
                                    if (indexMergeSignalUsable && prevFreshTabletSize >= 0 && eligibleForMerge) {
                                        indexMinAdjacentPairSize = Math.min(indexMinAdjacentPairSize,
                                                prevFreshTabletSize + dataSize);
                                    }
                                    prevFreshTabletSize = dataSize;
                                } // end for tablets
                                if (indexMergeSignalUsable) {
                                    minAdjacentTabletPairSize =
                                            Math.min(minAdjacentTabletPairSize, indexMinAdjacentPairSize);
                                } else {
                                    tableMergeSignalUsable = false;
                                }
                                indexRowCountMap.put(Pair.create(physicalPartition.getId(), index.getId()),
                                        indexRowCount);
                                if (!olapTable.isTempPartition(partition.getId())) {
                                    totalRowCount += indexRowCount;
                                }
                            } // end for indices
                        } // end for physical partitions
                    } // end for partitions
                    if (!tableMergeSignalUsable) {
                        minAdjacentTabletPairSize = Long.MAX_VALUE;
                    }
                    LOG.debug("finished to set row num for table: {} in database: {}",
                            table.getName(), db.getFullName());
                } finally {
                    locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
                }

                // update
                locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                            for (MaterializedIndex index :
                                    physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                                Long indexRowCount =
                                        indexRowCountMap.get(Pair.create(physicalPartition.getId(), index.getId()));
                                if (indexRowCount != null) {
                                    index.setRowCount(indexRowCount);
                                }
                            }
                        }
                    }
                    adjustStatUpdateRows(table.getId(), totalRowCount);
                } finally {
                    locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
                }

                // Emit a reshard candidate with the signals computed above; addReshardCandidate drops
                // non-actionable signals and the TabletReshardJobMgr drain owns job creation. This
                // periodic scan is the fallback for the publish-driven path, so unlike publish it
                // carries the merge signal too.
                if (reshardEligible) {
                    GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().addReshardCandidate(
                            db.getId(), olapTable.getId(), maxTabletSize, minAdjacentTabletPairSize,
                            maxAdaptiveSplitTabletSize, adaptiveBound);
                }
            }
        }
        LOG.info("finished to update index row num of all databases. cost: {} ms",
                (System.currentTimeMillis() - start));
        lastWorkTimestamp = LocalDateTime.now();
    }

    private void updateLocalTabletStat() {
        if (!RunMode.isSharedNothingMode()) {
            return;
        }
        ImmutableMap<Long, Backend> backends =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();

        long start = System.currentTimeMillis();
        for (Backend backend : backends.values()) {
            try {
                TTabletStatResult result = ThriftRPCRequestExecutor.callNoRetry(
                        ThriftConnectionPool.backendPool,
                        new TNetworkAddress(backend.getHost(), backend.getBePort()),
                        BackendService.Client::get_tablet_stat);
                LOG.debug("get tablet stat from backend: {}, num: {}", backend.getId(), result.getTablets_statsSize());
                updateLocalTabletStat(backend.getId(), result);

            } catch (Exception e) {
                LOG.warn("task exec error. backend[{}]", backend.getId(), e);
            }
        }
        LOG.info("finished to get local tablet stat of all backends. cost: {} ms",
                (System.currentTimeMillis() - start));
    }

    private void updateLocalTabletStat(Long beId, TTabletStatResult result) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Map.Entry<Long, TTabletStat> entry : result.getTablets_stats().entrySet()) {
            if (invertedIndex.getTabletMeta(entry.getKey()) == null) {
                // the replica is obsolete, ignore it.
                continue;
            }

            // Currently, only local table will update replica.
            Replica replica = invertedIndex.getReplica(entry.getKey(), beId);
            if (replica == null) {
                // replica may be deleted from catalog, ignore it.
                continue;
            }
            // TODO(cmy) no db lock protected. I think it is ok even we get wrong row num
            // The BE serves get_tablet_stat from a snapshot it rebuilds only every
            // tablet_stat_cache_update_interval_second (300s by default), so a successful RPC does
            // NOT mean the numbers are current. The reported version says which tablet version they
            // describe; a BE too old to report it leaves 0, which keeps exact-count callers on the
            // safe (meta scan) path.
            replica.updateStat(
                    entry.getValue().getData_size(),
                    entry.getValue().getRow_num(),
                    entry.getValue().getVersion_count(),
                    entry.getValue().isSetVersion() ? entry.getValue().getVersion() : 0L
            );
        }
    }

    private void updateLakeTabletStat() {
        if (!RunMode.isSharedDataMode()) {
            return;
        }

        List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                continue;
            }

            List<Table> tables = GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId());
            for (Table table : tables) {
                if (table.isCloudNativeTableOrMaterializedView()) {
                    updateLakeTableTabletStat(db, (OlapTable) table);
                }
            }
        }
    }

    private void adjustStatUpdateRows(long tableId, long totalRowCount) {
        BasicStatsMeta meta = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getTableBasicStatsMeta(tableId);
        if (meta != null) {
            meta.setTotalRows(totalRowCount);
            meta.resetDeltaRows();
            meta.updateTabletStatsReportTime();
        }
    }

    @NotNull
    private Collection<PhysicalPartition> getPartitions(@NotNull Database db, @NotNull OlapTable table) {
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        try {
            return table.getPhysicalPartitions();
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        }
    }

    @NotNull
    private PartitionSnapshot createPartitionSnapshot(@NotNull Database db,
                                                      @NotNull OlapTable table,
                                                      @NotNull PhysicalPartition partition) {
        String dbName = db.getFullName();
        String tableName = table.getName();
        long partitionId = partition.getId();
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        try {
            long visibleVersion = partition.getVisibleVersion();
            long visibleVersionTime = partition.getVisibleVersionTime();
            List<Tablet> tablets = new ArrayList<>(partition.getLatestBaseIndex().getTablets());
            return new PartitionSnapshot(dbName, tableName, partitionId, visibleVersion, visibleVersionTime, tablets);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        }
    }

    @Nullable
    private CollectTabletStatJob createCollectTabletStatJob(@NotNull Database db, @NotNull OlapTable table,
                                                            @NotNull PhysicalPartition partition) {
        PartitionSnapshot snapshot = createPartitionSnapshot(db, table, partition);
        long visibleVersionTime = snapshot.visibleVersionTime;
        snapshot.tablets.removeIf(t -> ((LakeTablet) t).getDataSizeUpdateTime() >= visibleVersionTime);
        if (snapshot.tablets.isEmpty()) {
            LOG.debug("Skipped tablet stat collection of partition {}", snapshot.debugName());
            return null;
        }
        return new CollectTabletStatJob(snapshot, computeResource);
    }

    private void updateLakeTableTabletStat(@NotNull Database db, @NotNull OlapTable table) {
        Collection<PhysicalPartition> partitions = getPartitions(db, table);
        for (PhysicalPartition partition : partitions) {
            CollectTabletStatJob job = createCollectTabletStatJob(db, table, partition);
            if (job == null) {
                continue;
            }
            job.execute();
        }
    }

    private static class PartitionSnapshot {
        private final String dbName;
        private final String tableName;
        private final long partitionId;
        private final long visibleVersion;
        private final long visibleVersionTime;
        private final List<Tablet> tablets;

        PartitionSnapshot(String dbName, String tableName, long partitionId, long visibleVersion,
                          long visibleVersionTime, List<Tablet> tablets) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.partitionId = partitionId;
            this.visibleVersion = visibleVersion;
            this.visibleVersionTime = visibleVersionTime;
            this.tablets = Objects.requireNonNull(tablets);
        }

        private String debugName() {
            return String.format("%s.%s.%d version %d", dbName, tableName, partitionId, visibleVersion);
        }
    }

    private static class CollectTabletStatJob {
        private final String dbName;
        private final String tableName;
        private final long partitionId;
        private final long version;
        private final Map<Long, Tablet> tablets;
        private long collectStatTime = 0;
        private List<Future<TabletStatResponse>> responseList;
        private final ComputeResource computeResource;

        CollectTabletStatJob(PartitionSnapshot snapshot, ComputeResource computeResource) {
            this.dbName = Objects.requireNonNull(snapshot.dbName, "dbName is null");
            this.tableName = Objects.requireNonNull(snapshot.tableName, "tableName is null");
            this.partitionId = snapshot.partitionId;
            this.version = snapshot.visibleVersion;
            this.tablets = new HashMap<>();
            for (Tablet tablet : snapshot.tablets) {
                this.tablets.put(tablet.getId(), tablet);
            }
            this.computeResource = computeResource;
        }

        void execute() {
            sendTasks();
            waitResponse();
        }

        private String debugName() {
            return String.format("%s.%s.%d", dbName, tableName, partitionId);
        }

        private void sendTasks() {
            final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            Map<ComputeNode, List<TabletInfo>> beToTabletInfos = new HashMap<>();
            for (Tablet tablet : tablets.values()) {
                ComputeNode node;
                try {
                    node = warehouseManager.getComputeNodeAssignedToTablet(computeResource, tablet.getId());
                    if (node == null) {
                        LOG.warn("Skip sending tablet stat task for partition {} because no alive node", debugName());
                        continue;
                    }
                } catch (ErrorReportException e) {
                    LOG.warn("Skip sending tablet stat task for partition {} because exception: {}",
                            debugName(), e.getMessage());
                    continue;
                }
                TabletInfo tabletInfo = new TabletInfo();
                tabletInfo.tabletId = tablet.getId();
                tabletInfo.version = version;
                beToTabletInfos.computeIfAbsent(node, k -> Lists.newArrayList()).add(tabletInfo);
            }

            collectStatTime = System.currentTimeMillis();
            responseList = Lists.newArrayListWithCapacity(beToTabletInfos.size());
            for (Map.Entry<ComputeNode, List<TabletInfo>> entry : beToTabletInfos.entrySet()) {
                ComputeNode node = entry.getKey();
                TabletStatRequest request = new TabletStatRequest();
                request.tabletInfos = entry.getValue();
                request.timeoutMs = LakeService.TIMEOUT_GET_TABLET_STATS;
                try {
                    LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
                    Future<TabletStatResponse> responseFuture = lakeService.getTabletStats(request);
                    responseList.add(responseFuture);
                    LOG.debug(
                            "Sent tablet stat collection task to node {} for partition {} of version {}. tablet " +
                                    "count={}",
                            node.getHost(), debugName(), version, entry.getValue().size());
                } catch (Throwable e) {
                    LOG.warn("Fail to send tablet stat task to host {} for partition {}: {}", node.getHost(),
                            debugName(),
                            e.getMessage());
                }
            }
        }

        private void waitResponse() {
            // responseList may be null if there aren't any alive node.
            if (responseList == null) {
                return;
            }
            for (Future<TabletStatResponse> responseFuture : responseList) {
                try {
                    TabletStatResponse response = responseFuture.get();
                    if (response != null && response.tabletStats != null) {
                        for (TabletStat stat : response.tabletStats) {
                            LakeTablet tablet = (LakeTablet) tablets.get(stat.tabletId);
                            tablet.setDataSize(stat.dataSize);
                            // The CN computes these strictly from the version we asked for
                            // (LakeServiceImpl::get_tablet_stats -> get_tablet_metadata(id, version)),
                            // so the requested version is exactly what the numbers describe.
                            tablet.setRowCount(stat.numRows, version);
                            tablet.setDataSizeUpdateTime(collectStatTime);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    LOG.warn("Fail to collect tablet stat for partition {}: {}", debugName(), e.getMessage());
                }
            }
        }
    }
}
