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

package com.starrocks.lake.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class CompactionMgr implements MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(CompactionMgr.class);

    @SerializedName(value = "partitionStatisticsHashMap")
    private ConcurrentHashMap<PartitionIdentifier, PartitionStatistics> partitionStatisticsHashMap =
            new ConcurrentHashMap<>();

    private Selector selector;
    private Sorter sorter;
    private CompactionScheduler compactionScheduler;

    /**
     *
     * In order to ensure that the input rowsets of compaction still exists when doing publishing version, it is
     * necessary to ensure that the compaction task of the same partition is executed serially, that is, the next
     * compaction task can be executed only after the status of the previous compaction task changes to visible or
     * canceled.
     * So when FE restarted, we should make sure all the active compaction transactions before restarting were tracked,
     * and exclude them from choosing as candidates for compaction.
     *
     * We use `activeCompactionTransactionMap` to track all lake compaction txns that are not published on FE restart.
     * The key of the map is the transaction id related to the compaction task, and the value is table id of the
     * compaction task. It's possible that multiple keys have the same value, because there might be multiple compaction
     * jobs on different partitions with the same table id.
     *
     * Note that, this will prevent all partitions whose tableId is maintained in the map from being compacted
     */
    private final ConcurrentHashMap<Long, Long> remainedActiveCompactionTxnWhenStart = new ConcurrentHashMap<>();

    @VisibleForTesting
    protected ConcurrentHashMap<Long, Long> getRemainedActiveCompactionTxnWhenStart() {
        return remainedActiveCompactionTxnWhenStart;
    }

    public CompactionMgr() {
        try {
            init();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void init() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
            IllegalAccessException {
        String packageName = CompactionMgr.class.getPackage().getName();
        Class<?> selectorClazz = Class.forName(packageName + "." + Config.lake_compaction_selector);
        selector = (Selector) selectorClazz.getConstructor().newInstance();

        Class<?> sorterClazz = Class.forName(packageName + "." + Config.lake_compaction_sorter);
        sorter = (Sorter) sorterClazz.getConstructor().newInstance();
    }

    public void setCompactionScheduler(CompactionScheduler compactionScheduler) {
        this.compactionScheduler = compactionScheduler;
    }

    public void start() {
        if (compactionScheduler == null) {
            compactionScheduler = new CompactionScheduler(this, GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr(), GlobalStateMgr.getCurrentState(),
                    Config.lake_compaction_disable_tables);
            GlobalStateMgr.getCurrentState().getConfigRefreshDaemon().registerListener(() -> {
                compactionScheduler.disableTables(Config.lake_compaction_disable_tables);
            });
            compactionScheduler.start();
        }
    }

    /**
     * iterate all transactions and find those with LAKE_COMPACTION labels and are not finished before FE restart
     * or Leader FE changed.
     **/
    public void buildActiveCompactionTransactionMap() {
        Map<Long, Long> activeTxnStates =
                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getLakeCompactionActiveTxnStats();
        for (Map.Entry<Long, Long> txnState : activeTxnStates.entrySet()) {
            // for lake compaction txn, there can only be one table id for each txn state
            remainedActiveCompactionTxnWhenStart.put(txnState.getKey(), txnState.getValue());
            LOG.info("Found lake compaction transaction not finished on table {}, txn_id: {}", txnState.getValue(),
                    txnState.getKey());
        }
    }

    protected void removeFromStartupActiveCompactionTransactionMap(long txnId) {
        if (remainedActiveCompactionTxnWhenStart.isEmpty()) {
            return;
        }
        boolean ret = remainedActiveCompactionTxnWhenStart.keySet().removeIf(key -> key == txnId);
        if (ret) {
            LOG.info("Removed transaction {} from startup active compaction transaction map", txnId);
        }
    }

    public void handleLoadingFinished(PartitionIdentifier partition, long version, long versionTime,
                                      Quantiles compactionScore) {
        PartitionVersion currentVersion = new PartitionVersion(version, versionTime);
        PartitionStatistics statistics = partitionStatisticsHashMap.compute(partition, (k, v) -> {
            if (v == null) {
                v = new PartitionStatistics(partition);
            }
            v.setCurrentVersion(currentVersion);
            v.setCompactionScore(compactionScore);
            return v;
        });
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished loading: {}", statistics);
        }
    }

    public void handleCompactionFinished(PartitionIdentifier partition, long version, long versionTime,
                                         Quantiles compactionScore, long txnId) {
        removeFromStartupActiveCompactionTransactionMap(txnId);
        PartitionVersion compactionVersion = new PartitionVersion(version, versionTime);
        PartitionStatistics statistics = partitionStatisticsHashMap.compute(partition, (k, v) -> {
            if (v == null) {
                v = new PartitionStatistics(partition);
            }
            v.setCurrentVersion(compactionVersion);
            v.setCompactionVersion(compactionVersion);
            v.setCompactionScoreAndAdjustPunishFactor(compactionScore);
            return v;
        });
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished compaction: {}", statistics);
        }
    }

    @NotNull
    List<PartitionStatisticsSnapshot> choosePartitionsToCompact(@NotNull Set<PartitionIdentifier> excludes,
                                                                @NotNull Set<Long> excludeTables) {
        Set<Long> copiedExcludeTables = new HashSet<>(excludeTables);
        copiedExcludeTables.addAll(remainedActiveCompactionTxnWhenStart.values());
        return choosePartitionsToCompact(copiedExcludeTables)
                .stream()
                .filter(p -> !excludes.contains(p.getPartition()))
                .collect(Collectors.toList());
    }

    @NotNull
    List<PartitionStatisticsSnapshot> choosePartitionsToCompact(Set<Long> excludeTables) {
        List<PartitionStatisticsSnapshot> selection = sorter.sort(
                selector.select(partitionStatisticsHashMap.values(), excludeTables));
        return selection;
    }

    @NotNull
    Set<PartitionIdentifier> getAllPartitions() {
        return new HashSet<>(partitionStatisticsHashMap.keySet());
    }

    @NotNull
    public Collection<PartitionStatistics> getAllStatistics() {
        return partitionStatisticsHashMap.values();
    }

    @Nullable
    public PartitionStatistics getStatistics(PartitionIdentifier identifier) {
        return partitionStatisticsHashMap.get(identifier);
    }

    public double getMaxCompactionScore() {
        return partitionStatisticsHashMap.values().stream().mapToDouble(stat -> stat.getCompactionScore().getMax())
                .max().orElse(0);
    }

    void enableCompactionAfter(PartitionIdentifier partition, long delayMs) {
        PartitionStatistics statistics = partitionStatisticsHashMap.computeIfPresent(partition, (k, v) -> {
            // FE's follower nodes may have a different timestamp with the leader node.
            v.setNextCompactionTime(System.currentTimeMillis() + delayMs);
            return v;
        });
        if (statistics != null && LOG.isDebugEnabled()) {
            LOG.debug("Enable compaction after {}ms: {}", delayMs, statistics);
        }
    }

    void removePartition(PartitionIdentifier partition) {
        partitionStatisticsHashMap.remove(partition);
    }

    @VisibleForTesting
    public void clearPartitions() {
        partitionStatisticsHashMap.clear();
    }

    @NotNull
    public List<CompactionRecord> getHistory() {
        if (compactionScheduler != null) {
            return compactionScheduler.getHistory();
        } else {
            return Collections.emptyList();
        }
    }

    public void cancelCompaction(long txnId) {
        compactionScheduler.cancelCompaction(txnId);
    }

    public boolean existCompaction(long txnId) {
        return compactionScheduler.existCompaction(txnId);
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        // partitions are added into map after loading, but they are never removed in checkpoint thread.
        // drop partition, drop table, truncate table, drop database, ...
        // all of above will cause partition info change, and it is difficult to call
        // remove partition for every case, so remove non-existed partitions only when writing image
        getAllPartitions()
                .stream()
                .filter(p -> !MetaUtils.isPhysicalPartitionExist(
                        GlobalStateMgr.getCurrentState(), p.getDbId(), p.getTableId(), p.getPartitionId()))
                .forEach(this::removePartition);

        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.COMPACTION_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        CompactionMgr compactionManager = reader.readJson(CompactionMgr.class);
        partitionStatisticsHashMap = compactionManager.partitionStatisticsHashMap;
    }

    public long getPartitionStatsCount() {
        return partitionStatisticsHashMap.size();
    }

    public PartitionStatistics triggerManualCompaction(PartitionIdentifier partition) {
        PartitionStatistics statistics = partitionStatisticsHashMap.compute(partition, (k, v) -> {
            if (v == null) {
                v = new PartitionStatistics(partition);
            }
            v.setPriority(PartitionStatistics.CompactionPriority.MANUAL_COMPACT);
            return v;
        });
        LOG.info("Trigger manual compaction, {}", statistics);
        return statistics;
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("PartitionStats", (long) partitionStatisticsHashMap.size());
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        List<Object> samples = partitionStatisticsHashMap.values()
                .stream()
                .limit(1)
                .collect(Collectors.toList());
        return Lists.newArrayList(Pair.create(samples, (long) partitionStatisticsHashMap.size()));
    }
}
