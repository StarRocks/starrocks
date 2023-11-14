// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class CompactionManager {
    private static final Logger LOG = LogManager.getLogger(CompactionManager.class);

    @SerializedName(value = "partitionStatisticsHashMap")
    private final Map<PartitionIdentifier, PartitionStatistics> partitionStatisticsHashMap = new HashMap<>();

    private Selector selector;
    private Sorter sorter;
    private CompactionScheduler compactionScheduler;

    public CompactionManager() {
        try {
            init();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void init() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
            IllegalAccessException {
        String packageName = CompactionManager.class.getPackage().getName();
        Class<?> selectorClazz = Class.forName(packageName + "." + Config.lake_compaction_selector);
        selector = (Selector) selectorClazz.getConstructor().newInstance();

        Class<?> sorterClazz = Class.forName(packageName + "." + Config.lake_compaction_sorter);
        sorter = (Sorter) sorterClazz.getConstructor().newInstance();
    }

    public synchronized void start() {
        if (compactionScheduler == null) {
            compactionScheduler = new CompactionScheduler(this, GlobalStateMgr.getCurrentSystemInfo(),
                    GlobalStateMgr.getCurrentGlobalTransactionMgr(), GlobalStateMgr.getCurrentState());
            compactionScheduler.start();
        }
    }

    public synchronized void handleLoadingFinished(PartitionIdentifier partition, long version, long versionTime) {
        PartitionStatistics statistics = partitionStatisticsHashMap.computeIfAbsent(partition, PartitionStatistics::new);
        PartitionVersion currentVersion = new PartitionVersion(version, versionTime);
        statistics.setCurrentVersion(currentVersion);
        if (statistics.getLastCompactionVersion() == null) {
            // Set version-1 as last compaction version
            statistics.setLastCompactionVersion(new PartitionVersion(version - 1, versionTime));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished loading: {}", statistics);
        }
    }

    public synchronized void handleCompactionFinished(PartitionIdentifier partition, long version, long versionTime) {
        PartitionStatistics statistics = partitionStatisticsHashMap.computeIfAbsent(partition, PartitionStatistics::new);
        PartitionVersion compactionVersion = new PartitionVersion(version, versionTime);
        statistics.setCurrentVersion(compactionVersion);
        statistics.setLastCompactionVersion(compactionVersion);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished compaction: {}", statistics);
        }
    }

    @NotNull
    synchronized List<PartitionIdentifier> choosePartitionsToCompact(@NotNull Set<PartitionIdentifier> excludes) {
        return choosePartitionsToCompact().stream().filter(p -> !excludes.contains(p)).collect(Collectors.toList());
    }

    @NotNull
    synchronized List<PartitionIdentifier> choosePartitionsToCompact() {
        List<PartitionStatistics> selection = sorter.sort(selector.select(partitionStatisticsHashMap.values()));
        return selection.stream().map(PartitionStatistics::getPartition).collect(Collectors.toList());
    }

    @NotNull
    synchronized Set<PartitionIdentifier> getAllPartitions() {
        return new HashSet<>(partitionStatisticsHashMap.keySet());
    }

    synchronized void enableCompactionAfter(PartitionIdentifier partition, long delayMs) {
        PartitionStatistics statistics = partitionStatisticsHashMap.get(partition);
        if (statistics != null) {
            // FE's follower nodes may have a different timestamp with the leader node.
            statistics.setNextCompactionTime(System.currentTimeMillis() + delayMs);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Enable compaction after {}ms: {}", delayMs, statistics);
            }
        }
    }

    synchronized void removePartition(PartitionIdentifier partition) {
        partitionStatisticsHashMap.remove(partition);
    }

    public synchronized long saveCompactionManager(DataOutput out, long checksum) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
        checksum ^= getChecksum();
        return checksum;
    }

    public synchronized long getChecksum() {
        return partitionStatisticsHashMap.size();
    }

    public static CompactionManager loadCompactionManager(DataInput in) throws IOException {
        String json = Text.readString(in);
        CompactionManager compactionManager = GsonUtils.GSON.fromJson(json, CompactionManager.class);
        try {
            compactionManager.init();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return compactionManager;
    }

    public long getPartitionStatsCount() {
        return partitionStatisticsHashMap.size();
    }
}