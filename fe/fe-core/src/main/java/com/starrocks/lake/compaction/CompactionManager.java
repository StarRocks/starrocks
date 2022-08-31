// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CompactionManager {
    private static final Logger LOG = LogManager.getLogger(CompactionManager.class);

    @SerializedName(value = "partitionStatisticsHashMap")
    private final Map<PartitionIdentifier, PartitionStatistics> partitionStatisticsHashMap = new HashMap<>();

    private final List<CompactionPicker> compactionPickers = Lists.newArrayList();

    public CompactionManager() {
        initCompactionPicker();
    }

    void initCompactionPicker() {
        compactionPickers.add(new CompactionPickerByCount(Config.experimental_lake_compaction_max_version_count));
        compactionPickers.add(new CompactionPickerByTime(Config.experimental_lake_compaction_max_interval_seconds * 1000,
                Config.experimental_lake_compaction_min_version_count));
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

    public synchronized void handleDropPartition(PartitionIdentifier partition) {
        partitionStatisticsHashMap.remove(partition);
    }

    public synchronized void handleDropTable(long tableId) {
        partitionStatisticsHashMap.entrySet().removeIf(e -> e.getKey().getTableId() == tableId);
    }

    public synchronized void handleDropDb(long dbId) {
        partitionStatisticsHashMap.entrySet().removeIf(e -> e.getKey().getDbId() == dbId);
    }

    synchronized PartitionIdentifier choosePartitionToCompact() {
        PartitionStatistics target = null;
        for (CompactionPicker picker : compactionPickers) {
            target = picker.pick(partitionStatisticsHashMap.values());
            if (target != null) {
                Preconditions.checkState(!target.isDoingCompaction());
                target.setDoingCompaction(true);
                break;
            }
        }
        if (LOG.isDebugEnabled() && target != null) {
            LOG.debug("Compacting partition: {}", target);
        }
        return (target != null) ? target.getPartitionId() : null;
    }

    synchronized void enableCompactionAfter(PartitionIdentifier partition, long delayMs) {
        PartitionStatistics statistics = partitionStatisticsHashMap.get(partition);
        Preconditions.checkState(statistics != null);
        Preconditions.checkState(statistics.isDoingCompaction());
        statistics.setDoingCompaction(false);
        // FE's follower nodes may have a different timestamp with the leader node.
        statistics.setNextCompactionTime(System.currentTimeMillis() + delayMs);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enable partition to do compaction: {}", statistics);
        }
    }

    // todo: remove partition on follower nodes.
    synchronized void removePartition(PartitionIdentifier partition) {
        PartitionStatistics statistics = partitionStatisticsHashMap.remove(partition);
        Preconditions.checkState(statistics != null);
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
        compactionManager.initCompactionPicker();
        return compactionManager;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionManager that = (CompactionManager) o;
        return Objects.equals(partitionStatisticsHashMap, that.partitionStatisticsHashMap) &&
                Objects.equals(compactionPickers, that.compactionPickers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionStatisticsHashMap, compactionPickers);
    }
}