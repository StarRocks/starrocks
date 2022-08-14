// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompactionManager {
    private static final Logger LOG = LogManager.getLogger(CompactionManager.class);

    private final Map<PartitionIdentifier, PartitionStatistics> partitionStatisticsHashMap = new HashMap<>();
    private final List<CompactionPicker> compactionPickers = Lists.newArrayList();

    public CompactionManager() {
        compactionPickers.add(new CompactionPickerByCount(Config.experimental_lake_compaction_max_version_count));
        compactionPickers.add(new CompactionPickerByTime(Config.experimental_lake_compaction_max_interval_seconds * 1000,
                Config.experimental_lake_compaction_min_version_count));
    }

    public synchronized void handleLoadingFinished(PartitionIdentifier partition, long version) {
        PartitionStatistics statistics = partitionStatisticsHashMap.get(partition);
        if (statistics == null) {
            // We don't know the compaction version and time, just set compaction version as |version-1|.
            // FE's follower nodes may have a different timestamp with the leader node.
            long now = System.currentTimeMillis();
            statistics = new PartitionStatistics(partition, now, version - 1, version);
            partitionStatisticsHashMap.put(partition, statistics);
        } else {
            Preconditions.checkState(version == statistics.getCurrentVersion() + 1);
            statistics.setCurrentVersion(version);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished loading: {}", statistics);
        }
    }

    public synchronized void handleCompactionFinished(PartitionIdentifier partition, long version) {
        // FE's follower nodes may have a different timestamp with the leader node.
        long now = System.currentTimeMillis();
        PartitionStatistics statistics = partitionStatisticsHashMap.get(partition);
        if (statistics == null) {
            statistics = new PartitionStatistics(partition, now, version, version);
            partitionStatisticsHashMap.put(partition, statistics);
        } else {
            Preconditions.checkState(version == statistics.getCurrentVersion() + 1);
            statistics.setCurrentVersion(version);
            statistics.setLastCompactionVersion(version);
            statistics.setLastCompactionTime(now);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished compaction: {}", statistics);
        }
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
}