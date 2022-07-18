// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.lake.compaction;

import com.clearspring.analytics.util.DoublyLinkedList;
import com.clearspring.analytics.util.ListNode2;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompactionManager {
    private static final Logger LOG = LogManager.getLogger(CompactionManager.class);

    private final Map<PartitionIdentifier, ListNode2<PartitionStatistics>> statisticsMap = new HashMap<>();
    private final DoublyLinkedList<PartitionStatistics> statisticsList = new DoublyLinkedList<>();
    private final List<PartitionPicker> pickerList = Lists.newArrayList();

    public CompactionManager() {
        pickerList.add(new PartitionPickerByCount(Config.experimental_lake_compaction_max_version_count));
        pickerList.add(new PartitionPickerByTime(Config.experimental_lake_compaction_max_interval_seconds * 1000,
                Config.experimental_lake_compaction_min_version_count));
    }

    public synchronized void handleLoadingFinished(PartitionIdentifier partition, long version) {
        ListNode2<PartitionStatistics> listNode = statisticsMap.get(partition);
        if (listNode == null) {
            // We don't know the compaction version and time, just set compaction version as |version-1|.
            // FE's follower nodes may have a different timestamp with the leader node.
            long now = System.currentTimeMillis();
            PartitionStatistics statistics = new PartitionStatistics(partition, now, version - 1, version);
            listNode = statisticsList.enqueue(statistics);
            statisticsMap.put(partition, listNode);
        } else {
            PartitionStatistics statistics = listNode.getValue();
            Preconditions.checkState(version == statistics.getCurrentVersion() + 1);
            statistics.setCurrentVersion(version);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished loading: {}", listNode.getValue());
        }
    }

    public synchronized void handleCompactionFinished(PartitionIdentifier partition, long version) {
        // FE's follower nodes may have a different timestamp with the leader node.
        long now = System.currentTimeMillis();
        ListNode2<PartitionStatistics> listNode = statisticsMap.get(partition);
        if (listNode == null) {
            PartitionStatistics statistics = new PartitionStatistics(partition, version, now, version);
            listNode = statisticsList.enqueue(statistics);
            statisticsMap.put(partition, listNode);
        } else {
            PartitionStatistics statistics = listNode.getValue();
            Preconditions.checkState(version == statistics.getCurrentVersion() + 1);
            statistics.setCurrentVersion(version);
            statistics.setLastCompactionVersion(version);
            statistics.setLastCompactionTime(now);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished compaction: {}", listNode.getValue());
        }
    }

    synchronized PartitionIdentifier choosePartitionToCompact() {
        PartitionStatistics target = null;
        for (PartitionPicker picker : pickerList) {
            target = picker.pick(statisticsList);
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
        ListNode2<PartitionStatistics> listNode = statisticsMap.get(partition);
        Preconditions.checkState(listNode != null);
        Preconditions.checkState(listNode.getValue().isDoingCompaction());
        listNode.getValue().setDoingCompaction(false);
        // FE's follower nodes may have a different timestamp with the leader node.
        listNode.getValue().setNextCompactionTime(System.currentTimeMillis() + delayMs);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enable partition to do compaction: {}", listNode.getValue());
        }
    }

    // todo: remove partition on follower nodes.
    synchronized void removePartition(PartitionIdentifier partition) {
        ListNode2<PartitionStatistics> listNode = statisticsMap.remove(partition);
        Preconditions.checkState(listNode != null);
        statisticsList.remove(listNode);
    }
}