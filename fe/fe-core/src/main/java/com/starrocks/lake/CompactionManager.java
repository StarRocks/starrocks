// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.lake;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class CompactionManager {
    private static final long COMPACTION_THRESHOLD_COUNT = 10;
    private final Map<PartitionIdentifier, CompactionContext> contextMap = new HashMap<>();
    private final Queue<CompactionContext> contextList = new LinkedList<>();

    public CompactionManager() {
    }

    public synchronized void handlePartitionVersionUpdated(PartitionIdentifier partition) {
        CompactionContext context = contextMap.computeIfAbsent(partition, CompactionContext::new);
        if (context.addUpdateCount(1) == COMPACTION_THRESHOLD_COUNT) {
            contextList.offer(context);
        }
    }

    synchronized CompactionContext getCompactionContext() {
        return contextList.poll();
    }

    synchronized void returnCompactionContext(CompactionContext context) {
        if (context.getUpdateCount() <= 0) {
            contextMap.remove(context.getPartition());
        } else if (context.getUpdateCount() >= COMPACTION_THRESHOLD_COUNT) {
            contextList.offer(context);
        }
    }

    synchronized void removeCompactionContext(CompactionContext context) {
        contextMap.remove(context.getPartition());
    }
}
