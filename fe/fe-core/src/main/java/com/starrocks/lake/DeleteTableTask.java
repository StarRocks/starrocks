// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.lake;

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.server.GlobalStateMgr;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

class DeleteTableTask implements Runnable {
    private final LakeTable table;

    DeleteTableTask(LakeTable table) {
        this.table = table;
    }

    @Override
    public void run() {
        Set<Long> tabletIds = new HashSet<>();
        for (Partition partition : table.getAllPartitions()) {
            List<MaterializedIndex> allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
            for (MaterializedIndex materializedIndex : allIndices) {
                for (Tablet tablet : materializedIndex.getTablets()) {
                    tabletIds.add(tablet.getId());
                }
            }
        }
        GlobalStateMgr.getCurrentState().getShardManager().getShardDeleter().addUnusedShardId(tabletIds);
    }
}
