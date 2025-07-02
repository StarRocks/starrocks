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

package com.starrocks.alter.dynamictablet;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class SplitTabletJob extends DynamicTabletJob {
    private static final Logger LOG = LogManager.getLogger(SplitTabletJob.class);

    // Physical partition id -> DynamicTabletContext
    @SerializedName(value = "dynamicTabletContexts")
    protected final Map<Long, DynamicTabletContext> dynamicTabletContexts;

    public SplitTabletJob(long jobId, long dbId, long tableId, Map<Long, DynamicTabletContext> dynamicTabletContexts) {
        super(jobId, JobType.SPLIT_TABLET, dbId, tableId);
        this.dynamicTabletContexts = dynamicTabletContexts;
    }

    // Begin and commit the split transaction
    @Override
    protected void runPendingJob() {

    }

    // Wait for previous version published, than publish the split transaction
    @Override
    protected void runPreparingJob() {

    }

    // Wait for the publish finished, than replace old tablets with new tablets
    @Override
    protected void runRunningJob() {

    }

    // Wait for all previous transactions finished, than delete old tablets
    @Override
    protected void runCleaningJob() {

    }

    @Override
    protected void runAbortingJob() {

    }

    @Override
    protected boolean canAbort() {
        return jobState == JobState.PENDING || jobState == JobState.PREPARING;
    }

    @Override
    public long getParallelTablets() {
        long parallelTablets = 0;
        for (DynamicTabletContext dynamicTabletContext : dynamicTabletContexts.values()) {
            parallelTablets += dynamicTabletContext.getParallelTablets();
        }
        return parallelTablets;
    }

    @Override
    public void replay() {
        switch (jobState) {
            case PENDING:
                setDynamicTablets();
                break;

            default:
                break;
        }
    }

    protected void setDynamicTablets() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) {
            // Table is dropped
            return;
        }

        OlapTable olapTable = (OlapTable) table;

        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(dbId, tableId, LockType.WRITE);
        try {
            for (Map.Entry<Long, DynamicTabletContext> physicalPartitionEntry : dynamicTabletContexts.entrySet()) {
                long physicalPartitionId = physicalPartitionEntry.getKey();
                DynamicTabletContext dynamicTabletContext = physicalPartitionEntry.getValue();

                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionId);
                if (physicalPartition == null) {
                    continue;
                }

                Map<Long, DynamicTablets> indexIdToDynamicTablets = dynamicTabletContext.getIndexIdToDynamicTablets();
                for (Map.Entry<Long, DynamicTablets> indexEntry : indexIdToDynamicTablets.entrySet()) {
                    MaterializedIndex index = physicalPartition.getIndex(indexEntry.getKey());
                    if (index == null) {
                        continue;
                    }

                    index.setDynamicTablets(indexEntry.getValue());
                }
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(dbId, tableId, LockType.WRITE);
        }
    }

    protected void clearDynamicTablets() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) {
            // Table is dropped
            return;
        }

        OlapTable olapTable = (OlapTable) table;

        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(dbId, tableId, LockType.WRITE);
        try {
            for (Map.Entry<Long, DynamicTabletContext> physicalPartitionEntry : dynamicTabletContexts.entrySet()) {
                long physicalPartitionId = physicalPartitionEntry.getKey();
                DynamicTabletContext dynamicTabletContext = physicalPartitionEntry.getValue();

                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionId);
                if (physicalPartition == null) {
                    continue;
                }

                Map<Long, DynamicTablets> indexIdToDynamicTablets = dynamicTabletContext.getIndexIdToDynamicTablets();
                for (Map.Entry<Long, DynamicTablets> indexEntry : indexIdToDynamicTablets.entrySet()) {
                    MaterializedIndex index = physicalPartition.getIndex(indexEntry.getKey());
                    if (index == null) {
                        continue;
                    }

                    index.setDynamicTablets(null);
                }
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(dbId, tableId, LockType.WRITE);
        }
    }
}
