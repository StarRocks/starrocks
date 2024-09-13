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

package com.starrocks.alter;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.UserException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.meta.TabletMetastore;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CompactionClause;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.CompactionTask;
import org.apache.hadoop.util.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;

public class CompactionHandler {
    private static final Logger LOG = LogManager.getLogger(CompactionHandler.class);

    // add synchronized to avoid process 2 or more stmts at same time
    public static synchronized ShowResultSet process(List<AlterClause> alterClauses, Database db,
                                                     OlapTable olapTable) throws UserException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);
        Preconditions.checkState(alterClause instanceof CompactionClause);

        CompactionClause compactionClause = (CompactionClause) alterClause;
        if (RunMode.isSharedDataMode()) {
            Locker locker = new Locker();
            locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
            try {
                List<Partition> allPartitions = findAllPartitions(olapTable, compactionClause);
                for (Partition partition : allPartitions) {
                    for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                        PartitionIdentifier partitionIdentifier =
                                new PartitionIdentifier(db.getId(), olapTable.getId(), physicalPartition.getId());
                        CompactionMgr compactionManager = GlobalStateMgr.getCurrentState().getCompactionMgr();
                        compactionManager.triggerManualCompaction(partitionIdentifier);
                    }
                }
            } finally {
                locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
            }
        } else {
            ArrayListMultimap<Long, Long> backendToTablets = ArrayListMultimap.create();
            AgentBatchTask batchTask = new AgentBatchTask();

            Locker locker = new Locker();
            locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
            try {
                List<Partition> allPartitions = findAllPartitions(olapTable, compactionClause);

                TabletMetastore tabletMetastore = GlobalStateMgr.getCurrentState().getTabletMetastore();
                for (Partition partition : allPartitions) {
                    List<PhysicalPartition> physicalPartitionList = tabletMetastore.getAllPhysicalPartition(partition);
                    for (PhysicalPartition physicalPartition : physicalPartitionList) {
                        List<MaterializedIndex> materializedIndices = tabletMetastore
                                .getMaterializedIndices(physicalPartition, MaterializedIndex.IndexExtState.VISIBLE);
                        for (MaterializedIndex materializedIndex : materializedIndices) {
                            List<Tablet> tabletList = tabletMetastore.getAllTablets(materializedIndex);
                            for (Tablet tablet : tabletList) {
                                for (Long backendId : ((LocalTablet) tablet).getBackendIds()) {
                                    backendToTablets.put(backendId, tablet.getId());
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw new UserException(e.getMessage());
            } finally {
                locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
            }

            for (Long backendId : backendToTablets.keySet()) {
                CompactionTask task = new CompactionTask(null, backendId,
                        db.getId(),
                        olapTable.getId(),
                        backendToTablets.get(backendId),
                        ((CompactionClause) alterClause).isBaseCompaction()
                );

                // add task to send
                batchTask.addTask(task);
            }
            if (batchTask.getTaskNum() > 0) {
                for (AgentTask task : batchTask.getAllTasks()) {
                    AgentTaskQueue.addTask(task);
                }
                AgentTaskExecutor.submit(batchTask);
                LOG.debug("tablet[{}] send compaction task. num: {}", batchTask.getTaskNum());
            }
        }
        return null;
    }

    @NotNull
    private static List<Partition> findAllPartitions(OlapTable olapTable, CompactionClause compactionClause) {
        List<Partition> allPartitions = new ArrayList<>();
        if (compactionClause.getPartitionNames().isEmpty()) {
            allPartitions.addAll(olapTable.getPartitions());
        } else {
            compactionClause.getPartitionNames().stream()
                    .map(partitionName -> new SimpleEntry<>(partitionName, olapTable.getPartition(partitionName)))
                    .forEach(entry -> {
                        Partition p = entry.getValue();
                        if (p == null) {
                            throw new RuntimeException("Partition not found: " + entry.getKey());
                        }
                        allPartitions.add(p);
                    });
        }
        return allPartitions;
    }
}
