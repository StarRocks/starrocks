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

<<<<<<< HEAD
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/SystemHandler.java

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

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
package com.starrocks.alter;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
<<<<<<< HEAD
import com.starrocks.catalog.Tablet;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CancelStmt;
=======
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterClause;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.sql.ast.CompactionClause;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.CompactionTask;
<<<<<<< HEAD
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
=======
import org.apache.hadoop.util.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;

<<<<<<< HEAD
public class CompactionHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(CompactionHandler.class);

    public CompactionHandler() {
        super("compaction");
    }


    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        throw new NotImplementedException();
    }

    @Override
    // add synchronized to avoid process 2 or more stmts at same time
    public synchronized ShowResultSet process(List<AlterClause> alterClauses, Database database,
                                              OlapTable olapTable) throws UserException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);
        if (alterClause instanceof CompactionClause) {
            CompactionClause compactionClause = (CompactionClause) alterClause;
            ArrayListMultimap<Long, Long> backendToTablets = ArrayListMultimap.create();
            AgentBatchTask batchTask = new AgentBatchTask();

            database.readLock();
            try {
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

                for (Partition partition : allPartitions) {
                    for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                        for (Tablet tablet : index.getTablets()) {
                            for (Long backendId : ((LocalTablet) tablet).getBackendIds()) {
                                backendToTablets.put(backendId, tablet.getId());
=======
public class CompactionHandler  {
    private static final Logger LOG = LogManager.getLogger(CompactionHandler.class);

    // add synchronized to avoid process 2 or more stmts at same time
    public static synchronized ShowResultSet process(List<AlterClause> alterClauses, Database db,
                                                     OlapTable olapTable) throws StarRocksException {
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
                for (Partition partition : allPartitions) {
                    for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                        for (MaterializedIndex index : physicalPartition.getMaterializedIndices(
                                MaterializedIndex.IndexExtState.VISIBLE)) {
                            for (Tablet tablet : index.getTablets()) {
                                for (Long backendId : ((LocalTablet) tablet).getBackendIds()) {
                                    backendToTablets.put(backendId, tablet.getId());
                                }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                            }
                        }
                    }
                }
            } catch (Exception e) {
<<<<<<< HEAD
                throw new UserException(e.getMessage());
            } finally {
                database.readUnlock();
=======
                throw new StarRocksException(e.getMessage());
            } finally {
                locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }

            for (Long backendId : backendToTablets.keySet()) {
                CompactionTask task = new CompactionTask(null, backendId,
<<<<<<< HEAD
                        database.getId(),
=======
                        db.getId(),
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
        } else {
            Preconditions.checkState(false, alterClause.getClass());
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        return null;
    }

<<<<<<< HEAD
    @Override
    public synchronized void cancel(CancelStmt stmt) throws DdlException {
        throw new NotImplementedException();
    }

=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
}
