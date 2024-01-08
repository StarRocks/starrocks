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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/Alter.java

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
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.meta.lock.LockType;
import com.starrocks.meta.lock.Locker;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CancelStmt;
import com.starrocks.sql.ast.UpdateSchemaClause;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.UpdateSchemaTask;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;


public class UpdateSchemaHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(UpdateSchemaHandler.class);
    protected ConcurrentMap<Long, AlterJobV2> updateSchemaJobs = Maps.newConcurrentMap();

    public UpdateSchemaHandler() {
        super("update schema");
    }

    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();
        runUpdateSchemaJob();
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        throw new NotImplementedException();
    }

    @Override
    // add synchronized to avoid process 2 or more stmts at same time
    // TODO(zhangqiang)
    public synchronized ShowResultSet process(List<AlterClause> alterClauses, Database db,
                                              OlapTable olapTable) throws UserException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);
        if (alterClause instanceof UpdateSchemaClause) {
            UpdateSchemaClause updateClause = (UpdateSchemaClause) alterClause;
            Map<Long, ArrayListMultimap<Long, Long>> backendToIndexTablets = new HashMap<>();
            Map<Long, TOlapTableColumnParam> indexToColumns = new HashMap<>();
            AgentBatchTask batchTask = new AgentBatchTask();

            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {

                // TODO(zhangqiang)
                // split a seperate function
                for (Map.Entry<Long, MaterializedIndexMeta> pair : table.getIndexIdToMeta().entrySet()) {
                    MaterializedIndexMeta indexMeta = pair.getValue();
                    List<String> columns = Lists.newArrayList();
                    List<TColumn> columnsDesc = Lists.newArrayList();
                    List<Integer> columnSortKeyUids = Lists.newArrayList();

                    for (Column column : indexMeta.getSchema()) {
                        TColumn tColumn = column.toThrift();
                        tColumn.setColumn_name(column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX, tColumn.column_name));
                        column.setIndexFlag(tColumn, table.getIndexes(), table.getBfColumns());
                        columnsDesc.add(tColumn);
                    }
                    if (indexMeta.getSortKeyUniqueIds() != null) {
                        columnSortKeyUids.addAll(indexMeta.getSortKeyUniqueIds());
                    }
                    TOlapTableColumnParam columnParam = new TOlapTableColumnParam(columnsDesc, columnSortKeyUids, 
                                                                                  indexMeta.getShortKeyColumnCount());
                    indexToColumns[indexMeta.getId()] = columnParam;
                }

                // TODO(zhangqiang)
                // split a seperate function
                List<Partition> allPartitions = new ArrayList<>();
                allPartitions.addAll(olapTable.getPartitions());
                for (Partition partition : allPartitions) {
                    for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                        for (MaterializedIndex index : physicalPartition.getMaterializedIndices(
                                MaterializedIndex.IndexExtState.VISIBLE)) {
                            for (Tablet tablet : index.getTablets()) {
                                for (Long backendId : ((LocalTablet) tablet).getBackendIds()) {
                                    if (!backendToIndexTablets.containsKey(backendId)) {
                                        ArrayListMultimap<Long, Long> indexToTablets = ArrayListMultimap.create();
                                        indexToTablets.put(index.getId(), tablet.getId());
                                        backendToIndexTablets.put(backendId, indexToTablets);
                                    } else {
                                        ArrayListMultimap<Long, Long> indexToTablets = backendToIndexTablets.get(backendId);
                                        indexToTablets.put(index.getId(), tablet.getId());
                                    }
                                }
                            }
                        }
                    }
                }
                // submit agent task
                for (Long backendId : backendToIndexTablets.keySet()) {
                    UpdateSchemaTask task =  new UpdateSchemaTask(null, backendId,
                                db.getId(),
                                olapTable.getId(),
                                backendToIndexTablets.get(backendId),
                                indexToColumns
                    );

                    // add task to send
                    batchTask.addTask(task);
                }
                if (batchTask.getTaskNum() > 0) {
                    for (AgentTask task : batchTask.getAllTasks()) {
                        AgentTaskQueue.addTask(task);
                    }
                    AgentTaskExecutor.submit(batchTask);
                    LOG.debug("tablet[{}] send update schema task. num: {}", batchTask.getTaskNum());
                }

            } catch (Exception e) {
                throw new UserException(e.getMessage());
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        } else {
            Preconditions.checkState(false, alterClause.getClass());
        }
        return null;
    }

    @Override
    public synchronized void cancel(CancelStmt stmt) throws DdlException {
        throw new NotImplementedException();
    }

    public void finishUpdateSchemaTask(UpdateSchemaTask task, TFinishTaskRequest request) {
        UpdateSchemaJob job = updateSchemaJobs.get(task.getJobId());
        if (job == nullptr) {
            // TODO
        }
        job.finishUpdateSchemaTask(task, request);
    }
}