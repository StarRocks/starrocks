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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/SchemaChangeJobV2.java

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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.UpdateSchemaTask;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TOlapTableColumnParam;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class UpdateSchemaJob extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(UpdateSchemaJob.class);

    private AgentBatchTask updateSchemaBatchTask = new AgentBatchTask();

    private Table<Long, Long, List<Long>> backendToIndexTabletMap = HashBasedTable.create();
    private Map<Long, TOlapTableColumnParam> indexToColumnParam = Maps.newHashMap();
    private long failedTimes = 0;


    public UpdateSchemaJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.UPDATE_SCHEMA, dbId, tableId, tableName, timeoutMs);
    }

    public AgentBatchTask getUpdateSchemaBatchTask() {
        return updateSchemaBatchTask;
    }

    @Override
    protected void runPendingJob() throws AlterCancelException {
        LOG.info("start run update schema job: {}", jobId);
        Preconditions.checkState(jobState == JobState.PENDING, jobState);
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            LOG.info("database does not exist when running update schema job {}", jobId);
            this.jobState = JobState.FINISHED;
            this.finishedTimeMs = System.currentTimeMillis();
            GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
            return;
        }

        try {
            if (!checkTableStable(db)) {
                return;
            }
        } catch (AlterCancelException e) {
            // table not exist
            this.jobState = JobState.FINISHED;
            this.finishedTimeMs = System.currentTimeMillis();
            GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
            return;
        }

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);        
        OlapTable tbl = (OlapTable) db.getTable(tableId);
        try {
            // get index schema
            indexToColumnParam.clear();
            for (Map.Entry<Long, MaterializedIndexMeta> pair : tbl.getIndexIdToMeta().entrySet()) {
                MaterializedIndexMeta indexMeta = pair.getValue();
                List<String> columns = Lists.newArrayList();
                List<TColumn> columnsDesc = Lists.newArrayList();
                List<Integer> columnSortKeyUids = Lists.newArrayList();

                for (Column column : indexMeta.getSchema()) {
                    TColumn tColumn = column.toThrift();
                    tColumn.setColumn_name(
                            column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX, tColumn.column_name));
                    column.setIndexFlag(tColumn, tbl.getIndexes(), tbl.getBfColumns());
                    columnsDesc.add(tColumn);
                }
                if (indexMeta.getSortKeyUniqueIds() != null) {
                    columnSortKeyUids.addAll(indexMeta.getSortKeyUniqueIds());
                }
                TOlapTableColumnParam columnParam = new TOlapTableColumnParam(columnsDesc, columnSortKeyUids,
                                                                              indexMeta.getShortKeyColumnCount());
                indexToColumnParam.put(indexMeta.getIndexId(), columnParam);
            }
            // get all tablet
            for (Partition partition : tbl.getPartitions()) {
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    for (MaterializedIndex index : physicalPartition.getMaterializedIndices(
                            MaterializedIndex.IndexExtState.VISIBLE)) {
                        for (Tablet tablet : index.getTablets()) {
                            for (Long backendId : ((LocalTablet) tablet).getBackendIds()) {
                                List<Long> tabletsList = backendToIndexTabletMap.get(backendId, index.getId());
                                if (tabletsList != null) {
                                    tabletsList.add(Long.valueOf(tablet.getId()));
                                } else {
                                    tabletsList = Lists.newArrayList();
                                    tabletsList.add(Long.valueOf(tablet.getId()));
                                    backendToIndexTabletMap.put(backendId, index.getId(), tabletsList);
                                }
                            }
                        }
                    }
                }
            }

            // create AgentBatch Task
            for (Table.Cell<Long, Long, List<Long>> cell : backendToIndexTabletMap.cellSet()) {
                Long backendId = cell.getRowKey();
                Long indexId = cell.getColumnKey();
                List<Long> tablets = cell.getValue();
                if (!indexToColumnParam.containsKey(indexId)) {
                    errMsg = "table[" + tbl.getName() + "] can not find index:" + indexId + " in MaterializedIndexMeta";
                    failedTimes++;
                    LOG.warn("execute update table {} schema job {} fail {} times", tbl.getName(), jobId, failedTimes);
                    if (failedTimes > 3) {
                        throw new AlterCancelException(errMsg);
                    }
                }
                MaterializedIndexMeta meta = tbl.getIndexMetaByIndexId(indexId);
                UpdateSchemaTask task = new UpdateSchemaTask(null, backendId, db.getId(), tbl.getId(),
                            indexId, jobId, tablets, meta.getSchemaId(), meta.getSchemaVersion(),
                            indexToColumnParam.get(indexId));
                // add task to send
                updateSchemaBatchTask.addTask(task);
            }

            if (updateSchemaBatchTask.getTaskNum() > 0) {
                for (AgentTask task : updateSchemaBatchTask.getAllTasks()) {
                    AgentTaskQueue.addTask(task);
                }
                AgentTaskExecutor.submit(updateSchemaBatchTask);
                LOG.info("table[{}] send update scheam task. num: {}", tbl.getName(), updateSchemaBatchTask.getTaskNum());
            }
            this.jobState = JobState.RUNNING;

            GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }


    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        this.jobState = JobState.RUNNING;
    }

    @Override
    protected void runRunningJob() throws AlterCancelException {
        LOG.info("waitting update schema job {} to be finished", jobId);
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            LOG.info("database does not exist when running update schema job {}", jobId);
            this.jobState = JobState.FINISHED;
            this.finishedTimeMs = System.currentTimeMillis();
            GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
            return;
        }

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        OlapTable tbl = (OlapTable) db.getTable(tableId);
        try {
            if (tbl == null) {
                LOG.info("database does not exist when running update schema job {}", jobId);
                this.jobState = JobState.FINISHED;
                this.finishedTimeMs = System.currentTimeMillis();
                GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
                return;
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        if (!updateSchemaBatchTask.isFinished()) {
            List<AgentTask> tasks = updateSchemaBatchTask.getUnfinishedTasks(2000);
            for (AgentTask task : tasks) {
                if (task.isFailed() || task.getFailedTimes() >= 3) {
                    throw new AlterCancelException("update schema task failed: " + task.getErrorMsg());
                }
            }
            return;
        }

        // remove task, write EditLog
        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();

        // write EditLog
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        tbl.setState(OlapTableState.NORMAL);
    }


    /*
     * cancelImpl() can be called any time any place.
     * We need to clean any possible residual of this job.
     */
    @Override
    protected synchronized boolean cancelImpl(String errMsg) {
        if (jobState.isFinalState()) {
            return false;
        }
        cancelInternal();
        return true;
    }

    private void cancelInternal() {
        // clear tasks if has
        AgentTaskQueue.removeBatchTask(updateSchemaBatchTask, TTaskType.UPDATE_SCHEMA);
        jobState = JobState.CANCELLED;
    }


    private void replayJob(UpdateSchemaJob replayedJob) {
        this.jobState = JobState.PENDING;
    }

    private void replayFinished(UpdateSchemaJob replayedJob) {
        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = replayedJob.finishedTimeMs;
    }

    private void replayCancelled(UpdateSchemaJob replayedJob) {
        cancelInternal();
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        UpdateSchemaJob replayedUpdateSchemaJob = (UpdateSchemaJob) replayedJob;
        switch (replayedJob.jobState) {
            case PENDING:
            case WAITING_TXN:
            case RUNNING:
                replayJob(replayedUpdateSchemaJob);
                break;
            case FINISHED:
                replayFinished(replayedUpdateSchemaJob);
                break;
            case CANCELLED:
                replayCancelled(replayedUpdateSchemaJob);
                break;
            default:
                break;
        }
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        String progress = FeConstants.NULL_STRING;
        if (jobState == JobState.RUNNING && updateSchemaBatchTask.getTaskNum() > 0) {
            progress = updateSchemaBatchTask.getFinishedTaskNum() + "/" + updateSchemaBatchTask.getTaskNum();
        }

        List<Comparable> info = Lists.newArrayList();
        info.add(jobId);
        info.add(tableName);
        info.add(TimeUtils.longToTimeString(createTimeMs));
        info.add(TimeUtils.longToTimeString(finishedTimeMs));
        info.add(jobState.name());
        info.add(errMsg);
        info.add(progress);
        info.add(timeoutMs / 1000);
        infos.add(info);
    }

    @Override
    protected void runFinishedRewritingJob() {
        // nothing to do
    }


    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, UpdateSchemaJob.class);
        Text.writeString(out, json);
    }

    @Override
    public Optional<Long> getTransactionId() {
        return Optional.of(Long.valueOf(-1));
    }

    public List<List<String>> getUnfinishedTasks(int limit) {
        List<List<String>> taskInfos = Lists.newArrayList();
        if (jobState == JobState.RUNNING) {
            List<AgentTask> tasks = updateSchemaBatchTask.getUnfinishedTasks(limit);
            for (AgentTask agentTask : tasks) {
                UpdateSchemaTask task = (UpdateSchemaTask) agentTask;
                List<String> info = Lists.newArrayList();
                info.add(String.valueOf(task.getBackendId()));
                info.add(String.valueOf(task.getTableId()));
                info.add(String.valueOf(task.getSignature()));
                taskInfos.add(info);
            }
        }
        return taskInfos;
    }
} 