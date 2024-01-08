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



public class UpdateSchemaJob extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(UpdateSchemaJob.class);

    @SerializedName(value = "backendToIndexTabletMap")
    private Table<Long, Long, List<Long>> backendToIndexTabletMap = HashBasedTable.create();
    @SerializedName(value = "indexToColumnParam")
    private Map<Long, TOlapTableColumnParam> indexToColumnParam = Maps.newHashMap();

    private AgentBatchTask updateSchemaBatchTask = new AgentBatchTask();

    private Table<Long, Long, List<Long>> backendToIndexTabletMap = HashBasedTable.create();
    private Map<Long, TOlapTableColumnParam> indexToColumnParam = Maps.newHashMap();
    private Map<Long, Set<Long>> successTabletToBackends = Maps.newHashMap<>();


    public UpdateSchemaJob(long jobId, long dbId, long tableId, String tableName, long timeooutMs) {
        super(jobId, JobType.UPDATE_SCHEMA, dbId, tableId, tableName, timeoutMs);
    }

    @Override
    public synchronized void run() {
        try {
            while (true) {
                JobState
            }
        } catch (AlterCancelException e) {

        }
    }

    @Override
    protected void runPendingJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.PENDING, jobState);
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Database " + dbId + " does not exist");
        }

        if (!checkTableStable(db)) {
            return;
        }

        //
        OlapTable tbl = (OlapTable) db.getTable(tableId);
        if (tbl == null) {
            throw new AlterCancelException("Table " + tableId + " does not exist");
        }

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            // get index schema
            for (Map.Entry<Long, MaterializedIndexMeta> pair : tbl.getIndexIdToMeta()) {
                MaterializedIndexMeta indexMeta = pait.getValue();
                List<String> columns = Lists.newArrayList();
                List<TColumn> columnsDesc = Lists.newArrayList();
                List<Integer> columnSortKeyUids = Lists.newArrayList();

                for (Column column : indexMeta.getSchema()) {
                    TColumn tColumn = column.toThrift();
                    tColumn.setColumn_name(column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX, tColumn.column_name));
                    column.setIndexFlag(tCOlumn, tbl.getIndexes(), tbl.getBfColumns());
                    columnsDesc.add(tColumn);
                }
                if (indexMeta.getSortKeyUniqueIds() != null) {
                    columnSortKeyUids.addAll(indexMeta.getSortKeyUniqueIds());
                }
                TOlapTableColumnParam columnParam = new TOlapTableColumnParam(columnsDesc, columnSortKeyUids,
                                                                              indexMeta.getShortKeyColumnCount());
                indexToColumnParam.put(indexMeta.getId(), columnParam);
            }
            // get all tablet
            for (Partition partition : tbl.getPartitions()) {
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    for (MaterializedIndex index : physicalPartition.getMaterializedIndices(
                            MaterializedIndex.IndexExtState.VISIBLE)) {
                        for (Tablet tablet : index.getTablets()) {
                            for (Long backendId : ((LocalTablet) tablet).getBackendIds()) {
                                backendToIndexTabletMap.put(backendId, index.getId(), tablet.getId());
                            }
                        }
            }

            // create AgentBatch Task
            for (Table.Cell<Long, Long, List<Long>> cell : backendToIndexTabletMap.cellSet()) {
                Long backendId = cell.getRowKey();
                Long indexId = cell.getColumnKey();
                List<Long> tablets = cell.getValue();
                if (!indexToColumnParam.contains(indexId)) {
                    // throw exception
                }
                UpdateSchemaTask task = new UpdateSchemaTask(null, backendId, db.getId(), tbl.getId(),
                            indexId, tablets, indexToColumnParam.get(indexId));
                // add task to send
                updateSchemaBatchTask.addTask(task);
            }

            if (batchTask.getTaskNum() > 0) {
                for (AgentTask task : batchTask.getAllTasks()) {
                    AgentTaskQueue.addTask(task);
                }
                AgentTaskExecutor.submit(batchTask);
                LOG.debug("table[{}] send update scheam task. num: {}", tbl.getName(), batchTask.getTaskNum());
            }
            this.jobState = JobState.RUNNING;
            tbl.setState(OlapTableState.UPDATE_SCHEMA);

            GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
        } catch (AlterCancelException e) {
            // cancel
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
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new AlterCancelException("Database " + dbId + " does not exist");
        }

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        if (!updateSchemaBatchTask.isFinished()) {
            // TODO retry
            List<AgentTask> task = updateSchemaBatchTask.getUnfinishedTasks(2000);
        }

        // remove task, write EditLog
        onFinished(tbl);

        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();

        // write EditLog
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(this);
    }


    /*
     * cancelImpl() can be called any time any place.
     * We need to clean any possible residual of this job.
     */
    @Override
    protected synchronized boolean cancelImpl(String errMsg) {

    }


    private void replayJob(UpdateSchemaJob replayedJob) {
        this.jobState = JobState.PENDING;
    }

    @Override
    public void replay(AlterJObV2 replayedJob) {
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

    public void handleFinishedUpdateSchemaTask(UpdateSchemaTask finishedTask, TFinishTaskRequest request) {
        List<Long> errorTabletIds = null;
        if (request.isSetError_tablet_ids()) {
            errorTabletIds = request.getError_tablet_ids();
        }
        if (errorTabletIds.size() == finishedTask.getTablets().size()) {
            // all tablets failed, retry
        } else {
            UpdateSchemaTask task = new UpdateSchemaTask(null, finishedTask.getBackendId(), finishedTask.getDbId(),
                        finishedTask.getTableId(), finishedTask.getIndexId(), )
            finishedTask.setFinished(true);
        }
    }

} 