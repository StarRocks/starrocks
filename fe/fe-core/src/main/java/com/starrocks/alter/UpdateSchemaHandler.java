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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.UserException;
import com.starrocks.common.util.ListComparator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CancelStmt;
import com.starrocks.task.UpdateSchemaTask;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;


public class UpdateSchemaHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(UpdateSchemaHandler.class);
    protected ConcurrentMap<Long, AlterJobV2> updateSchemaJobs = Maps.newConcurrentMap();

    protected ThreadPoolExecutor updateSchemaExecutor;

    public UpdateSchemaHandler() {
        super("updateSchema");
        updateSchemaExecutor = ThreadPoolManager
                .newDaemonCacheThreadPool(Config.alter_max_worker_threads, Config.alter_max_worker_queue_size,
                          "updateSchema_pool", true);
    }

    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();
        runUpdateSchemaJob();
    }

    public void addUpdateSchemaJob(AlterJobV2 alterJob) {
        this.updateSchemaJobs.put(alterJob.getJobId(), alterJob);
    }

    private void runUpdateSchemaJob() {
        for (AlterJobV2 alterJob : updateSchemaJobs.values()) {
            if (alterJob.jobState.isFinalState()) {
                continue;
            }
            alterJob.run();
        }
    }

    @Override
    public synchronized ShowResultSet process(List<AlterClause> alterClauses, Database db, 
                                              OlapTable olapTable) throws UserException {
        return null;
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        throw new NotImplementedException();
    }

    @Override
    public synchronized void cancel(CancelStmt stmt) throws DdlException {
        throw new NotImplementedException();
    }

    public void handleFinishUpdateSchemaTask(UpdateSchemaTask task) throws RejectedExecutionException {
        updateSchemaExecutor.submit(task);
    }

    public void replayUpdateSchemaJob(AlterJobV2 alterJob) {
        AlterJobV2 existingJob = updateSchemaJobs.get(alterJob.getJobId());
        if (existingJob == null) {
            alterJob.replay(alterJob);
            updateSchemaJobs.put(alterJob.getJobId(), alterJob);
        } else {
            existingJob.replay(alterJob);
        }
    }

    private void getUpdateSchemaJobInfos(Database db, AlterJobV2.JobType type, List<List<Comparable>> updateSchemaJobInfos) {
        List<AlterJobV2> immutableUpdateSchemaJobs = ImmutableList.copyOf(updateSchemaJobs.values());
        ConnectContext ctx = ConnectContext.get();
        for (AlterJobV2 updateSchemaJob : immutableUpdateSchemaJobs) {
            if (updateSchemaJob.getDbId() != db.getId()) {
                continue;
            }
            if (updateSchemaJob.getType() != type) {
                continue;
            }
            updateSchemaJob.getInfo(updateSchemaJobInfos);
        }
    }

    public List<List<Comparable>> getUpdateSchemaJobInfosByDb(Database db) {
        List<List<Comparable>> updateSchemaJobInfos = new LinkedList<>();
        getUpdateSchemaJobInfos(db, AlterJobV2.JobType.UPDATE_SCHEMA, updateSchemaJobInfos);

        // sort by "JobId", "PartitionName", "CreateTime", "FinishTime"
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2, 3);
        updateSchemaJobInfos.sort(comparator);
        return updateSchemaJobInfos;        
    }

    public AlterJobV2 getUnfinishedUpdateSchemaJobV2ByJobId(long jobId) {
        for (AlterJobV2 job : updateSchemaJobs.values()) {
            if (job.getJobId() == jobId && !job.isDone()) {
                return job;
            }
        }
        return null;
    }
}