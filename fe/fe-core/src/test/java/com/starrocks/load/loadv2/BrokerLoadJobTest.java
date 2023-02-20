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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/loadv2/BrokerLoadJobTest.java

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

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.LabelName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.BrokerFileGroupAggInfo;
import com.starrocks.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.EtlStatus;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.task.LeaderTask;
import com.starrocks.task.LeaderTaskExecutor;
import com.starrocks.task.PriorityLeaderTask;
import com.starrocks.task.PriorityLeaderTaskExecutor;
import com.starrocks.transaction.TransactionState;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BrokerLoadJobTest {

    @BeforeClass
    public static void start() {
        MetricRepo.init();
    }

    @Test
    public void testFromLoadStmt(@Injectable LoadStmt loadStmt,
                                 @Injectable LabelName labelName,
                                 @Injectable DataDescription dataDescription,
                                 @Mocked GlobalStateMgr globalStateMgr,
                                 @Injectable Database database) {
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);

        String tableName = "table";
        String databaseName = "database";
        new Expectations() {
            {
                loadStmt.getLabel();
                minTimes = 0;
                result = labelName;
                labelName.getDbName();
                minTimes = 0;
                result = databaseName;
                globalStateMgr.getDb(databaseName);
                minTimes = 0;
                result = database;
                loadStmt.getDataDescriptions();
                minTimes = 0;
                result = dataDescriptionList;
                dataDescription.getTableName();
                minTimes = 0;
                result = tableName;
                database.getTable(tableName);
                minTimes = 0;
                result = null;
            }
        };

        try {
            BulkLoadJob.fromLoadStmt(loadStmt, null);
            Assert.fail();
        } catch (DdlException e) {
            System.out.println("could not find table named " + tableName);
        }

    }

    @Test
    public void testFromLoadStmt2(@Injectable LoadStmt loadStmt,
                                  @Injectable DataDescription dataDescription,
                                  @Injectable LabelName labelName,
                                  @Injectable Database database,
                                  @Injectable OlapTable olapTable,
                                  @Mocked GlobalStateMgr globalStateMgr) {

        String label = "label";
        long dbId = 1;
        String tableName = "table";
        String databaseName = "database";
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);
        BrokerDesc brokerDesc = new BrokerDesc("broker0", Maps.newHashMap());

        new Expectations() {
            {
                loadStmt.getLabel();
                minTimes = 0;
                result = labelName;
                labelName.getDbName();
                minTimes = 0;
                result = databaseName;
                labelName.getLabelName();
                minTimes = 0;
                result = label;
                globalStateMgr.getDb(databaseName);
                minTimes = 0;
                result = database;
                loadStmt.getDataDescriptions();
                minTimes = 0;
                result = dataDescriptionList;
                dataDescription.getTableName();
                minTimes = 0;
                result = tableName;
                database.getTable(tableName);
                minTimes = 0;
                result = olapTable;
                dataDescription.getPartitionNames();
                minTimes = 0;
                result = null;
                database.getId();
                minTimes = 0;
                result = dbId;
                loadStmt.getBrokerDesc();
                minTimes = 0;
                result = brokerDesc;
                loadStmt.getEtlJobType();
                minTimes = 0;
                result = EtlJobType.BROKER;
            }
        };

        try {
            BrokerLoadJob brokerLoadJob = (BrokerLoadJob) BulkLoadJob.fromLoadStmt(loadStmt, null);
            Assert.assertEquals(Long.valueOf(dbId), Deencapsulation.getField(brokerLoadJob, "dbId"));
            Assert.assertEquals(label, Deencapsulation.getField(brokerLoadJob, "label"));
            Assert.assertEquals(JobState.PENDING, Deencapsulation.getField(brokerLoadJob, "state"));
            Assert.assertEquals(EtlJobType.BROKER, Deencapsulation.getField(brokerLoadJob, "jobType"));
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testAlterLoad(@Injectable LoadStmt loadStmt,
                              @Injectable AlterLoadStmt alterLoadStmt,
                              @Injectable DataDescription dataDescription,
                              @Injectable LabelName labelName,
                              @Injectable Database database,
                              @Injectable OlapTable olapTable,
                              @Mocked GlobalStateMgr globalStateMgr) {

        String label = "label";
        long dbId = 1;
        String tableName = "table";
        String databaseName = "database";
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);
        BrokerDesc brokerDesc = new BrokerDesc("broker0", Maps.newHashMap());
        Map<String, String> properties = new HashMap<>();
        properties.put(LoadStmt.PRIORITY, "HIGH");

        new Expectations() {
            {
                loadStmt.getLabel();
                minTimes = 0;
                result = labelName;
                labelName.getDbName();
                minTimes = 0;
                result = databaseName;
                labelName.getLabelName();
                minTimes = 0;
                result = label;
                globalStateMgr.getDb(databaseName);
                minTimes = 0;
                result = database;
                loadStmt.getDataDescriptions();
                minTimes = 0;
                result = dataDescriptionList;
                dataDescription.getTableName();
                minTimes = 0;
                result = tableName;
                database.getTable(tableName);
                minTimes = 0;
                result = olapTable;
                dataDescription.getPartitionNames();
                minTimes = 0;
                result = null;
                database.getId();
                minTimes = 0;
                result = dbId;
                loadStmt.getBrokerDesc();
                minTimes = 0;
                result = brokerDesc;
                loadStmt.getEtlJobType();
                minTimes = 0;
                result = EtlJobType.BROKER;
                alterLoadStmt.getAnalyzedJobProperties();
                minTimes = 0;
                result = properties;
            }
        };

        try {
            BrokerLoadJob brokerLoadJob = (BrokerLoadJob) BulkLoadJob.fromLoadStmt(loadStmt, null);
            Assert.assertEquals(Long.valueOf(dbId), Deencapsulation.getField(brokerLoadJob, "dbId"));
            Assert.assertEquals(label, Deencapsulation.getField(brokerLoadJob, "label"));
            Assert.assertEquals(JobState.PENDING, Deencapsulation.getField(brokerLoadJob, "state"));
            Assert.assertEquals(EtlJobType.BROKER, Deencapsulation.getField(brokerLoadJob, "jobType"));
            brokerLoadJob.alterJob(alterLoadStmt);
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testGetTableNames(@Injectable BrokerFileGroupAggInfo fileGroupAggInfo,
                                  @Injectable BrokerFileGroup brokerFileGroup,
                                  @Mocked GlobalStateMgr globalStateMgr,
                                  @Injectable Database database,
                                  @Injectable Table table) throws MetaNotFoundException {
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        brokerFileGroups.add(brokerFileGroup);
        Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        FileGroupAggKey aggKey = new FileGroupAggKey(1L, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "fileGroupAggInfo", fileGroupAggInfo);
        String tableName = "table";
        new Expectations() {
            {
                fileGroupAggInfo.getAggKeyToFileGroups();
                minTimes = 0;
                result = aggKeyToFileGroups;
                fileGroupAggInfo.getAllTableIds();
                minTimes = 0;
                result = Sets.newHashSet(1L);
                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = database;
                database.getTable(1L);
                minTimes = 0;
                result = table;
                table.getName();
                minTimes = 0;
                result = tableName;
            }
        };

        Assert.assertEquals(1, brokerLoadJob.getTableNamesForShow().size());
        Assert.assertEquals(true, brokerLoadJob.getTableNamesForShow().contains(tableName));
    }

    @Test
    public void testExecuteJob(@Mocked LeaderTaskExecutor leaderTaskExecutor) throws LoadException {
        new Expectations() {
            {
                leaderTaskExecutor.submit((LeaderTask) any);
                minTimes = 0;
                result = true;
            }
        };

        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        brokerLoadJob.unprotectedExecuteJob();

        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assert.assertEquals(1, idToTasks.size());
    }

    @Test
    public void testPendingTaskOnFinishedWithJobCancelled(@Injectable BrokerPendingTaskAttachment attachment) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.CANCELLED);
        brokerLoadJob.onTaskFinished(attachment);

        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(0, finishedTaskIds.size());
    }

    @Test
    public void testPendingTaskOnFinishedWithDuplicated(@Injectable BrokerPendingTaskAttachment attachment) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Set<Long> finishedTaskIds = Sets.newHashSet();
        long taskId = 1L;
        finishedTaskIds.add(taskId);
        Deencapsulation.setField(brokerLoadJob, "finishedTaskIds", finishedTaskIds);
        new Expectations() {
            {
                attachment.getTaskId();
                minTimes = 0;
                result = taskId;
            }
        };

        brokerLoadJob.onTaskFinished(attachment);
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assert.assertEquals(0, idToTasks.size());
    }

    @Test
    public void testPendingTaskOnFinished(@Injectable BrokerPendingTaskAttachment attachment,
                                          @Mocked GlobalStateMgr globalStateMgr,
                                          @Injectable Database database,
                                          @Injectable BrokerFileGroupAggInfo fileGroupAggInfo,
                                          @Injectable BrokerFileGroup brokerFileGroup1,
                                          @Injectable BrokerFileGroup brokerFileGroup2,
                                          @Injectable BrokerFileGroup brokerFileGroup3,
                                          @Mocked LeaderTaskExecutor leaderTaskExecutor,
                                          @Mocked PriorityLeaderTaskExecutor priorityLeaderTaskExecutor,
                                          @Injectable OlapTable olapTable,
                                          @Mocked LoadingTaskPlanner loadingTaskPlanner) {
        Config.enable_pipeline_load = false;
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        long taskId = 1L;
        long tableId1 = 1L;
        long tableId2 = 2L;
        long partitionId1 = 3L;
        long partitionId2 = 4;

        Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> fileGroups1 = Lists.newArrayList();
        fileGroups1.add(brokerFileGroup1);
        aggKeyToFileGroups.put(new FileGroupAggKey(tableId1, null), fileGroups1);

        List<BrokerFileGroup> fileGroups2 = Lists.newArrayList();
        fileGroups2.add(brokerFileGroup2);
        fileGroups2.add(brokerFileGroup3);
        aggKeyToFileGroups.put(new FileGroupAggKey(tableId2, Lists.newArrayList(partitionId1)), fileGroups2);
        // add another file groups with different partition id
        aggKeyToFileGroups.put(new FileGroupAggKey(tableId2, Lists.newArrayList(partitionId2)), fileGroups2);

        Deencapsulation.setField(brokerLoadJob, "fileGroupAggInfo", fileGroupAggInfo);
        new Expectations() {
            {
                attachment.getTaskId();
                minTimes = 0;
                result = taskId;
                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = database;
                fileGroupAggInfo.getAggKeyToFileGroups();
                minTimes = 0;
                result = aggKeyToFileGroups;
                database.getTable(anyLong);
                minTimes = 0;
                result = olapTable;
                globalStateMgr.getNextId();
                minTimes = 0;
                result = 1L;
                result = 2L;
                result = 3L;
                leaderTaskExecutor.submit((LeaderTask) any);
                minTimes = 0;
                result = true;
                priorityLeaderTaskExecutor.submit((PriorityLeaderTask) any);
                minTimes = 0;
                result = true;
            }
        };

        brokerLoadJob.onTaskFinished(attachment);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(1, finishedTaskIds.size());
        Assert.assertEquals(true, finishedTaskIds.contains(taskId));
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(brokerLoadJob, "idToTasks");
        Assert.assertEquals(3, idToTasks.size());
        Config.enable_pipeline_load = true;
    }

    @Test
    public void testLoadingTaskOnFinishedWithUnfinishedTask(@Injectable BrokerLoadingTaskAttachment attachment,
                                                            @Injectable LoadTask loadTask1,
                                                            @Injectable LoadTask loadTask2) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        idToTasks.put(2L, loadTask2);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 1;
                attachment.getTaskId();
                minTimes = 0;
                result = 1L;
            }
        };

        brokerLoadJob.onTaskFinished(attachment);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(1, finishedTaskIds.size());
        EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
        Assert.assertEquals("10", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
        Assert.assertEquals("1", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
        int progress = Deencapsulation.getField(brokerLoadJob, "progress");
        Assert.assertEquals(50, progress);
    }

    @Test
    public void testLoadingTaskOnFinishedWithErrorNum(@Injectable BrokerLoadingTaskAttachment attachment1,
                                                      @Injectable BrokerLoadingTaskAttachment attachment2,
                                                      @Injectable LoadTask loadTask1,
                                                      @Injectable LoadTask loadTask2,
                                                      @Mocked GlobalStateMgr globalStateMgr) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        idToTasks.put(2L, loadTask2);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment2.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 20;
                attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 1;
                attachment2.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 2;
                attachment1.getTaskId();
                minTimes = 0;
                result = 1L;
                attachment2.getTaskId();
                minTimes = 0;
                result = 2L;
            }
        };

        brokerLoadJob.onTaskFinished(attachment1);
        brokerLoadJob.onTaskFinished(attachment2);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(2, finishedTaskIds.size());
        EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
        Assert.assertEquals("30", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
        Assert.assertEquals("3", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
        int progress = Deencapsulation.getField(brokerLoadJob, "progress");
        Assert.assertEquals(99, progress);
        Assert.assertEquals(JobState.CANCELLED, Deencapsulation.getField(brokerLoadJob, "state"));
    }

    @Test
    public void testLoadingTaskOnFinished(@Injectable BrokerLoadingTaskAttachment attachment1,
                                          @Injectable LoadTask loadTask1,
                                          @Mocked GlobalStateMgr globalStateMgr,
                                          @Injectable Database database) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerLoadJob, "state", JobState.LOADING);
        Map<Long, LoadTask> idToTasks = Maps.newHashMap();
        idToTasks.put(1L, loadTask1);
        Deencapsulation.setField(brokerLoadJob, "idToTasks", idToTasks);
        new Expectations() {
            {
                attachment1.getCounter(BrokerLoadJob.DPP_NORMAL_ALL);
                minTimes = 0;
                result = 10;
                attachment1.getCounter(BrokerLoadJob.DPP_ABNORMAL_ALL);
                minTimes = 0;
                result = 0;
                attachment1.getTaskId();
                minTimes = 0;
                result = 1L;
                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = database;
            }
        };

        brokerLoadJob.onTaskFinished(attachment1);
        Set<Long> finishedTaskIds = Deencapsulation.getField(brokerLoadJob, "finishedTaskIds");
        Assert.assertEquals(1, finishedTaskIds.size());
        EtlStatus loadingStatus = Deencapsulation.getField(brokerLoadJob, "loadingStatus");
        Assert.assertEquals("10", loadingStatus.getCounters().get(BrokerLoadJob.DPP_NORMAL_ALL));
        Assert.assertEquals("0", loadingStatus.getCounters().get(BrokerLoadJob.DPP_ABNORMAL_ALL));
        int progress = Deencapsulation.getField(brokerLoadJob, "progress");
        Assert.assertEquals(99, progress);
    }

    @Test
    public void testExecuteReplayOnAborted(@Injectable TransactionState txnState,
                                           @Injectable LoadJobFinalOperation attachment,
                                           @Injectable EtlStatus etlStatus) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        new Expectations() {
            {
                txnState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                attachment.getLoadingStatus();
                minTimes = 0;
                result = etlStatus;
                attachment.getProgress();
                minTimes = 0;
                result = 99;
                attachment.getFinishTimestamp();
                minTimes = 0;
                result = 1;
                attachment.getJobState();
                minTimes = 0;
                result = JobState.CANCELLED;
            }
        };
        brokerLoadJob.replayTxnAttachment(txnState);
        Assert.assertEquals(99, (int) Deencapsulation.getField(brokerLoadJob, "progress"));
        Assert.assertEquals(1, brokerLoadJob.getFinishTimestamp());
        Assert.assertEquals(JobState.CANCELLED, brokerLoadJob.getState());
    }

    @Test
    public void testExecuteReplayOnVisible(@Injectable TransactionState txnState,
                                           @Injectable LoadJobFinalOperation attachment,
                                           @Injectable EtlStatus etlStatus) {
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob();
        new Expectations() {
            {
                txnState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                attachment.getLoadingStatus();
                minTimes = 0;
                result = etlStatus;
                attachment.getProgress();
                minTimes = 0;
                result = 99;
                attachment.getFinishTimestamp();
                minTimes = 0;
                result = 1;
                attachment.getJobState();
                minTimes = 0;
                result = JobState.LOADING;
            }
        };
        brokerLoadJob.replayTxnAttachment(txnState);
        Assert.assertEquals(99, (int) Deencapsulation.getField(brokerLoadJob, "progress"));
        Assert.assertEquals(1, brokerLoadJob.getFinishTimestamp());
        Assert.assertEquals(JobState.LOADING, brokerLoadJob.getState());
    }
}
