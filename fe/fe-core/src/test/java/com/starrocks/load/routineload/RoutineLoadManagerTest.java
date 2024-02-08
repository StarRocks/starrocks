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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/routineload/RoutineLoadManagerTest.java

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

package com.starrocks.load.routineload;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.RoutineLoadOperation;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.PauseRoutineLoadStmt;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TKafkaRLTaskProgress;
import com.starrocks.thrift.TLoadSourceType;
import com.starrocks.thrift.TRLTaskTxnCommitAttachment;
import com.starrocks.thrift.TResourceInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TxnCommitAttachment;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RoutineLoadManagerTest {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadManagerTest.class);

    @Mocked
    private SystemInfoService systemInfoService;

    @Before
    public void setUp() {
        UtFrameUtils.setUpForPersistTest();
    }

    @After
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAddJobByStmt(@Injectable TResourceInfo tResourceInfo,
                                 @Mocked ConnectContext connectContext,
                                 @Mocked GlobalStateMgr globalStateMgr) throws UserException {
        String jobName = "job1";
        String dbName = "db1";
        LabelName labelName = new LabelName(dbName, jobName);
        String tableNameString = "table1";
        TableName tableName = new TableName(dbName, tableNameString);
        List<ParseNode> loadPropertyList = new ArrayList<>();
        ColumnSeparator columnSeparator = new ColumnSeparator(",");
        loadPropertyList.add(columnSeparator);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();
        String topicName = "topic1";
        customProperties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, topicName);
        String serverAddress = "http://127.0.0.1:8080";
        customProperties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, serverAddress);
        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                loadPropertyList, properties,
                typeName, customProperties);
        createRoutineLoadStmt.setOrigStmt(new OriginStatement("dummy", 0));

        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, jobName, 1L, 1L,
                serverAddress, topicName);

        new MockUp<KafkaRoutineLoadJob>() {
            @Mock
            public KafkaRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) {
                return kafkaRoutineLoadJob;
            }
        };

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        routineLoadManager.createRoutineLoadJob(createRoutineLoadStmt);

        Map<String, RoutineLoadJob> idToRoutineLoadJob =
                Deencapsulation.getField(routineLoadManager, "idToRoutineLoadJob");
        Assert.assertEquals(1, idToRoutineLoadJob.size());
        RoutineLoadJob routineLoadJob = idToRoutineLoadJob.values().iterator().next();
        Assert.assertEquals(1L, routineLoadJob.getDbId());
        Assert.assertEquals(jobName, routineLoadJob.getName());
        Assert.assertEquals(1L, routineLoadJob.getTableId());
        Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
        Assert.assertEquals(true, routineLoadJob instanceof KafkaRoutineLoadJob);

        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob =
                Deencapsulation.getField(routineLoadManager, "dbToNameToRoutineLoadJob");
        Assert.assertEquals(1, dbToNameToRoutineLoadJob.size());
        Assert.assertEquals(Long.valueOf(1L), dbToNameToRoutineLoadJob.keySet().iterator().next());
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = dbToNameToRoutineLoadJob.get(1L);
        Assert.assertEquals(jobName, nameToRoutineLoadJob.keySet().iterator().next());
        Assert.assertEquals(1, nameToRoutineLoadJob.values().size());
        Assert.assertEquals(routineLoadJob, nameToRoutineLoadJob.values().iterator().next().get(0));
    }

    @Test
    public void testCreateJobAuthDeny(@Injectable TResourceInfo tResourceInfo,
                                      @Mocked ConnectContext connectContext,
                                      @Mocked GlobalStateMgr globalStateMgr) {
        String jobName = "job1";
        String dbName = "db1";
        LabelName labelName = new LabelName(dbName, jobName);
        String tableNameString = "table1";
        TableName tableName = new TableName(dbName, tableNameString);
        List<ParseNode> loadPropertyList = new ArrayList<>();
        ColumnSeparator columnSeparator = new ColumnSeparator(",");
        loadPropertyList.add(columnSeparator);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();
        String topicName = "topic1";
        customProperties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, topicName);
        String serverAddress = "http://127.0.0.1:8080";
        customProperties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, serverAddress);
        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                loadPropertyList, properties,
                typeName, customProperties);
        createRoutineLoadStmt.setOrigStmt(new OriginStatement("dummy", 0));

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        try {
            routineLoadManager.createRoutineLoadJob(createRoutineLoadStmt);
            Assert.fail();
        } catch (LoadException | DdlException e) {
            Assert.fail();
        } catch (AnalysisException e) {
            LOG.info("Access deny");
        } catch (UserException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateWithSameName(@Mocked ConnectContext connectContext) {
        String jobName = "job1";
        String topicName = "topic1";
        String serverAddress = "http://127.0.0.1:8080";
        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, jobName, 1L, 1L,
                serverAddress, topicName);

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();

        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newConcurrentMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newConcurrentMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        KafkaRoutineLoadJob kafkaRoutineLoadJobWithSameName = new KafkaRoutineLoadJob(1L, jobName,
                1L, 1L, serverAddress, topicName);
        routineLoadJobList.add(kafkaRoutineLoadJobWithSameName);
        nameToRoutineLoadJob.put(jobName, routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);

        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);
        try {
            routineLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob, "db");
            Assert.fail();
        } catch (DdlException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testCreateWithSameNameOfStoppedJob(@Mocked ConnectContext connectContext,
                                                   @Mocked GlobalStateMgr globalStateMgr,
                                                   @Mocked EditLog editLog) throws DdlException {
        String jobName = "job1";
        String topicName = "topic1";
        String serverAddress = "http://127.0.0.1:8080";
        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, jobName, 1L, 1L,
                serverAddress, topicName);

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();

        new Expectations() {
            {
                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newConcurrentMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newConcurrentMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        KafkaRoutineLoadJob kafkaRoutineLoadJobWithSameName = new KafkaRoutineLoadJob(1L, jobName,
                1L, 1L, serverAddress, topicName);
        Deencapsulation.setField(kafkaRoutineLoadJobWithSameName, "state", RoutineLoadJob.JobState.STOPPED);
        routineLoadJobList.add(kafkaRoutineLoadJobWithSameName);
        nameToRoutineLoadJob.put(jobName, routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Map<String, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
        idToRoutineLoadJob.put(UUID.randomUUID().toString(), kafkaRoutineLoadJobWithSameName);

        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        routineLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob, "db");

        Map<Long, Map<String, List<RoutineLoadJob>>> result =
                Deencapsulation.getField(routineLoadManager, "dbToNameToRoutineLoadJob");
        Map<String, RoutineLoadJob> result1 = Deencapsulation.getField(routineLoadManager, "idToRoutineLoadJob");
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(Long.valueOf(1L), result.keySet().iterator().next());
        Map<String, List<RoutineLoadJob>> resultNameToRoutineLoadJob = result.get(1L);
        Assert.assertEquals(jobName, resultNameToRoutineLoadJob.keySet().iterator().next());
        Assert.assertEquals(2, resultNameToRoutineLoadJob.values().iterator().next().size());
        Assert.assertEquals(2, result1.values().size());
    }

    @Test
    public void testGetTotalIdleTaskNum() {
        List<Long> beIds = Lists.newArrayList(1L, 2L);

        new Expectations() {
            {
                systemInfoService.getBackendIds(true);
                minTimes = 0;
                result = beIds;
            }
        };

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        routineLoadManager.updateBeTaskSlot();
        routineLoadManager.takeBeTaskSlot();

        Assert.assertEquals(Config.max_routine_load_task_num_per_be * 2 - 1,
                routineLoadManager.getClusterIdleSlotNum());
    }

    @Test
    public void testTakeBeTaskSlot() throws Exception {
        List<Long> beIds = Lists.newArrayList(1L, 2L);

        new Expectations() {
            {
                systemInfoService.getBackendIds(true);
                minTimes = 0;
                result = beIds;
            }
        };

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        routineLoadManager.updateBeTaskSlot();

        // take tow slots
        long beId1 = routineLoadManager.takeBeTaskSlot();
        long beId2 = routineLoadManager.takeBeTaskSlot();
        Assert.assertTrue(beId1 != beId2);
        Assert.assertTrue(beId1 != -1L);
        Assert.assertTrue(beId2 != -1L);

        // take all slots
        ExecutorService es = Executors.newCachedThreadPool();
        for (int i = 0; i < (2 * Config.max_routine_load_task_num_per_be) - 2; i++) {
            es.submit(() -> Assert.assertTrue(routineLoadManager.takeBeTaskSlot() > 0));
        }

        es.shutdown();
        es.awaitTermination(1, TimeUnit.HOURS);
        Assert.assertEquals(-1L, routineLoadManager.takeBeTaskSlot());
        Assert.assertEquals(-1L, routineLoadManager.takeBeTaskSlot(1L));
        Assert.assertEquals(-1L, routineLoadManager.takeBeTaskSlot(2L));

        // release all slots
        ExecutorService es2 = Executors.newCachedThreadPool();
        for (int i = 0; i < Config.max_routine_load_task_num_per_be; i++) {
            es2.submit(() -> {
                routineLoadManager.releaseBeTaskSlot(1L);
                routineLoadManager.releaseBeTaskSlot(2L);
            });
        }
        es2.shutdown();
        es2.awaitTermination(1, TimeUnit.HOURS);
        Assert.assertEquals(2 * Config.max_routine_load_task_num_per_be, routineLoadManager.getClusterIdleSlotNum());
    }

    @Test
    public void testGetJobByName(@Injectable RoutineLoadJob routineLoadJob1,
                                 @Injectable RoutineLoadJob routineLoadJob2,
                                 @Injectable RoutineLoadJob routineLoadJob3) {
        String jobName = "ilovestarrocks";
        List<RoutineLoadJob> routineLoadJobList1 = Lists.newArrayList();
        routineLoadJobList1.add(routineLoadJob1);
        routineLoadJobList1.add(routineLoadJob2);
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadList1 = Maps.newHashMap();
        nameToRoutineLoadList1.put(jobName, routineLoadJobList1);

        List<RoutineLoadJob> routineLoadJobList2 = Lists.newArrayList();
        routineLoadJobList2.add(routineLoadJob3);
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadList2 = Maps.newHashMap();
        nameToRoutineLoadList2.put(jobName, routineLoadJobList2);

        Map<String, Map<String, List<RoutineLoadJob>>> dbToNameRoutineLoadList = Maps.newHashMap();
        dbToNameRoutineLoadList.put("db1", nameToRoutineLoadList1);
        dbToNameRoutineLoadList.put("db2", nameToRoutineLoadList2);

        new Expectations() {
            {
                routineLoadJob1.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob2.isFinal();
                minTimes = 0;
                result = false;
            }
        };

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameRoutineLoadList);
        List<RoutineLoadJob> result = routineLoadManager.getJobByName(jobName);

        Assert.assertEquals(3, result.size());
        Assert.assertEquals(routineLoadJob2, result.get(0));
        Assert.assertEquals(routineLoadJob1, result.get(1));
        Assert.assertEquals(routineLoadJob3, result.get(2));

    }

    @Test
    public void testSetRoutineLoadJobOtherMsg() {
        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "test", 1L, 1L, "host:port",
                "topic");
        Map<Long, Integer> beTasksNum = routineLoadManager.getBeTasksNum();
        beTasksNum.put(1L, 0);
        try {
            routineLoadManager.addRoutineLoadJob(routineLoadJob, "db");
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }

        {
            TxnCommitAttachment txnCommitAttachment = new InsertTxnCommitAttachment();
            routineLoadManager.setRoutineLoadJobOtherMsg("foo", txnCommitAttachment);
        }

        {
            RLTaskTxnCommitAttachment txnCommitAttachment = new RLTaskTxnCommitAttachment();
            routineLoadManager.setRoutineLoadJobOtherMsg("foo", txnCommitAttachment);
        }

        {

            TRLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = new TRLTaskTxnCommitAttachment();
            rlTaskTxnCommitAttachment.setId(new TUniqueId());
            rlTaskTxnCommitAttachment.setLoadedRows(100);
            rlTaskTxnCommitAttachment.setFilteredRows(1);
            rlTaskTxnCommitAttachment.setJobId(Deencapsulation.getField(routineLoadJob, "id"));
            rlTaskTxnCommitAttachment.setLoadSourceType(TLoadSourceType.KAFKA);
            TKafkaRLTaskProgress tKafkaRLTaskProgress = new TKafkaRLTaskProgress();
            Map<Integer, Long> kafkaProgress = Maps.newHashMap();
            kafkaProgress.put(1, 100L); // start from 0, so rows number is 101, and consumed offset is 100
            tKafkaRLTaskProgress.setPartitionCmtOffset(kafkaProgress);
            rlTaskTxnCommitAttachment.setKafkaRLTaskProgress(tKafkaRLTaskProgress);
            TxnCommitAttachment txnCommitAttachment = new RLTaskTxnCommitAttachment(rlTaskTxnCommitAttachment);
            routineLoadManager.setRoutineLoadJobOtherMsg("foo abc", txnCommitAttachment);
            Assert.assertEquals(true, routineLoadJob.getOtherMsg().contains("foo abc"));
        }
    }

    @Test
    public void testGetJob(@Injectable RoutineLoadJob routineLoadJob1,
                           @Injectable RoutineLoadJob routineLoadJob2,
                           @Injectable RoutineLoadJob routineLoadJob3) throws MetaNotFoundException {

        new Expectations() {
            {
                routineLoadJob1.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob2.isFinal();
                minTimes = 0;
                result = false;
                routineLoadJob3.isFinal();
                minTimes = 0;
                result = true;
            }
        };

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newHashMap();
        idToRoutineLoadJob.put(1L, routineLoadJob1);
        idToRoutineLoadJob.put(2L, routineLoadJob2);
        idToRoutineLoadJob.put(3L, routineLoadJob3);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        List<RoutineLoadJob> result = routineLoadManager.getJob(null, null, true);

        Assert.assertEquals(3, result.size());
        Assert.assertEquals(routineLoadJob2, result.get(0));
        Assert.assertEquals(routineLoadJob1, result.get(1));
        Assert.assertEquals(routineLoadJob3, result.get(2));
    }

    @Test
    public void testGetJobIncludeHistory(@Injectable RoutineLoadJob routineLoadJob1,
                                         @Injectable RoutineLoadJob routineLoadJob2,
                                         @Injectable RoutineLoadJob routineLoadJob3,
                                         @Mocked GlobalStateMgr globalStateMgr,
                                         @Mocked Database database) throws MetaNotFoundException {
        new Expectations() {
            {
                routineLoadJob1.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob2.isFinal();
                minTimes = 0;
                result = false;
                routineLoadJob3.isFinal();
                minTimes = 0;
                result = true;
                globalStateMgr.getDb(anyString);
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        routineLoadJobList.add(routineLoadJob1);
        routineLoadJobList.add(routineLoadJob2);
        routineLoadJobList.add(routineLoadJob3);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);
        List<RoutineLoadJob> result = routineLoadManager.getJob("", "", true);

        Assert.assertEquals(3, result.size());
        Assert.assertEquals(routineLoadJob2, result.get(0));
        Assert.assertEquals(routineLoadJob1, result.get(1));
        Assert.assertEquals(routineLoadJob3, result.get(2));
    }

    @Test
    public void testPauseRoutineLoadJob(@Injectable PauseRoutineLoadStmt pauseRoutineLoadStmt,
                                        @Mocked GlobalStateMgr globalStateMgr,
                                        @Mocked Database database,
                                        @Mocked ConnectContext connectContext) throws UserException {
        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJobList.add(routineLoadJob);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
        idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);

        new Expectations() {
            {
                pauseRoutineLoadStmt.getDbFullName();
                minTimes = 0;
                result = "";
                pauseRoutineLoadStmt.getName();
                minTimes = 0;
                result = "";
                globalStateMgr.getDb("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        routineLoadManager.pauseRoutineLoadJob(pauseRoutineLoadStmt);

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());

        for (int i = 0; i < 3; i++) {
            Deencapsulation.setField(routineLoadJob, "pauseReason",
                    new ErrorReason(InternalErrorCode.REPLICA_FEW_ERR, ""));
            routineLoadManager.updateRoutineLoadJob();
            Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
            Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
            boolean autoResumeLock = Deencapsulation.getField(routineLoadJob, "autoResumeLock");
            Assert.assertEquals(autoResumeLock, false);
        }
        routineLoadManager.updateRoutineLoadJob();
        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
        boolean autoResumeLock = Deencapsulation.getField(routineLoadJob, "autoResumeLock");
        Assert.assertEquals(autoResumeLock, true);
    }

    @Test
    public void testResumeRoutineLoadJob(@Injectable ResumeRoutineLoadStmt resumeRoutineLoadStmt,
                                         @Mocked GlobalStateMgr globalStateMgr,
                                         @Mocked Database database,
                                         @Mocked ConnectContext connectContext) throws UserException {
        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJobList.add(routineLoadJob);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

        new Expectations() {
            {
                resumeRoutineLoadStmt.getDbFullName();
                minTimes = 0;
                result = "";
                resumeRoutineLoadStmt.getName();
                minTimes = 0;
                result = "";
                globalStateMgr.getDb("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        routineLoadManager.resumeRoutineLoadJob(resumeRoutineLoadStmt);

        Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
    }

    @Test
    public void testStopRoutineLoadJob(@Injectable StopRoutineLoadStmt stopRoutineLoadStmt,
                                       @Mocked GlobalStateMgr globalStateMgr,
                                       @Mocked Database database,
                                       @Mocked ConnectContext connectContext) throws UserException {
        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJobList.add(routineLoadJob);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

        new Expectations() {
            {
                stopRoutineLoadStmt.getDbFullName();
                minTimes = 0;
                result = "";
                stopRoutineLoadStmt.getName();
                minTimes = 0;
                result = "";
                globalStateMgr.getDb("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        routineLoadManager.stopRoutineLoadJob(stopRoutineLoadStmt);

        Assert.assertEquals(RoutineLoadJob.JobState.STOPPED, routineLoadJob.getState());
    }

    @Test
    public void testCleanOldRoutineLoadJobs(@Injectable RoutineLoadJob routineLoadJob,
                                            @Mocked GlobalStateMgr globalStateMgr,
                                            @Mocked EditLog editLog) {
        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        routineLoadJobList.add(routineLoadJob);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newHashMap();
        idToRoutineLoadJob.put(1L, routineLoadJob);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

        new Expectations() {
            {
                routineLoadJob.needRemove();
                minTimes = 0;
                result = true;
                routineLoadJob.getDbId();
                minTimes = 0;
                result = 1L;
                routineLoadJob.getName();
                minTimes = 0;
                result = "";
                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
        routineLoadManager.cleanOldRoutineLoadJobs();

        Assert.assertEquals(0, dbToNameToRoutineLoadJob.size());
        Assert.assertEquals(0, idToRoutineLoadJob.size());
    }

    @Test
    public void testReplayRemoveOldRoutineLoad(@Injectable RoutineLoadOperation operation,
                                               @Injectable RoutineLoadJob routineLoadJob) {
        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newHashMap();
        idToRoutineLoadJob.put(1L, routineLoadJob);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        routineLoadJobList.add(routineLoadJob);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

        new Expectations() {
            {
                routineLoadJob.getName();
                minTimes = 0;
                result = "";
                routineLoadJob.getDbId();
                minTimes = 0;
                result = 1L;
                operation.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        routineLoadManager.replayRemoveOldRoutineLoad(operation);
        Assert.assertEquals(0, idToRoutineLoadJob.size());
    }

    @Test
    public void testReplayChangeRoutineLoadJob(@Injectable RoutineLoadOperation operation) {
        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "name", "");
        Deencapsulation.setField(routineLoadJob, "dbId", 1L);
        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newHashMap();
        idToRoutineLoadJob.put(1L, routineLoadJob);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        routineLoadJobList.add(routineLoadJob);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

        new Expectations() {
            {
                operation.getId();
                minTimes = 0;
                result = 1L;
                operation.getJobState();
                minTimes = 0;
                result = RoutineLoadJob.JobState.PAUSED;
            }
        };

        routineLoadManager.replayChangeRoutineLoadJob(operation);
        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
    }

    @Test
    public void testAlterRoutineLoadJob(@Injectable StopRoutineLoadStmt stopRoutineLoadStmt,
                                        @Mocked GlobalStateMgr globalStateMgr,
                                        @Mocked Database database,
                                        @Mocked ConnectContext connectContext) throws UserException {
        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJobList.add(routineLoadJob);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

        new Expectations() {
            {
                stopRoutineLoadStmt.getDbFullName();
                minTimes = 0;
                result = "";
                stopRoutineLoadStmt.getName();
                minTimes = 0;
                result = "";
                globalStateMgr.getDb("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        routineLoadManager.stopRoutineLoadJob(stopRoutineLoadStmt);

        Assert.assertEquals(RoutineLoadJob.JobState.STOPPED, routineLoadJob.getState());
    }


    @Test
    public void testLoadImageWithoutExpiredJob() throws Exception {

        Config.label_keep_max_second = 10;
        Config.enable_dict_optimize_routine_load = true;
        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.setThreadLocalInfo();
        String db = "test";
        String createSQL = "CREATE ROUTINE LOAD db0.routine_load_0 ON t1 " +
                "PROPERTIES(\"format\" = \"json\",\"jsonpaths\"=\"[\\\"$.k1\\\",\\\"$.k2.\\\\\\\"k2.1\\\\\\\"\\\"]\") " +
                "FROM KAFKA(\"kafka_broker_list\" = \"xxx.xxx.xxx.xxx:xxx\",\"kafka_topic\" = \"topic_0\");";

        RoutineLoadMgr leaderLoadManager = new RoutineLoadMgr();
        long now = System.currentTimeMillis();

        // 1. create a job that will be discard after image load
        long discardJobId = 1L;
        RoutineLoadJob discardJob = new KafkaRoutineLoadJob(discardJobId, "discardJob",
                1, 1, "xxx", "xxtopic");
        discardJob.setOrigStmt(new OriginStatement(createSQL, 0));
        leaderLoadManager.addRoutineLoadJob(discardJob, db);
        discardJob.updateState(RoutineLoadJob.JobState.CANCELLED,
                new ErrorReason(InternalErrorCode.CREATE_TASKS_ERR, "fake"), false);
        discardJob.endTimestamp = now - Config.label_keep_max_second * 2 * 1000L;

        // 2. create a new job that will keep for a while
        long goodJobId = 2L;
        RoutineLoadJob goodJob = new KafkaRoutineLoadJob(goodJobId, "goodJob",
                1, 1, "xxx", "xxtopic");
        goodJob.setOrigStmt(new OriginStatement(createSQL, 0));
        leaderLoadManager.addRoutineLoadJob(goodJob, db);
        goodJob.updateState(RoutineLoadJob.JobState.CANCELLED,
                new ErrorReason(InternalErrorCode.CREATE_TASKS_ERR, "fake"), false);
        Assert.assertNotNull(leaderLoadManager.getJob(discardJobId));
        Assert.assertNotNull(leaderLoadManager.getJob(goodJobId));

        // 3. save image & reload
        UtFrameUtils.PseudoImage pseudoImage = new UtFrameUtils.PseudoImage();
        leaderLoadManager.write(pseudoImage.getDataOutputStream());
        RoutineLoadMgr restartedRoutineLoadManager = new RoutineLoadMgr();
        restartedRoutineLoadManager.readFields(pseudoImage.getDataInputStream());
        // discard expired job
        Assert.assertNull(restartedRoutineLoadManager.getJob(discardJobId));
        Assert.assertNotNull(restartedRoutineLoadManager.getJob(goodJobId));

        // 4. clean expire
        leaderLoadManager.cleanOldRoutineLoadJobs();
        // discard expired job
        Assert.assertNull(leaderLoadManager.getJob(discardJobId));
        Assert.assertNotNull(leaderLoadManager.getJob(goodJobId));

    }

    @Test
    public void testJsonFormatImage() throws Exception {

        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.setThreadLocalInfo();
        String db = "test";
        String createSQL = "CREATE ROUTINE LOAD db0.routine_load_0 ON t1 " +
                "PROPERTIES(\"format\" = \"json\",\"jsonpaths\"=\"[\\\"$.k1\\\",\\\"$.k2.\\\\\\\"k2.1\\\\\\\"\\\"]\") " +
                "FROM KAFKA(\"kafka_broker_list\" = \"xxx.xxx.xxx.xxx:xxx\",\"kafka_topic\" = \"topic_0\");";

        RoutineLoadMgr leaderLoadManager = new RoutineLoadMgr();

        RoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, "kafkaJob",
                1, 1, "xxx", "xxtopic");
        kafkaRoutineLoadJob.setOrigStmt(new OriginStatement(createSQL, 0));
        leaderLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob, db);

        createSQL = "CREATE ROUTINE LOAD db0.routine_load_2 ON t1 " +
                "PROPERTIES(\"format\" = \"json\",\"jsonpaths\"=\"[\\\"$.k1\\\",\\\"$.k2.\\\\\\\"k2.1\\\\\\\"\\\"]\") " +
                "FROM PULSAR(\"pulsar_service_url\" = \"xxx.xxx.xxx.xxx:xxx\",\"pulsar_topic\" = \"topic_0\");";
        RoutineLoadJob pulsarRoutineLoadJob = new PulsarRoutineLoadJob(2L, "pulsarJob", 1, 1, "xxxx", "xxx", "xxx");
        pulsarRoutineLoadJob.setOrigStmt(new OriginStatement(createSQL, 0));
        leaderLoadManager.addRoutineLoadJob(pulsarRoutineLoadJob, db);

        UtFrameUtils.PseudoImage pseudoImage = new UtFrameUtils.PseudoImage();
        leaderLoadManager.saveRoutineLoadJobsV2(pseudoImage.getDataOutputStream());
        RoutineLoadMgr restartedRoutineLoadManager = new RoutineLoadMgr();
        SRMetaBlockReader reader = new SRMetaBlockReader(pseudoImage.getDataInputStream());
        restartedRoutineLoadManager.loadRoutineLoadJobsV2(reader);
        reader.close();

        Assert.assertNotNull(restartedRoutineLoadManager.getJob(1L));
        Assert.assertNotNull(restartedRoutineLoadManager.getJob(2L));
    }
}
