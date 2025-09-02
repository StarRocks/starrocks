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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.persist.RoutineLoadOperation;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.LabelName;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.PauseRoutineLoadStmt;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.sql.ast.expression.TableName;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RoutineLoadManagerTest {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadManagerTest.class);

    @Mocked
    private SystemInfoService systemInfoService;

    @BeforeEach
    public void setUp() {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAddJobByStmt(@Injectable TResourceInfo tResourceInfo,
                                 @Mocked ConnectContext connectContext,
                                 @Mocked GlobalStateMgr globalStateMgr) throws StarRocksException {
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
        Assertions.assertEquals(1, idToRoutineLoadJob.size());
        RoutineLoadJob routineLoadJob = idToRoutineLoadJob.values().iterator().next();
        Assertions.assertEquals(1L, routineLoadJob.getDbId());
        Assertions.assertEquals(jobName, routineLoadJob.getName());
        Assertions.assertEquals(1L, routineLoadJob.getTableId());
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
        Assertions.assertEquals(true, routineLoadJob instanceof KafkaRoutineLoadJob);

        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob =
                Deencapsulation.getField(routineLoadManager, "dbToNameToRoutineLoadJob");
        Assertions.assertEquals(1, dbToNameToRoutineLoadJob.size());
        Assertions.assertEquals(Long.valueOf(1L), dbToNameToRoutineLoadJob.keySet().iterator().next());
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = dbToNameToRoutineLoadJob.get(1L);
        Assertions.assertEquals(jobName, nameToRoutineLoadJob.keySet().iterator().next());
        Assertions.assertEquals(1, nameToRoutineLoadJob.values().size());
        Assertions.assertEquals(routineLoadJob, nameToRoutineLoadJob.values().iterator().next().get(0));
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
            Assertions.fail();
        } catch (LoadException | DdlException e) {
            Assertions.fail();
        } catch (AnalysisException e) {
            LOG.info("Access deny");
        } catch (StarRocksException e) {
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
            Assertions.fail();
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
        idToRoutineLoadJob.put(UUIDUtil.genUUID().toString(), kafkaRoutineLoadJobWithSameName);

        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        routineLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob, "db");

        Map<Long, Map<String, List<RoutineLoadJob>>> result =
                Deencapsulation.getField(routineLoadManager, "dbToNameToRoutineLoadJob");
        Map<String, RoutineLoadJob> result1 = Deencapsulation.getField(routineLoadManager, "idToRoutineLoadJob");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(Long.valueOf(1L), result.keySet().iterator().next());
        Map<String, List<RoutineLoadJob>> resultNameToRoutineLoadJob = result.get(1L);
        Assertions.assertEquals(jobName, resultNameToRoutineLoadJob.keySet().iterator().next());
        Assertions.assertEquals(2, resultNameToRoutineLoadJob.values().iterator().next().size());
        Assertions.assertEquals(2, result1.values().size());
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
        routineLoadManager.takeBeTaskSlot(WarehouseManager.DEFAULT_WAREHOUSE_ID, 1L);

        Assertions.assertEquals(Config.max_routine_load_task_num_per_be * 2 - 1,
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
        long beId1 = routineLoadManager.takeBeTaskSlot(WarehouseManager.DEFAULT_WAREHOUSE_ID, 1L);
        long beId2 = routineLoadManager.takeBeTaskSlot(WarehouseManager.DEFAULT_WAREHOUSE_ID, 2L);
        Assertions.assertTrue(beId1 != beId2);
        Assertions.assertTrue(beId1 != -1L);
        Assertions.assertTrue(beId2 != -1L);

        // take all slots
        ExecutorService es = Executors.newCachedThreadPool();
        for (int i = 0; i < (2 * Config.max_routine_load_task_num_per_be) - 2; i++) {
            es.submit(() -> Assertions.assertTrue(
                    routineLoadManager.takeBeTaskSlot(WarehouseManager.DEFAULT_WAREHOUSE_ID, 3L) > 0));
        }

        es.shutdown();
        es.awaitTermination(1, TimeUnit.HOURS);
        Assertions.assertEquals(-1L, routineLoadManager.takeBeTaskSlot(WarehouseManager.DEFAULT_WAREHOUSE_ID, 4L));
        Assertions.assertEquals(-1L, routineLoadManager.takeBeTaskSlot(1L, 5L));
        Assertions.assertEquals(-1L, routineLoadManager.takeBeTaskSlot(2L, 6L));

        // release all slots
        ExecutorService es2 = Executors.newCachedThreadPool();
        for (int i = 0; i < Config.max_routine_load_task_num_per_be; i++) {
            es2.submit(() -> {
                routineLoadManager.releaseBeTaskSlot(WarehouseManager.DEFAULT_WAREHOUSE_ID, 1L, 1L);
                routineLoadManager.releaseBeTaskSlot(WarehouseManager.DEFAULT_WAREHOUSE_ID, 2L, 2L);
            });
        }
        es2.shutdown();
        es2.awaitTermination(1, TimeUnit.HOURS);
        Assertions.assertEquals(2 * Config.max_routine_load_task_num_per_be, routineLoadManager.getClusterIdleSlotNum());
    }

    @Test
    public void testTakeBeTaskSlotWithJobs() throws Exception {
        Config.max_routine_load_task_num_per_be = 1000;
        List<Long> beIds = Lists.newArrayList(1L, 2L, 3L, 4L, 5L);

        new Expectations() {
            {
                systemInfoService.getBackendIds(true);
                minTimes = 0;
                result = beIds;
            }
        };

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        routineLoadManager.updateBeTaskSlot();
        
        List<Integer> jobIDs = IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList());
        
        Collections.shuffle(jobIDs);
        for (long jobID : jobIDs) {
            for (long taskId = 0; taskId < 3; taskId++) {
                long beId = routineLoadManager.takeBeTaskSlot(WarehouseManager.DEFAULT_WAREHOUSE_ID, jobID);
            }
        }

        Map<Long, Set<Long>> nodeToJobs = routineLoadManager.getNodeToJobs(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assertions.assertEquals(5, nodeToJobs.size());
        for (long beId : nodeToJobs.keySet()) {
            Assertions.assertEquals(60, nodeToJobs.get(beId).size());
            long total = 0;
            for (long jobId : nodeToJobs.get(beId)) {
                total += jobId;
                routineLoadManager.takeNodeById(WarehouseManager.DEFAULT_WAREHOUSE_ID, jobId, beId);
            }
            LOG.warn("beId: {}, total: {}", beId, total);
        }
        for (long beId : nodeToJobs.keySet()) {
            Assertions.assertEquals(60, nodeToJobs.get(beId).size());
            Assertions.assertEquals(120, routineLoadManager.getNodeTasksNum().get(beId).intValue());
        }
        Config.max_routine_load_task_num_per_be = 16;
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

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(routineLoadJob2, result.get(0));
        Assertions.assertEquals(routineLoadJob1, result.get(1));
        Assertions.assertEquals(routineLoadJob3, result.get(2));

    }

    @Test
    public void testSetRoutineLoadJobOtherMsg() {
        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "test", 1L, 1L, "host:port",
                "topic");
        Map<Long, Integer> beTasksNum = routineLoadManager.getNodeTasksNum();
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
            Assertions.assertEquals(true, routineLoadJob.getOtherMsg().contains("foo abc"));
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

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(routineLoadJob2, result.get(0));
        Assertions.assertEquals(routineLoadJob1, result.get(1));
        Assertions.assertEquals(routineLoadJob3, result.get(2));
    }

    @Test
    public void testGetJobByJobName(@Injectable RoutineLoadJob routineLoadJob1,
                                    @Injectable RoutineLoadJob routineLoadJob2,
                                    @Injectable RoutineLoadJob routineLoadJob3) throws MetaNotFoundException {
        new Expectations() {
            {
                routineLoadJob1.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob1.getName();
                minTimes = 0;
                result = "aaa";
                routineLoadJob2.isFinal();
                minTimes = 0;
                result = false;
                routineLoadJob2.getName();
                minTimes = 0;
                result = "aaa";
                routineLoadJob3.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob3.getName();
                minTimes = 0;
                result = "bbb";
            }
        };

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newHashMap();
        idToRoutineLoadJob.put(1L, routineLoadJob1);
        idToRoutineLoadJob.put(2L, routineLoadJob2);
        idToRoutineLoadJob.put(3L, routineLoadJob3);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        List<RoutineLoadJob> result = routineLoadManager.getJob(null, "aaa", false);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(routineLoadJob2, result.get(0));
    }

    @Test
    public void testGetJobByDb(@Injectable RoutineLoadJob routineLoadJob1,
                               @Injectable RoutineLoadJob routineLoadJob2,
                               @Injectable RoutineLoadJob routineLoadJob3,
                               @Mocked GlobalStateMgr globalStateMgr,
                               @Mocked Database database) throws MetaNotFoundException {
        new Expectations() {
            {
                routineLoadJob1.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob1.getName();
                minTimes = 0;
                result = "aaa";
                routineLoadJob2.isFinal();
                minTimes = 0;
                result = false;
                routineLoadJob2.getName();
                minTimes = 0;
                result = "aaa";
                routineLoadJob3.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob3.getName();
                minTimes = 0;
                result = "bbb";
                globalStateMgr.getLocalMetastore().getDb("db1");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newConcurrentMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newConcurrentMap();
        nameToRoutineLoadJob.put("aaa", Lists.newArrayList(routineLoadJob1, routineLoadJob2));
        nameToRoutineLoadJob.put("bbb", Lists.newArrayList(routineLoadJob3));
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);
        List<RoutineLoadJob> result = routineLoadManager.getJob("db1", null, true);

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(routineLoadJob2, result.get(0));
        Assertions.assertEquals(routineLoadJob1, result.get(1));
        Assertions.assertEquals(routineLoadJob3, result.get(2));
    }

    @Test
    public void testGetJobByDbAndJobName(@Injectable RoutineLoadJob routineLoadJob1,
                                         @Injectable RoutineLoadJob routineLoadJob2,
                                         @Injectable RoutineLoadJob routineLoadJob3,
                                         @Mocked GlobalStateMgr globalStateMgr,
                                         @Mocked Database database) throws MetaNotFoundException {
        new Expectations() {
            {
                routineLoadJob1.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob1.getName();
                minTimes = 0;
                result = "aaa";
                routineLoadJob2.isFinal();
                minTimes = 0;
                result = false;
                routineLoadJob2.getName();
                minTimes = 0;
                result = "aaa";
                routineLoadJob3.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob3.getName();
                minTimes = 0;
                result = "bbb";
                globalStateMgr.getLocalMetastore().getDb("db1");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newConcurrentMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newConcurrentMap();
        nameToRoutineLoadJob.put("aaa", Lists.newArrayList(routineLoadJob1, routineLoadJob2));
        nameToRoutineLoadJob.put("bbb", Lists.newArrayList(routineLoadJob3));
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);
        List<RoutineLoadJob> result = routineLoadManager.getJob("db1", "aaa", true);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(routineLoadJob2, result.get(0));
        Assertions.assertEquals(routineLoadJob1, result.get(1));
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
                globalStateMgr.getLocalMetastore().getDb(anyString);
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

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(routineLoadJob2, result.get(0));
        Assertions.assertEquals(routineLoadJob1, result.get(1));
        Assertions.assertEquals(routineLoadJob3, result.get(2));
    }

    @Test
    public void testPauseRoutineLoadJob(@Injectable PauseRoutineLoadStmt pauseRoutineLoadStmt,
                                        @Mocked GlobalStateMgr globalStateMgr,
                                        @Mocked Database database,
                                        @Mocked ConnectContext connectContext) throws StarRocksException {
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
                globalStateMgr.getLocalMetastore().getDb("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        routineLoadManager.pauseRoutineLoadJob(pauseRoutineLoadStmt);

        Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());

        for (int i = 0; i < 3; i++) {
            Deencapsulation.setField(routineLoadJob, "pauseReason",
                    new ErrorReason(InternalErrorCode.REPLICA_FEW_ERR, ""));
            routineLoadManager.updateRoutineLoadJob();
            Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
            Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
            boolean autoResumeLock = Deencapsulation.getField(routineLoadJob, "autoResumeLock");
            Assertions.assertEquals(autoResumeLock, false);
        }
        routineLoadManager.updateRoutineLoadJob();
        Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
        boolean autoResumeLock = Deencapsulation.getField(routineLoadJob, "autoResumeLock");
        Assertions.assertEquals(autoResumeLock, true);
    }

    @Test
    public void testResumeRoutineLoadJob(@Injectable ResumeRoutineLoadStmt resumeRoutineLoadStmt,
                                         @Mocked GlobalStateMgr globalStateMgr,
                                         @Mocked Database database,
                                         @Mocked ConnectContext connectContext) throws StarRocksException {
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
                globalStateMgr.getLocalMetastore().getDb("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        routineLoadManager.resumeRoutineLoadJob(resumeRoutineLoadStmt);

        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
    }

    @Test
    public void testStopRoutineLoadJob(@Injectable StopRoutineLoadStmt stopRoutineLoadStmt,
                                       @Mocked GlobalStateMgr globalStateMgr,
                                       @Mocked Database database,
                                       @Mocked ConnectContext connectContext) throws StarRocksException {
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
                globalStateMgr.getLocalMetastore().getDb("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        routineLoadManager.stopRoutineLoadJob(stopRoutineLoadStmt);

        Assertions.assertEquals(RoutineLoadJob.JobState.STOPPED, routineLoadJob.getState());
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

        Assertions.assertEquals(0, dbToNameToRoutineLoadJob.size());
        Assertions.assertEquals(0, idToRoutineLoadJob.size());
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
        Assertions.assertEquals(0, idToRoutineLoadJob.size());
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
        Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
    }

    @Test
    public void testAlterRoutineLoadJob(@Injectable StopRoutineLoadStmt stopRoutineLoadStmt,
                                        @Mocked GlobalStateMgr globalStateMgr,
                                        @Mocked Database database,
                                        @Mocked ConnectContext connectContext) throws StarRocksException {
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
                globalStateMgr.getLocalMetastore().getDb("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        routineLoadManager.stopRoutineLoadJob(stopRoutineLoadStmt);

        Assertions.assertEquals(RoutineLoadJob.JobState.STOPPED, routineLoadJob.getState());
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
        discardJob.setOrigStmt(new OriginStatementInfo(createSQL, 0));
        leaderLoadManager.addRoutineLoadJob(discardJob, db);
        discardJob.updateState(RoutineLoadJob.JobState.CANCELLED,
                new ErrorReason(InternalErrorCode.CREATE_TASKS_ERR, "fake"), false);
        discardJob.endTimestamp = now - Config.label_keep_max_second * 2 * 1000L;

        // 2. create a new job that will keep for a while
        long goodJobId = 2L;
        RoutineLoadJob goodJob = new KafkaRoutineLoadJob(goodJobId, "goodJob",
                1, 1, "xxx", "xxtopic");
        goodJob.setOrigStmt(new OriginStatementInfo(createSQL, 0));
        leaderLoadManager.addRoutineLoadJob(goodJob, db);
        goodJob.updateState(RoutineLoadJob.JobState.CANCELLED,
                new ErrorReason(InternalErrorCode.CREATE_TASKS_ERR, "fake"), false);
        Assertions.assertNotNull(leaderLoadManager.getJob(discardJobId));
        Assertions.assertNotNull(leaderLoadManager.getJob(goodJobId));

        // 4. clean expire
        leaderLoadManager.cleanOldRoutineLoadJobs();
        // discard expired job
        Assertions.assertNull(leaderLoadManager.getJob(discardJobId));
        Assertions.assertNotNull(leaderLoadManager.getJob(goodJobId));

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
        kafkaRoutineLoadJob.setOrigStmt(new OriginStatementInfo(createSQL, 0));
        leaderLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob, db);

        createSQL = "CREATE ROUTINE LOAD db0.routine_load_2 ON t1 " +
                "PROPERTIES(\"format\" = \"json\",\"jsonpaths\"=\"[\\\"$.k1\\\",\\\"$.k2.\\\\\\\"k2.1\\\\\\\"\\\"]\") " +
                "FROM PULSAR(\"pulsar_service_url\" = \"xxx.xxx.xxx.xxx:xxx\",\"pulsar_topic\" = \"topic_0\");";
        RoutineLoadJob pulsarRoutineLoadJob = new PulsarRoutineLoadJob(2L, "pulsarJob", 1, 1, "xxxx", "xxx", "xxx");
        pulsarRoutineLoadJob.setOrigStmt(new OriginStatementInfo(createSQL, 0));
        leaderLoadManager.addRoutineLoadJob(pulsarRoutineLoadJob, db);

        UtFrameUtils.PseudoImage pseudoImage = new UtFrameUtils.PseudoImage();
        leaderLoadManager.saveRoutineLoadJobsV2(pseudoImage.getImageWriter());
        RoutineLoadMgr restartedRoutineLoadManager = new RoutineLoadMgr();
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(pseudoImage.getJsonReader());
        restartedRoutineLoadManager.loadRoutineLoadJobsV2(reader);
        reader.close();

        Assertions.assertNotNull(restartedRoutineLoadManager.getJob(1L));
        Assertions.assertNotNull(restartedRoutineLoadManager.getJob(2L));
    }


    @Test
    public void testCheckTaskInJob() throws Exception {

        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, "job1", 1L, 1L, null, "topic1");
        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 100L);
        partitionIdToOffset.put(2, 200L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(new UUID(1, 1), kafkaRoutineLoadJob, 20000,
                System.currentTimeMillis(), partitionIdToOffset, Config.routine_load_task_timeout_second * 1000);
        kafkaRoutineLoadJob.routineLoadTaskInfoList.add(routineLoadTaskInfo);
        RoutineLoadMgr routineLoadMgr = new RoutineLoadMgr();
        routineLoadMgr.addRoutineLoadJob(kafkaRoutineLoadJob, "db");
        boolean taskExist = routineLoadMgr.checkTaskInJob(kafkaRoutineLoadJob.getId(), routineLoadTaskInfo.getId());
        Assertions.assertTrue(taskExist);
        boolean taskNotExist = routineLoadMgr.checkTaskInJob(-1L, routineLoadTaskInfo.getId());
        Assertions.assertFalse(taskNotExist);
    }

    @Test
    public void testGetRunningRoutingLoadCount() throws Exception {
        KafkaRoutineLoadJob job1 = new KafkaRoutineLoadJob(1L, "job1", 1L, 1L, null, "topic1");
        job1.warehouseId = 1;
        job1.state = RoutineLoadJob.JobState.NEED_SCHEDULE;

        KafkaRoutineLoadJob job2 = new KafkaRoutineLoadJob(2L, "job2", 1L, 1L, null, "topic1");
        job2.warehouseId = 1;
        job2.state = RoutineLoadJob.JobState.CANCELLED;


        KafkaRoutineLoadJob job3 = new KafkaRoutineLoadJob(3L, "job3", 1L, 1L, null, "topic1");
        job3.warehouseId = 2;
        job3.state = RoutineLoadJob.JobState.NEED_SCHEDULE;

        KafkaRoutineLoadJob job4 = new KafkaRoutineLoadJob(4L, "job4", 1L, 1L, null, "topic1");
        job4.warehouseId = 2;
        job4.state = RoutineLoadJob.JobState.CANCELLED;

        RoutineLoadMgr routineLoadMgr = new RoutineLoadMgr();
        routineLoadMgr.addRoutineLoadJob(job1, "db");
        routineLoadMgr.addRoutineLoadJob(job2, "db");
        routineLoadMgr.addRoutineLoadJob(job3, "db");
        routineLoadMgr.addRoutineLoadJob(job4, "db");

        Map<Long, Long> result = routineLoadMgr.getRunningRoutingLoadCount();
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(Long.valueOf(1), result.get(1L));
        Assertions.assertEquals(Long.valueOf(1), result.get(2L));
    }
}
