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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/routineload/KafkaRoutineLoadJobTest.java

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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.KafkaUtil;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TResourceInfo;
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KafkaRoutineLoadJobTest {
    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJobTest.class);

    private String jobName = "job1";
    private String dbName = "db1";
    private LabelName labelName = new LabelName(dbName, jobName);
    private String tableNameString = "table1";
    private String topicName = "topic1";
    private String serverAddress = "http://127.0.0.1:8080";
    private String kafkaPartitionString = "1,2,3";

    private PartitionNames partitionNames;

    private ColumnSeparator columnSeparator = new ColumnSeparator(",");

    @Mocked
    ConnectContext connectContext;
    @Mocked
    TResourceInfo tResourceInfo;

    @Before
    public void init() {
        List<String> partitionNameList = Lists.newArrayList();
        partitionNameList.add("p1");
        partitionNames = new PartitionNames(false, partitionNameList);
    }

    @Test
    public void testBeNumMin(@Mocked GlobalStateMgr globalStateMgr,
                             @Mocked SystemInfoService systemInfoService,
                             @Mocked Database database,
                             @Mocked RoutineLoadDesc routineLoadDesc) throws MetaNotFoundException {
        List<Integer> partitionList1 = Lists.newArrayList(1, 2);
        List<Integer> partitionList2 = Lists.newArrayList(1, 2, 3);
        List<Integer> partitionList3 = Lists.newArrayList(1, 2, 3, 4);
        List<Integer> partitionList4 = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7);
        List<Long> beIds1 = Lists.newArrayList(1L);
        List<Long> beIds2 = Lists.newArrayList(1L, 2L, 3L, 4L);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
                systemInfoService.getBackendIds(true);
                result = beIds2;
                minTimes = 0;
                systemInfoService.getAliveBackendNumber();
                result = beIds2.size();
                minTimes = 0;
                systemInfoService.getTotalBackendNumber();
                result = beIds2.size();
                minTimes = 0;
            }
        };

        // 3 partitions, 4 be
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList2);
        Assert.assertEquals(3, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 4 partitions, 4 be
        routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList3);
        Assert.assertEquals(4, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 7 partitions, 4 be
        routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList4);
        Assert.assertEquals(4, routineLoadJob.calculateCurrentConcurrentTaskNum());
    }

    @Test 
    public void testShowConfluentSchemaRegistryUrl() {
        KafkaRoutineLoadJob routineLoadJob1 = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        routineLoadJob1.setConfluentSchemaRegistryUrl("http://abc:def@addr.com");
        String sourceString = routineLoadJob1.dataSourcePropertiesJsonToString();
        String expected = "{\"topic\":\"topic1\",\"confluent.schema.registry.url\":\"http://addr.com\"," + 
                                            "\"currentKafkaPartitions\":\"\",\"brokerList\":\"127.0.0.1:9020\"}";
        Assert.assertEquals(expected, sourceString);

        KafkaRoutineLoadJob routineLoadJob2 = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        routineLoadJob2.setConfluentSchemaRegistryUrl("https://abc:def@addr.com");
        sourceString = routineLoadJob2.dataSourcePropertiesJsonToString();
        expected = "{\"topic\":\"topic1\",\"confluent.schema.registry.url\":\"https://addr.com\"," + 
                                    "\"currentKafkaPartitions\":\"\",\"brokerList\":\"127.0.0.1:9020\"}";
        Assert.assertEquals(expected, sourceString);

        KafkaRoutineLoadJob routineLoadJob3 = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        routineLoadJob3.setConfluentSchemaRegistryUrl("https://addr.com");
        sourceString = routineLoadJob3.dataSourcePropertiesJsonToString();
        expected = "{\"topic\":\"topic1\",\"confluent.schema.registry.url\":\"https://addr.com\"," + 
                                    "\"currentKafkaPartitions\":\"\",\"brokerList\":\"127.0.0.1:9020\"}";
        Assert.assertEquals(expected, sourceString);

        KafkaRoutineLoadJob routineLoadJob4 = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        routineLoadJob4.setConfluentSchemaRegistryUrl("http://addr.com");
        sourceString = routineLoadJob4.dataSourcePropertiesJsonToString();
        expected = "{\"topic\":\"topic1\",\"confluent.schema.registry.url\":\"http://addr.com\"," + 
                                    "\"currentKafkaPartitions\":\"\",\"brokerList\":\"127.0.0.1:9020\"}";
        Assert.assertEquals(expected, sourceString);
    }

    @Test
    public void testDivideRoutineLoadJob(@Injectable RoutineLoadMgr routineLoadManager,
                                         @Mocked RoutineLoadDesc routineLoadDesc)
            throws StarRocksException {

        GlobalStateMgr globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);

        RoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                        1L, "127.0.0.1:9020", "topic1");

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getRoutineLoadMgr();
                minTimes = 0;
                result = routineLoadManager;
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.setField(globalStateMgr, "routineLoadTaskScheduler", routineLoadTaskScheduler);

        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", Arrays.asList(1, 4, 6));

        routineLoadJob.divideRoutineLoadJob(2);

        // todo(ml): assert
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList =
                Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Assert.assertEquals(2, routineLoadTaskInfoList.size());
        for (RoutineLoadTaskInfo routineLoadTaskInfo : routineLoadTaskInfoList) {
            KafkaTaskInfo kafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
            Assert.assertEquals(false, kafkaTaskInfo.isRunning());
            if (kafkaTaskInfo.getPartitions().size() == 2) {
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(1));
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(6));
            } else if (kafkaTaskInfo.getPartitions().size() == 1) {
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(4));
            } else {
                Assert.fail();
            }
        }
    }

    @Test
    public void testProcessTimeOutTasks(@Injectable GlobalTransactionMgr globalTransactionMgr,
                                        @Injectable RoutineLoadMgr routineLoadManager) {
        GlobalStateMgr globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);

        RoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                        1L, "127.0.0.1:9020", "topic1");
        long maxBatchIntervalS = 10;
        new Expectations() {
            {
                globalStateMgr.getRoutineLoadMgr();
                minTimes = 0;
                result = routineLoadManager;
            }
        };

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = new ArrayList<>();
        Map<Integer, Long> partitionIdsToOffset = Maps.newHashMap();
        partitionIdsToOffset.put(100, 0L);
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(new UUID(1, 1), routineLoadJob,
                maxBatchIntervalS * 2 * 1000, System.currentTimeMillis(), partitionIdsToOffset,
                routineLoadJob.getTaskTimeoutSecond() * 1000);
        kafkaTaskInfo.setExecuteStartTimeMs(System.currentTimeMillis() - maxBatchIntervalS * 2 * 1000 - 1);
        routineLoadTaskInfoList.add(kafkaTaskInfo);

        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);

        routineLoadJob.processTimeoutTasks();
        new Verifications() {
            {
                List<RoutineLoadTaskInfo> idToRoutineLoadTask =
                        Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
                Assert.assertNotEquals("1", idToRoutineLoadTask.get(0).getId().toString());
                Assert.assertEquals(1, idToRoutineLoadTask.size());
            }
        };
    }

    @Test
    public void testFromCreateStmtWithErrorTable(@Mocked GlobalStateMgr globalStateMgr,
                                                 @Injectable Database database) throws LoadException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, partitionNames);
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), tableNameString);
                minTimes = 0;
                result = null;
            }
        };

        try {
            KafkaRoutineLoadJob kafkaRoutineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
            Assert.fail();
        } catch (StarRocksException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testFromCreateStmt(@Mocked GlobalStateMgr globalStateMgr,
                                   @Injectable Database database,
                                   @Injectable OlapTable table) throws StarRocksException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, partitionNames);
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        List<Pair<Integer, Long>> partitionIdToOffset = Lists.newArrayList();
        for (String s : kafkaPartitionString.split(",")) {
            partitionIdToOffset.add(new Pair<>(Integer.valueOf(s), 0L));
        }
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaPartitionOffsets", partitionIdToOffset);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaBrokerList", serverAddress);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaTopic", topicName);
        long dbId = 1L;
        long tableId = 2L;

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), tableNameString);
                minTimes = 0;
                result = table;
                database.getId();
                minTimes = 0;
                result = dbId;
                table.getId();
                minTimes = 0;
                result = tableId;
                table.isOlapOrCloudNativeTable();
                minTimes = 0;
                result = true;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties) throws
                    StarRocksException {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        KafkaRoutineLoadJob kafkaRoutineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
        Assert.assertEquals(jobName, kafkaRoutineLoadJob.getName());
        Assert.assertEquals(dbId, kafkaRoutineLoadJob.getDbId());
        Assert.assertEquals(tableId, kafkaRoutineLoadJob.getTableId());
        Assert.assertEquals(serverAddress, Deencapsulation.getField(kafkaRoutineLoadJob, "brokerList"));
        Assert.assertEquals(topicName, Deencapsulation.getField(kafkaRoutineLoadJob, "topic"));
        List<Integer> kafkaPartitionResult = Deencapsulation.getField(kafkaRoutineLoadJob, "customKafkaPartitions");
        Assert.assertEquals(kafkaPartitionString, Joiner.on(",").join(kafkaPartitionResult));
    }

    private CreateRoutineLoadStmt initCreateRoutineLoadStmt() {
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(partitionNames);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, topicName);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, serverAddress);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, kafkaPartitionString);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                loadPropertyList, properties,
                typeName, customProperties);
        Deencapsulation.setField(createRoutineLoadStmt, "name", jobName);
        return createRoutineLoadStmt;
    }

    @Test
    public void testSerializationCsv(@Mocked GlobalStateMgr globalStateMgr,
                                     @Injectable Database database,
                                     @Injectable OlapTable table) throws StarRocksException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        Map<String, String> jobProperties = createRoutineLoadStmt.getJobProperties();
        jobProperties.put("format", "csv");
        jobProperties.put("trim_space", "true");
        jobProperties.put("enclose", "'");
        jobProperties.put("escape", "\\");
        jobProperties.put("timezone", "Asia/Shanghai");
        createRoutineLoadStmt.checkJobProperties();

        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, partitionNames);
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        List<Pair<Integer, Long>> partitionIdToOffset = Lists.newArrayList();
        for (String s : kafkaPartitionString.split(",")) {
            partitionIdToOffset.add(new Pair<>(Integer.valueOf(s), 0L));
        }
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaPartitionOffsets", partitionIdToOffset);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaBrokerList", serverAddress);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaTopic", topicName);
        long dbId = 1L;
        long tableId = 2L;

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), tableNameString);
                minTimes = 0;
                result = table;
                database.getId();
                minTimes = 0;
                result = dbId;
                table.getId();
                minTimes = 0;
                result = tableId;
                table.isOlapOrCloudNativeTable();
                minTimes = 0;
                result = true;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties) throws
                    StarRocksException {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        String createSQL = "CREATE ROUTINE LOAD db1.job1 ON table1 " +
                "PROPERTIES('format' = 'csv', 'trim_space' = 'true') " +
                "FROM KAFKA('kafka_broker_list' = 'http://127.0.0.1:8080','kafka_topic' = 'topic1');";
        KafkaRoutineLoadJob job = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
        job.setOrigStmt(new OriginStatement(createSQL, 0));
        Assert.assertEquals("csv", job.getFormat());
        Assert.assertTrue(job.isTrimspace());
        Assert.assertEquals((byte) "'".charAt(0), job.getEnclose());
        Assert.assertEquals((byte) "\\".charAt(0), job.getEscape());

        String data = GsonUtils.GSON.toJson(job, KafkaRoutineLoadJob.class);
        KafkaRoutineLoadJob newJob = GsonUtils.GSON.fromJson(data, KafkaRoutineLoadJob.class);
        Assert.assertEquals("csv", newJob.getFormat());
        Assert.assertTrue(newJob.isTrimspace());
        Assert.assertEquals((byte) "'".charAt(0), newJob.getEnclose());
        Assert.assertEquals((byte) "\\".charAt(0), newJob.getEscape());
    }

    @Test
    public void testSerializationJson(@Mocked GlobalStateMgr globalStateMgr,
                                      @Injectable Database database,
                                      @Injectable OlapTable table) throws StarRocksException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        Map<String, String> jobProperties = createRoutineLoadStmt.getJobProperties();
        jobProperties.put("format", "json");
        jobProperties.put("strip_outer_array", "true");
        jobProperties.put("jsonpaths", "['$.category','$.price','$.author']");
        jobProperties.put("json_root", "");
        jobProperties.put("timezone", "Asia/Shanghai");
        createRoutineLoadStmt.checkJobProperties();

        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, partitionNames);
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        List<Pair<Integer, Long>> partitionIdToOffset = Lists.newArrayList();
        for (String s : kafkaPartitionString.split(",")) {
            partitionIdToOffset.add(new Pair<>(Integer.valueOf(s), 0L));
        }
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaPartitionOffsets", partitionIdToOffset);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaBrokerList", serverAddress);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaTopic", topicName);
        long dbId = 1L;
        long tableId = 2L;

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), tableNameString);
                minTimes = 0;
                result = table;
                database.getId();
                minTimes = 0;
                result = dbId;
                table.getId();
                minTimes = 0;
                result = tableId;
                table.isOlapOrCloudNativeTable();
                minTimes = 0;
                result = true;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties) throws
                    StarRocksException {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        String createSQL = "CREATE ROUTINE LOAD db1.job1 ON table1 " +
                "PROPERTIES('format' = 'json', 'strip_outer_array' = 'true') " +
                "FROM KAFKA('kafka_broker_list' = 'http://127.0.0.1:8080','kafka_topic' = 'topic1');";
        KafkaRoutineLoadJob job = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
        job.setOrigStmt(new OriginStatement(createSQL, 0));
        Assert.assertEquals("json", job.getFormat());
        Assert.assertTrue(job.isStripOuterArray());
        Assert.assertEquals("['$.category','$.price','$.author']", job.getJsonPaths());
        Assert.assertEquals("", job.getJsonRoot());

        String data = GsonUtils.GSON.toJson(job, KafkaRoutineLoadJob.class);
        KafkaRoutineLoadJob newJob = GsonUtils.GSON.fromJson(data, KafkaRoutineLoadJob.class);
        Assert.assertEquals("json", newJob.getFormat());
        Assert.assertTrue(newJob.isStripOuterArray());
        Assert.assertEquals("['$.category','$.price','$.author']", newJob.getJsonPaths());
        Assert.assertEquals("", newJob.getJsonRoot());
    }

    @Test
    public void testGetStatistic() {
        RoutineLoadJob job = new KafkaRoutineLoadJob(1L, "routine_load", 1L, 1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(job, "receivedBytes", 10);
        Deencapsulation.setField(job, "totalRows", 20);
        Deencapsulation.setField(job, "errorRows", 2);
        Deencapsulation.setField(job, "unselectedRows", 2);
        Deencapsulation.setField(job, "totalTaskExcutionTimeMs", 1000);
        String statistic = job.getStatistic();
        Assert.assertTrue(statistic.contains("\"receivedBytesRate\":10"));
        Assert.assertTrue(statistic.contains("\"loadRowsRate\":16"));
    }

    @Test
    public void testCreateStmtWithPauseOnFatalParseError(@Mocked GlobalStateMgr globalStateMgr,
                                                            @Injectable Database database,
                                                            @Injectable OlapTable table) throws StarRocksException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc =
                new RoutineLoadDesc(columnSeparator, null, null, null, partitionNames);
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        List<Pair<Integer, Long>> partitionIdToOffset = Lists.newArrayList();
        for (String s : kafkaPartitionString.split(",")) {
            partitionIdToOffset.add(new Pair<>(Integer.valueOf(s), 0L));
        }
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaPartitionOffsets", partitionIdToOffset);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaBrokerList", serverAddress);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaTopic", topicName);
        long dbId = 1L;
        long tableId = 2L;

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), tableNameString);
                minTimes = 0;
                result = table;
                database.getId();
                minTimes = 0;
                result = dbId;
                table.getId();
                minTimes = 0;
                result = tableId;
                table.isOlapOrCloudNativeTable();
                minTimes = 0;
                result = true;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties) throws
                    StarRocksException {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        {
            KafkaRoutineLoadJob kafkaRoutineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
            Assert.assertFalse(kafkaRoutineLoadJob.isPauseOnFatalParseError());
        }
        {
            Deencapsulation.setField(createRoutineLoadStmt, "pauseOnFatalParseError", true);
            KafkaRoutineLoadJob kafkaRoutineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
            Assert.assertTrue(kafkaRoutineLoadJob.isPauseOnFatalParseError());
        }
    }

    @Test
    public void testGetSourceLagString() {
        RoutineLoadJob job = new KafkaRoutineLoadJob(1L, "routine_load", 1L, 1L, "127.0.0.1:9020", "topic1");
        // check empty value
        String progressJsonStr = null;
        String sourceLagString = job.getSourceLagString(progressJsonStr);
        Assert.assertTrue(sourceLagString.contains("null"));

        progressJsonStr = "{\"0\":\"100\"}";
        Map<Integer, Long> latestPartitionOffsets = null;
        Deencapsulation.setField(job, "latestPartitionOffsets", latestPartitionOffsets);
        sourceLagString = job.getSourceLagString(progressJsonStr);
        Assert.assertTrue(sourceLagString.contains("null"));

        progressJsonStr = "{\"0\":null}";
        latestPartitionOffsets = Maps.newHashMap();
        latestPartitionOffsets.put(0, 200L);
        Deencapsulation.setField(job, "latestPartitionOffsets", latestPartitionOffsets);
        sourceLagString = job.getSourceLagString(progressJsonStr);
        Assert.assertTrue(sourceLagString.contains("{}"));

        progressJsonStr = "{\"0\":\"" + KafkaProgress.OFFSET_ZERO + "\"}";
        sourceLagString = job.getSourceLagString(progressJsonStr);
        Assert.assertTrue(sourceLagString.contains("{}"));

        progressJsonStr = "{\"0\":\"XXX\"}";
        sourceLagString = job.getSourceLagString(progressJsonStr);
        Assert.assertTrue(sourceLagString.contains("{}"));

        progressJsonStr = "{\"0\":\"100\"}";
        latestPartitionOffsets = Maps.newHashMap();
        latestPartitionOffsets.put(0, 200L);
        Deencapsulation.setField(job, "latestPartitionOffsets", latestPartitionOffsets);
        sourceLagString = job.getSourceLagString(progressJsonStr);
        Assert.assertTrue(sourceLagString.contains("\"0\":\"100\""));

        //check  progress > latestPartitionOffsets
        progressJsonStr = "{\"0\":\"200\"}";
        latestPartitionOffsets.put(0, 100L);
        Deencapsulation.setField(job, "latestPartitionOffsets", latestPartitionOffsets);
        sourceLagString = job.getSourceLagString(progressJsonStr);
        Assert.assertTrue(sourceLagString.contains("\"0\":\"0\""));

    }


}
