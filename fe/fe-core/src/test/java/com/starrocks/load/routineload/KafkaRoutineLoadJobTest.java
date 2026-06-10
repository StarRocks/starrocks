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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.KafkaUtil;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.metric.RoutineLoadLagTimeMetricMgr;
import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.LabelName;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.RoutineLoadDataSourceProperties;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TResourceInfo;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    private PartitionRef partitionNames;

    private ColumnSeparator columnSeparator = new ColumnSeparator(",");

    @Mocked
    ConnectContext connectContext;
    @Mocked
    TResourceInfo tResourceInfo;

    @BeforeEach
    public void init() {
        List<String> partitionNameList = Lists.newArrayList();
        partitionNameList.add("p1");
        partitionNames = new PartitionRef(partitionNameList, false, NodePosition.ZERO);
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
        Assertions.assertEquals(3, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 4 partitions, 4 be
        routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList3);
        Assertions.assertEquals(4, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 7 partitions, 4 be
        routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList4);
        Assertions.assertEquals(4, routineLoadJob.calculateCurrentConcurrentTaskNum());
    }

    @Test 
    public void testShowConfluentSchemaRegistryUrl() {
        KafkaRoutineLoadJob routineLoadJob1 = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        routineLoadJob1.setConfluentSchemaRegistryUrl("http://abc:def@addr.com");
        String sourceString = routineLoadJob1.dataSourcePropertiesJsonToString();
        String expected = "{\"topic\":\"topic1\",\"confluent.schema.registry.url\":\"http://addr.com\"," + 
                                            "\"currentKafkaPartitions\":\"\",\"brokerList\":\"127.0.0.1:9020\"}";
        Assertions.assertEquals(expected, sourceString);

        KafkaRoutineLoadJob routineLoadJob2 = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        routineLoadJob2.setConfluentSchemaRegistryUrl("https://abc:def@addr.com");
        sourceString = routineLoadJob2.dataSourcePropertiesJsonToString();
        expected = "{\"topic\":\"topic1\",\"confluent.schema.registry.url\":\"https://addr.com\"," + 
                                    "\"currentKafkaPartitions\":\"\",\"brokerList\":\"127.0.0.1:9020\"}";
        Assertions.assertEquals(expected, sourceString);

        KafkaRoutineLoadJob routineLoadJob3 = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        routineLoadJob3.setConfluentSchemaRegistryUrl("https://addr.com");
        sourceString = routineLoadJob3.dataSourcePropertiesJsonToString();
        expected = "{\"topic\":\"topic1\",\"confluent.schema.registry.url\":\"https://addr.com\"," + 
                                    "\"currentKafkaPartitions\":\"\",\"brokerList\":\"127.0.0.1:9020\"}";
        Assertions.assertEquals(expected, sourceString);

        KafkaRoutineLoadJob routineLoadJob4 = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        routineLoadJob4.setConfluentSchemaRegistryUrl("http://addr.com");
        sourceString = routineLoadJob4.dataSourcePropertiesJsonToString();
        expected = "{\"topic\":\"topic1\",\"confluent.schema.registry.url\":\"http://addr.com\"," + 
                                    "\"currentKafkaPartitions\":\"\",\"brokerList\":\"127.0.0.1:9020\"}";
        Assertions.assertEquals(expected, sourceString);
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
        Assertions.assertEquals(2, routineLoadTaskInfoList.size());
        for (RoutineLoadTaskInfo routineLoadTaskInfo : routineLoadTaskInfoList) {
            KafkaTaskInfo kafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
            Assertions.assertEquals(false, kafkaTaskInfo.isRunning());
            if (kafkaTaskInfo.getPartitions().size() == 2) {
                Assertions.assertTrue(kafkaTaskInfo.getPartitions().contains(1));
                Assertions.assertTrue(kafkaTaskInfo.getPartitions().contains(6));
            } else if (kafkaTaskInfo.getPartitions().size() == 1) {
                Assertions.assertTrue(kafkaTaskInfo.getPartitions().contains(4));
            } else {
                Assertions.fail();
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
                Assertions.assertNotEquals("1", idToRoutineLoadTask.get(0).getId().toString());
                Assertions.assertEquals(1, idToRoutineLoadTask.size());
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
            Assertions.fail();
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
        Assertions.assertEquals(jobName, kafkaRoutineLoadJob.getName());
        Assertions.assertEquals(dbId, kafkaRoutineLoadJob.getDbId());
        Assertions.assertEquals(tableId, kafkaRoutineLoadJob.getTableId());
        Assertions.assertEquals(serverAddress, Deencapsulation.getField(kafkaRoutineLoadJob, "brokerList"));
        Assertions.assertEquals(topicName, Deencapsulation.getField(kafkaRoutineLoadJob, "topic"));
        List<Integer> kafkaPartitionResult = Deencapsulation.getField(kafkaRoutineLoadJob, "customKafkaPartitions");
        Assertions.assertEquals(kafkaPartitionString, Joiner.on(",").join(kafkaPartitionResult));
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
    public void testFromCreateStmtWithPartitionDiscovery(@Mocked GlobalStateMgr globalStateMgr,
                                                         @Injectable Database database,
                                                         @Injectable OlapTable table) throws StarRocksException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, partitionNames);
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        List<Pair<Integer, Long>> partitionIdToOffset = Lists.newArrayList();
        partitionIdToOffset.add(new Pair<>(1, 10L));
        partitionIdToOffset.add(new Pair<>(2, 20L));
        partitionIdToOffset.add(new Pair<>(3, 30L));
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaPartitionOffsets", partitionIdToOffset);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaBrokerList", serverAddress);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaTopic", topicName);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaPartitionDiscovery", true);
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
                                                       ImmutableMap<String, String> properties,
                                                       ComputeResource computeResource) {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        KafkaRoutineLoadJob kafkaRoutineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);

        // partitions are seeds for the initial offsets, not a pinned consume list
        List<Integer> pinnedPartitions = Deencapsulation.getField(kafkaRoutineLoadJob, "customKafkaPartitions");
        Assertions.assertTrue(pinnedPartitions.isEmpty(), "kafka_partition_discovery=true should not pin partitions");
        KafkaProgress progress = Deencapsulation.getField(kafkaRoutineLoadJob, "progress");
        for (Pair<Integer, Long> partitionOffset : partitionIdToOffset) {
            Assertions.assertEquals(partitionOffset.second, progress.getOffsetByPartition(partitionOffset.first));
        }
    }

    @Test
    public void testFromCreateStmtWithPartitionDiscoveryInvalidSeedPartition(@Mocked GlobalStateMgr globalStateMgr,
                                                                             @Injectable Database database,
                                                                             @Injectable OlapTable table) {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, partitionNames);
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        List<Pair<Integer, Long>> partitionIdToOffset = Lists.newArrayList();
        partitionIdToOffset.add(new Pair<>(1, 10L));
        partitionIdToOffset.add(new Pair<>(999, 20L));
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaPartitionOffsets", partitionIdToOffset);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaBrokerList", serverAddress);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaTopic", topicName);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaPartitionDiscovery", true);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), tableNameString);
                minTimes = 0;
                result = table;
                database.getId();
                minTimes = 0;
                result = 1L;
                table.getId();
                minTimes = 0;
                result = 2L;
                table.isOlapOrCloudNativeTable();
                minTimes = 0;
                result = true;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties,
                                                       ComputeResource computeResource) {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        // a seeded partition that does not exist in the topic must be rejected at creation time:
        // it is never pinned into customKafkaPartitions, so the prepare()-time check skips it and
        // the typo would otherwise silently discard the seeded offset
        LoadException e = Assertions.assertThrows(LoadException.class,
                () -> KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt));
        Assertions.assertTrue(e.getMessage().contains("999"), e.getMessage());
    }

    @Test
    public void testLaterDiscoveredPartitionDefaultsToBeginning() {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        // the job already has consuming progress; partition 7 shows up afterwards
        KafkaProgress progress = Deencapsulation.getField(routineLoadJob, "progress");
        progress.addPartitionOffset(Pair.create(0, 100L));
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", Lists.newArrayList(0, 7));

        Deencapsulation.invoke(routineLoadJob, "updateNewPartitionProgress");

        Assertions.assertEquals(KafkaProgress.OFFSET_BEGINNING_VAL, progress.getOffsetByPartition(7).longValue());
        Assertions.assertEquals(100L, progress.getOffsetByPartition(0).longValue());
    }

    @Test
    public void testInitialDiscoveryDefaultsToEnd() {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        // empty progress: the job's very first discovery keeps the start-from-latest default
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", Lists.newArrayList(7));

        Deencapsulation.invoke(routineLoadJob, "updateNewPartitionProgress");

        KafkaProgress progress = Deencapsulation.getField(routineLoadJob, "progress");
        Assertions.assertEquals(KafkaProgress.OFFSET_END_VAL, progress.getOffsetByPartition(7).longValue());
    }

    @Test
    public void testKafkaDefaultOffsetsWinsForLaterDiscoveredPartition() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put("kafka_default_offsets", "OFFSET_END");
        Deencapsulation.setField(routineLoadJob, "customProperties", customProperties);
        routineLoadJob.convertCustomProperties(true);
        KafkaProgress progress = Deencapsulation.getField(routineLoadJob, "progress");
        progress.addPartitionOffset(Pair.create(0, 100L));
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", Lists.newArrayList(0, 7));

        Deencapsulation.invoke(routineLoadJob, "updateNewPartitionProgress");

        Assertions.assertEquals(KafkaProgress.OFFSET_END_VAL, progress.getOffsetByPartition(7).longValue());
    }

    @Test
    public void testAlterEnablePartitionDiscoveryUnpinsPartitions() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "customKafkaPartitions", Lists.newArrayList(1, 2, 3));

        Map<String, String> properties = Maps.newHashMap();
        properties.put("property.kafka_partition_discovery", "true");
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties("KAFKA", properties);
        dataSourceProperties.analyze();

        routineLoadJob.modifyDataSourceProperties(dataSourceProperties);

        List<Integer> pinnedPartitions = Deencapsulation.getField(routineLoadJob, "customKafkaPartitions");
        Assertions.assertTrue(pinnedPartitions.isEmpty(),
                "enabling kafka_partition_discovery should unpin the partition list");
        Assertions.assertTrue((Boolean) Deencapsulation.getField(routineLoadJob, "kafkaPartitionDiscovery"));
    }

    @Test
    public void testAlterDisablePartitionDiscoveryRejected() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "customKafkaPartitions", Lists.newArrayList(1, 2, 3));

        Map<String, String> properties = Maps.newHashMap();
        properties.put("property.kafka_partition_discovery", "false");
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties("KAFKA", properties);
        dataSourceProperties.analyze();

        Assertions.assertThrows(DdlException.class,
                () -> routineLoadJob.checkDataSourceProperties(dataSourceProperties));
    }

    @Test
    public void testAlterSeedOffsetsBeyondPinnedSetWithPartitionDiscovery() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "customKafkaPartitions", Lists.newArrayList(1, 2));

        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties,
                                                       ComputeResource computeResource) {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2,3");
        properties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "10,20,30");
        properties.put("property.kafka_partition_discovery", "true");
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties("KAFKA", properties);
        dataSourceProperties.analyze();

        // with discovery enabled the seed list may go beyond the pinned set; validated against the broker
        routineLoadJob.checkDataSourceProperties(dataSourceProperties);
        routineLoadJob.modifyDataSourceProperties(dataSourceProperties);

        KafkaProgress progress = Deencapsulation.getField(routineLoadJob, "progress");
        Assertions.assertEquals(30L, progress.getOffsetByPartition(3).longValue());
        List<Integer> pinnedPartitions = Deencapsulation.getField(routineLoadJob, "customKafkaPartitions");
        Assertions.assertTrue(pinnedPartitions.isEmpty());
    }

    @Test
    public void testAlterSeedInvalidPartitionErrorSurfacesRootCause() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "customKafkaPartitions", Lists.newArrayList(1, 2));

        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties,
                                                       ComputeResource computeResource) {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,999");
        properties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "10,20");
        properties.put("property.kafka_partition_discovery", "true");
        RoutineLoadDataSourceProperties dataSourceProperties = new RoutineLoadDataSourceProperties("KAFKA", properties);
        dataSourceProperties.analyze();

        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> routineLoadJob.checkDataSourceProperties(dataSourceProperties));
        // the error must surface the real cause (the partition does not exist in the topic)
        // instead of the misleading "not in the consumed partitions" wrapper
        Assertions.assertTrue(e.getMessage().contains("999"), e.getMessage());
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
        jobProperties.put("max_filter_ratio", "0");
        jobProperties.put("max_error_number", "10");
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
                globalStateMgr.getSqlParser();
                minTimes = 0;
                result = new SqlParser(AstBuilder.getInstance());
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
                "PROPERTIES('format' = 'csv', 'trim_space' = 'true', 'max_filter_ratio' = '0', 'max_error_number' = '10') " +
                "FROM KAFKA('kafka_broker_list' = 'http://127.0.0.1:8080','kafka_topic' = 'topic1');";
        KafkaRoutineLoadJob job = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
        job.setOrigStmt(new OriginStatementInfo(createSQL, 0));
        Assertions.assertEquals("csv", job.getFormat());
        Assertions.assertTrue(job.isTrimspace());
        Assertions.assertEquals((byte) "'".charAt(0), job.getEnclose());
        Assertions.assertEquals((byte) "\\".charAt(0), job.getEscape());

        String data = GsonUtils.GSON.toJson(job, KafkaRoutineLoadJob.class);
        KafkaRoutineLoadJob newJob = GsonUtils.GSON.fromJson(data, KafkaRoutineLoadJob.class);
        Assertions.assertEquals("csv", newJob.getFormat());
        Assertions.assertTrue(newJob.isTrimspace());
        Assertions.assertEquals((byte) "'".charAt(0), newJob.getEnclose());
        Assertions.assertEquals((byte) "\\".charAt(0), newJob.getEscape());
        Assertions.assertEquals(0, newJob.getMaxFilterRatio());
        Assertions.assertEquals(10, newJob.maxErrorNum);
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
                globalStateMgr.getSqlParser();
                minTimes = 0;
                result = new SqlParser(AstBuilder.getInstance());
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
        job.setOrigStmt(new OriginStatementInfo(createSQL, 0));
        Assertions.assertEquals("json", job.getFormat());
        Assertions.assertTrue(job.isStripOuterArray());
        Assertions.assertEquals("['$.category','$.price','$.author']", job.getJsonPaths());
        Assertions.assertEquals("", job.getJsonRoot());

        String data = GsonUtils.GSON.toJson(job, KafkaRoutineLoadJob.class);
        KafkaRoutineLoadJob newJob = GsonUtils.GSON.fromJson(data, KafkaRoutineLoadJob.class);
        Assertions.assertEquals("json", newJob.getFormat());
        Assertions.assertTrue(newJob.isStripOuterArray());
        Assertions.assertEquals("['$.category','$.price','$.author']", newJob.getJsonPaths());
        Assertions.assertEquals("", newJob.getJsonRoot());
    }

    @Test
    public void testSerializationJsonWithEnvelope(@Mocked GlobalStateMgr globalStateMgr,
                                                  @Injectable Database database,
                                                  @Injectable OlapTable table) throws StarRocksException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        Map<String, String> jobProperties = createRoutineLoadStmt.getJobProperties();
        jobProperties.put("format", "json");
        jobProperties.put("json_root", "");
        jobProperties.put("strip_outer_array", "false");
        jobProperties.put("envelope", CreateRoutineLoadStmt.ENVELOPE_DEBEZIUM);
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
                table.getKeysType();
                minTimes = 0;
                result = KeysType.PRIMARY_KEYS;
                globalStateMgr.getSqlParser();
                minTimes = 0;
                result = new SqlParser(AstBuilder.getInstance());
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

        KafkaRoutineLoadJob job = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
        Assertions.assertEquals(CreateRoutineLoadStmt.ENVELOPE_DEBEZIUM, job.getEnvelope());
        Assertions.assertTrue(job.jobPropertiesToSql()
                .contains("\"" + CreateRoutineLoadStmt.ENVELOPE + "\"=\"" + CreateRoutineLoadStmt.ENVELOPE_DEBEZIUM + "\""));
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
        Assertions.assertTrue(statistic.contains("\"receivedBytesRate\":10"));
        Assertions.assertTrue(statistic.contains("\"loadRowsRate\":16"));
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
            Assertions.assertFalse(kafkaRoutineLoadJob.isPauseOnFatalParseError());
        }
        {
            Deencapsulation.setField(createRoutineLoadStmt, "pauseOnFatalParseError", true);
            KafkaRoutineLoadJob kafkaRoutineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
            Assertions.assertTrue(kafkaRoutineLoadJob.isPauseOnFatalParseError());
        }
    }

    @Test
    public void testGetSourceLagString() {
        RoutineLoadJob job = new KafkaRoutineLoadJob(1L, "routine_load", 1L, 1L, "127.0.0.1:9020", "topic1");
        // check empty value
        String progressJsonStr = null;
        String sourceLagString = job.getSourceLagString(progressJsonStr);
        Assertions.assertTrue(sourceLagString.contains("null"));

        progressJsonStr = "{\"0\":\"100\"}";
        Map<Integer, Long> latestPartitionOffsets = null;
        Deencapsulation.setField(job, "latestPartitionOffsets", latestPartitionOffsets);
        sourceLagString = job.getSourceLagString(progressJsonStr);
        Assertions.assertTrue(sourceLagString.contains("null"));

        progressJsonStr = "{\"0\":null}";
        latestPartitionOffsets = Maps.newHashMap();
        latestPartitionOffsets.put(0, 200L);
        Deencapsulation.setField(job, "latestPartitionOffsets", latestPartitionOffsets);
        sourceLagString = job.getSourceLagString(progressJsonStr);
        Assertions.assertTrue(sourceLagString.contains("{}"));

        progressJsonStr = "{\"0\":\"" + KafkaProgress.OFFSET_ZERO + "\"}";
        sourceLagString = job.getSourceLagString(progressJsonStr);
        Assertions.assertTrue(sourceLagString.contains("{}"));

        progressJsonStr = "{\"0\":\"XXX\"}";
        sourceLagString = job.getSourceLagString(progressJsonStr);
        Assertions.assertTrue(sourceLagString.contains("{}"));

        progressJsonStr = "{\"0\":\"100\"}";
        latestPartitionOffsets = Maps.newHashMap();
        latestPartitionOffsets.put(0, 200L);
        Deencapsulation.setField(job, "latestPartitionOffsets", latestPartitionOffsets);
        sourceLagString = job.getSourceLagString(progressJsonStr);
        Assertions.assertTrue(sourceLagString.contains("\"0\":\"100\""));

        //check  progress > latestPartitionOffsets
        progressJsonStr = "{\"0\":\"200\"}";
        latestPartitionOffsets.put(0, 100L);
        Deencapsulation.setField(job, "latestPartitionOffsets", latestPartitionOffsets);
        sourceLagString = job.getSourceLagString(progressJsonStr);
        Assertions.assertTrue(sourceLagString.contains("\"0\":\"0\""));

    }

    @Test
    public void testUpdateLagTimeMetricsFromProgress() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 1L, "127.0.0.1:9020", "topic1");
        
        // Create timestamp progress with future timestamp (clock drift scenario)
        Map<Integer, Long> partitionTimestamps = Maps.newHashMap();
        long currentTime = System.currentTimeMillis();
        partitionTimestamps.put(0, currentTime + 60000); // 1 minute in the future (clock drift)
        partitionTimestamps.put(1, currentTime - 5000);  // Normal timestamp
        
        KafkaProgress timestampProgress = new KafkaProgress(partitionTimestamps);
        Deencapsulation.setField(job, "timestampProgress", timestampProgress);
        
        new MockUp<RoutineLoadLagTimeMetricMgr>() {
            @Mock
            public void updateRoutineLoadLagTimeMetric(long dbId, String jobName, Map<Integer, Long> partitionLagTimes) {
                // Verify clock drift handling: partition 0 should have lag 0, partition 1 should have positive lag
                Assertions.assertTrue(partitionLagTimes.containsKey(0));
                Assertions.assertTrue(partitionLagTimes.containsKey(1));
                Assertions.assertEquals(Long.valueOf(0L), partitionLagTimes.get(0)); // Clock drift case
                Assertions.assertTrue(partitionLagTimes.get(1) > 0); // Normal case
            }
        };
        
        // Execute: Call the private method
        Deencapsulation.invoke(job, "updateLagTimeMetricsFromProgress");
    }

    @Test
    public void testUpdateLagTimeMetricsFromProgressWithException() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 1L, "127.0.0.1:9020", "topic1");
        // Set null timestamp progress to trigger exception
        Deencapsulation.setField(job, "timestampProgress", null);
        Deencapsulation.invoke(job, "updateLagTimeMetricsFromProgress");
    }

    @Test
    public void testGetRoutineLoadLagTimeSuccess() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 1L, "127.0.0.1:9020", "topic1");
        
        // Mock successful retrieval from RoutineLoadLagTimeMetricMgr
        Map<Integer, Long> expectedLagTimes = Maps.newHashMap();
        expectedLagTimes.put(0, 10L);
        expectedLagTimes.put(1, 15L);
        
        new MockUp<RoutineLoadLagTimeMetricMgr>() {
            @Mock
            public Map<Integer, Long> getPartitionLagTimes(long dbId, String jobName) {
                return expectedLagTimes;
            }
        };
        
        // Execute: Call the private method
        Map<Integer, Long> result = Deencapsulation.invoke(job, "getRoutineLoadLagTime");
        
        // Verify: Should return the expected lag times
        Assertions.assertEquals(expectedLagTimes, result);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(Long.valueOf(10L), result.get(0));
        Assertions.assertEquals(Long.valueOf(15L), result.get(1));
    }

    @Test
    public void testGetRoutineLoadLagTimeEmpty() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 1L, "127.0.0.1:9020", "topic1");
        
        // Mock empty retrieval from RoutineLoadLagTimeMetricMgr
        new MockUp<RoutineLoadLagTimeMetricMgr>() {
            @Mock
            public Map<Integer, Long> getPartitionLagTimes(long dbId, String jobName) {
                return Maps.newHashMap(); // Empty map
            }
        };
        
        // Execute: Call the private method
        Map<Integer, Long> result = Deencapsulation.invoke(job, "getRoutineLoadLagTime");
        
        // Verify: Should return empty map
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetRoutineLoadLagTimeWithNull() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 1L, "127.0.0.1:9020", "topic1");
        
        // Mock null retrieval from RoutineLoadLagTimeMetricMgr
        new MockUp<RoutineLoadLagTimeMetricMgr>() {
            @Mock
            public Map<Integer, Long> getPartitionLagTimes(long dbId, String jobName) {
                return null; // Null return
            }
        };
        
        // Execute: Call the private method
        Map<Integer, Long> result = Deencapsulation.invoke(job, "getRoutineLoadLagTime");
        
        // Verify: Should return empty map as fallback
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testFromCreateStmtEnvelopeDebeziumRequiresPrimaryKeyTable(
            @Mocked GlobalStateMgr globalStateMgr,
            @Injectable Database database,
            @Injectable OlapTable table) {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, partitionNames);
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaBrokerList", serverAddress);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaTopic", topicName);
        Deencapsulation.setField(createRoutineLoadStmt, "format", "json");
        Deencapsulation.setField(createRoutineLoadStmt, "envelope", "debezium");

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), tableNameString);
                minTimes = 0;
                result = table;
                database.getId();
                minTimes = 0;
                result = 1L;
                table.getId();
                minTimes = 0;
                result = 2L;
                table.isOlapOrCloudNativeTable();
                minTimes = 0;
                result = true;
                table.getKeysType();
                result = KeysType.DUP_KEYS;
            }
        };

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class,
                "envelope=debezium is only supported on PRIMARY KEY tables",
                () -> KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt));
    }

    @Test
    public void testGetRoutineLoadLagTimeWithException() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 1L, "127.0.0.1:9020", "topic1");
        
        // Mock exception from RoutineLoadLagTimeMetricMgr
        new MockUp<RoutineLoadLagTimeMetricMgr>() {
            @Mock
            public Map<Integer, Long> getPartitionLagTimes(long dbId, String jobName) {
                throw new RuntimeException("Test exception");
            }
        };
        
        // Execute: Call the private method
        Map<Integer, Long> result = Deencapsulation.invoke(job, "getRoutineLoadLagTime");
        
        // Verify: Should return empty map as fallback
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testJobPropertiesColumnToColumnExpr() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 1L, "127.0.0.1:9020", "topic1");

        List<ImportColumnDesc> columnDescs = Lists.newArrayList();
        // plain column without expr
        columnDescs.add(new ImportColumnDesc("col1"));
        // mapping column with expr
        Expr expr = SqlParser.parseSqlToExpr("col1 + 1", 0);
        columnDescs.add(new ImportColumnDesc("col2", expr));
        // column whose name needs quoting (contains the separator), must be backtick-wrapped
        // so the rendered SQL stays unambiguous
        columnDescs.add(new ImportColumnDesc("a,b"));
        // column whose name itself contains a backtick: the embedded backtick must be doubled
        // (a`b -> `a``b`), otherwise the rendered SQL is malformed.
        columnDescs.add(new ImportColumnDesc("a`b"));
        Deencapsulation.setField(job, "columnDescs", columnDescs);

        String jobProperties = Deencapsulation.invoke(job, "jobPropertiesToJsonString");
        // The expr must be rendered as readable SQL, not a Java object reference like
        // com.starrocks.sql.ast.ImportColumnDesc@19e02a72
        Assertions.assertFalse(jobProperties.contains("ImportColumnDesc@"), jobProperties);
        // column names are backtick-wrapped (embedded backticks doubled), mapping column rendered
        // as "`name`=<exprSql>"
        Assertions.assertTrue(
                jobProperties.contains("\"columnToColumnExpr\":\"`col1`,`col2`=col1 + 1,`a,b`,`a``b`\""),
                jobProperties);
    }


}
