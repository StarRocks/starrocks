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
package com.starrocks.load.routineload;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

public class RoutineLoadJobMetaTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_strict_storage_medium_check = false;
        Config.stream_load_max_txn_num_per_be = 5;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE site_access_auto (\n" +
                        "    event_day DATETIME NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "PROPERTIES(\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");");
    }

    @Test
    public void testMetaNotFound() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb("test");
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "rj", db.getId(), 2L, "", "");

        Assertions.assertThrows(MetaNotFoundException.class,
                () -> routineLoadJob.plan(new TUniqueId(1, 2), 1, "", -1L));
    }

    @Test
    public void testTxnNotFound() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_auto");
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "rj", db.getId(), table.getId(), "", "");

        Assertions.assertThrows(StarRocksException.class,
                () -> routineLoadJob.plan(new TUniqueId(1, 2), 1, "", -1L));
    }

    @Test
    public void testPlanPipeline() throws Exception {
        // Cover the pipeline routine load path (Config.enable_pipeline_routine_load + a valid beId):
        // RoutineLoadJob.plan -> LoadPlanner(ROUTINE_LOAD, syncStreamLoad) pinned to the BE ->
        // DefaultCoordinator.buildLocalStreamLoadParams. The mock cluster has backend 10001.
        boolean saved = Config.enable_pipeline_routine_load;
        Config.enable_pipeline_routine_load = true;
        try {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "site_access_auto");
            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(100L, "pipeline_rl_job", db.getId(),
                    table.getId(), "localhost:9092", "topic1");

            String label = "pipeline_rl_label";
            long txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                    db.getId(), Lists.newArrayList(table.getId()), label, null,
                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localhost"),
                    TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, job.getId(),
                    60, job.getComputeResource());

            TExecPlanFragmentParams params = job.plan(new TUniqueId(6, 7), txnId, label, 10001L);
            Assertions.assertNotNull(params);
            Assertions.assertEquals(TLoadJobType.ROUTINE_LOAD, params.query_options.getLoad_job_type());
            Assertions.assertTrue(params.is_pipeline);
        } finally {
            Config.enable_pipeline_routine_load = saved;
        }
    }

    @Test
    public void testPulsarTaskPlanPipeline() throws Exception {
        // Cover PulsarTaskInfo's pipeline plan call site (PulsarTaskInfo.plan -> RoutineLoadJob.plan
        // with the assigned beId) under Config.enable_pipeline_routine_load.
        boolean saved = Config.enable_pipeline_routine_load;
        Config.enable_pipeline_routine_load = true;
        try {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "site_access_auto");
            PulsarRoutineLoadJob job = new PulsarRoutineLoadJob(101L, "pulsar_rl_job", db.getId(),
                    table.getId(), "pulsar://localhost:6650", "topic1", "sub1");

            String label = "pulsar_rl_label";
            long txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                    db.getId(), Lists.newArrayList(table.getId()), label, null,
                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localhost"),
                    TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, job.getId(),
                    60, job.getComputeResource());

            PulsarTaskInfo task = new PulsarTaskInfo(UUIDUtil.genUUID(), job, 1000, 2000,
                    Arrays.asList("0"), Maps.newHashMap(), 3000);
            task.setBeId(10001L);
            Deencapsulation.setField(task, "txnId", txnId);
            Deencapsulation.setField(task, "label", label);

            TRoutineLoadTask t = task.createRoutineLoadTask();
            Assertions.assertNotNull(t.getParams());
            Assertions.assertTrue(t.getParams().is_pipeline);
        } finally {
            Config.enable_pipeline_routine_load = saved;
        }
    }

    @Test
    public void testKafkaTaskInfoCreateRoutineLoadTaskFormatArrow() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "site_access_auto");
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(102L, "kafka_arrow_rl_job", db.getId(),
                table.getId(), "localhost:9092", "topic1");
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.put("format", "arrow");

        String label = "kafka_arrow_rl_label";
        long txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                db.getId(), Lists.newArrayList(table.getId()), label, null,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localhost"),
                TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, job.getId(),
                60, job.getComputeResource());

        KafkaTaskInfo task = new KafkaTaskInfo(UUIDUtil.genUUID(), job, 1000, 2000,
                com.google.common.collect.ImmutableMap.of(0, 0L), 3000);
        task.setBeId(10001L);
        Deencapsulation.setField(task, "txnId", txnId);
        Deencapsulation.setField(task, "label", label);

        TRoutineLoadTask t = task.createRoutineLoadTask();
        Assertions.assertEquals(com.starrocks.thrift.TFileFormatType.FORMAT_ARROW, t.getFormat());
    }

    @Test
    public void testPulsarTaskInfoCreateRoutineLoadTaskFormatArrow() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "site_access_auto");
        PulsarRoutineLoadJob job = new PulsarRoutineLoadJob(103L, "pulsar_arrow_rl_job", db.getId(),
                table.getId(), "pulsar://localhost:6650", "topic1", "sub1");
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.put("format", "arrow");

        String label = "pulsar_arrow_rl_label";
        long txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                db.getId(), Lists.newArrayList(table.getId()), label, null,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localhost"),
                TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, job.getId(),
                60, job.getComputeResource());

        PulsarTaskInfo task = new PulsarTaskInfo(UUIDUtil.genUUID(), job, 1000, 2000,
                Arrays.asList("0"), Maps.newHashMap(), 3000);
        task.setBeId(10001L);
        Deencapsulation.setField(task, "txnId", txnId);
        Deencapsulation.setField(task, "label", label);

        TRoutineLoadTask t = task.createRoutineLoadTask();
        Assertions.assertEquals(com.starrocks.thrift.TFileFormatType.FORMAT_ARROW, t.getFormat());
    }

    @Test
    public void testPulsarTaskInfoCreateRoutineLoadTaskFormatAvro() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "site_access_auto");
        PulsarRoutineLoadJob job = new PulsarRoutineLoadJob(104L, "pulsar_avro_rl_job", db.getId(),
                table.getId(), "pulsar://localhost:6650", "topic1", "sub1");
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.put("format", "avro");

        String label = "pulsar_avro_rl_label";
        long txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                db.getId(), Lists.newArrayList(table.getId()), label, null,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localhost"),
                TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, job.getId(),
                60, job.getComputeResource());

        PulsarTaskInfo task = new PulsarTaskInfo(UUIDUtil.genUUID(), job, 1000, 2000,
                Arrays.asList("0"), Maps.newHashMap(), 3000);
        task.setBeId(10001L);
        Deencapsulation.setField(task, "txnId", txnId);
        Deencapsulation.setField(task, "label", label);

        TRoutineLoadTask t = task.createRoutineLoadTask();
        Assertions.assertEquals(com.starrocks.thrift.TFileFormatType.FORMAT_AVRO, t.getFormat());
    }

    @Test
    public void testKafkaTaskInfoCreateRoutineLoadTaskFormatAvro() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "site_access_auto");
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(105L, "kafka_avro_rl_job", db.getId(),
                table.getId(), "localhost:9092", "topic1");
        Map<String, String> jobProperties = Deencapsulation.getField(job, "jobProperties");
        jobProperties.put("format", "avro");

        String label = "kafka_avro_rl_label";
        long txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                db.getId(), Lists.newArrayList(table.getId()), label, null,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localhost"),
                TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, job.getId(),
                60, job.getComputeResource());

        KafkaTaskInfo task = new KafkaTaskInfo(UUIDUtil.genUUID(), job, 1000, 2000,
                com.google.common.collect.ImmutableMap.of(0, 0L), 3000);
        task.setBeId(10001L);
        Deencapsulation.setField(task, "txnId", txnId);
        Deencapsulation.setField(task, "label", label);

        TRoutineLoadTask t = task.createRoutineLoadTask();
        Assertions.assertEquals(com.starrocks.thrift.TFileFormatType.FORMAT_AVRO, t.getFormat());
    }
}
