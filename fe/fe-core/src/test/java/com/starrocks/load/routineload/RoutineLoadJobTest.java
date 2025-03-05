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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/routineload/RoutineLoadJobTest.java

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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.KafkaUtil;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.RoutineLoadOperation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.thrift.TKafkaRLTaskProgress;
import com.starrocks.thrift.TRoutineLoadJobInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.DefaultWarehouse;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RoutineLoadJobTest {
    @Test
    public void testAfterAbortedReasonOffsetOutOfRange(@Mocked GlobalStateMgr globalStateMgr,
                                                       @Injectable TransactionState transactionState,
                                                       @Injectable RoutineLoadTaskInfo routineLoadTaskInfo)
            throws StarRocksException {

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        long txnId = 1L;

        new Expectations() {
            {
                transactionState.getTransactionId();
                minTimes = 0;
                result = txnId;
                routineLoadTaskInfo.getTxnId();
                minTimes = 0;
                result = txnId;
            }
        };

        new MockUp<RoutineLoadJob>() {
            @Mock
            void writeUnlock() {
            }
        };

        String txnStatusChangeReasonString = TxnStatusChangeReason.OFFSET_OUT_OF_RANGE.toString();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        routineLoadJob.afterAborted(transactionState, true, txnStatusChangeReasonString);

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
    }

    @Test
    public void testAfterAborted(@Mocked RoutineLoadMgr routineLoadMgr,
                                 @Injectable TransactionState transactionState,
                                 @Injectable KafkaTaskInfo routineLoadTaskInfo) throws StarRocksException {
        Deencapsulation.setField(routineLoadTaskInfo, "routineLoadManager", routineLoadMgr);
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        long txnId = 1L;

        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
        TKafkaRLTaskProgress tKafkaRLTaskProgress = new TKafkaRLTaskProgress();
        tKafkaRLTaskProgress.partitionCmtOffset = Maps.newHashMap();
        KafkaProgress kafkaProgress = new KafkaProgress(tKafkaRLTaskProgress.getPartitionCmtOffset());
        Deencapsulation.setField(attachment, "progress", kafkaProgress);

        KafkaProgress currentProgress = new KafkaProgress(tKafkaRLTaskProgress.getPartitionCmtOffset());

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();

        new Expectations() {
            {
                transactionState.getTransactionId();
                minTimes = 0;
                result = txnId;
                routineLoadTaskInfo.getTxnId();
                minTimes = 0;
                result = txnId;
                transactionState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                routineLoadTaskInfo.getPartitions();
                minTimes = 0;
                result = Lists.newArrayList();
                routineLoadTaskInfo.getId();
                minTimes = 0;
                result = UUID.randomUUID();
                routineLoadMgr.getJob(anyLong);
                minTimes = 0;
                result = routineLoadJob;
            }
        };

        new MockUp<RoutineLoadJob>() {
            @Mock
            void writeUnlock() {
            }
        };

        String txnStatusChangeReasonString = "no data";
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        Deencapsulation.setField(routineLoadJob, "progress", currentProgress);
        TableMetricsEntity entity =
                TableMetricsRegistry.getInstance().getMetricsEntity(routineLoadTaskInfo.getJob().tableId);
        long prevValue = entity.counterRoutineLoadAbortedTasksTotal.getValue();
        routineLoadJob.afterAborted(transactionState, true, txnStatusChangeReasonString);

        Assert.assertEquals(RoutineLoadJob.JobState.RUNNING, routineLoadJob.getState());
        Assert.assertEquals(Long.valueOf(1), Deencapsulation.getField(routineLoadJob, "abortedTaskNum"));
        Assert.assertTrue(routineLoadJob.getOtherMsg(), routineLoadJob.getOtherMsg().endsWith(txnStatusChangeReasonString));

        Assert.assertEquals(Long.valueOf(prevValue + 1), entity.counterRoutineLoadAbortedTasksTotal.getValue());

        routineLoadTaskInfoList.clear();
        routineLoadJob.afterAborted(transactionState, true, txnStatusChangeReasonString);
        Assert.assertEquals(Long.valueOf(2), Deencapsulation.getField(routineLoadJob, "abortedTaskNum"));
        Assert.assertEquals(Long.valueOf(prevValue + 2), entity.counterRoutineLoadAbortedTasksTotal.getValue());
    }

    @Test
    public void testAfterCommitted(@Mocked RoutineLoadMgr routineLoadMgr,
                                 @Injectable TransactionState transactionState,
                                 @Injectable KafkaTaskInfo routineLoadTaskInfo) throws StarRocksException {
        Deencapsulation.setField(routineLoadTaskInfo, "routineLoadManager", routineLoadMgr);
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        long txnId = 1L;

        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
        TKafkaRLTaskProgress tKafkaRLTaskProgress = new TKafkaRLTaskProgress();
        tKafkaRLTaskProgress.partitionCmtOffset = Maps.newHashMap();
        KafkaProgress kafkaProgress = new KafkaProgress(tKafkaRLTaskProgress.getPartitionCmtOffset());
        Deencapsulation.setField(attachment, "progress", kafkaProgress);

        KafkaProgress currentProgress = new KafkaProgress(tKafkaRLTaskProgress.getPartitionCmtOffset());

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();

        new Expectations() {
            {
                transactionState.getTransactionId();
                minTimes = 0;
                result = txnId;
                routineLoadTaskInfo.getTxnId();
                minTimes = 0;
                result = txnId;
                transactionState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                routineLoadTaskInfo.getPartitions();
                minTimes = 0;
                result = Lists.newArrayList();
                routineLoadTaskInfo.getId();
                minTimes = 0;
                result = UUID.randomUUID();
                routineLoadMgr.getJob(anyLong);
                minTimes = 0;
                result = routineLoadJob;
            }
        };

        new MockUp<RoutineLoadJob>() {
            @Mock
            void writeUnlock() {
            }
        };

        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        Deencapsulation.setField(routineLoadJob, "progress", currentProgress);
        TableMetricsEntity entity =
                TableMetricsRegistry.getInstance().getMetricsEntity(routineLoadTaskInfo.getJob().tableId);
        long prevValue = entity.counterRoutineLoadCommittedTasksTotal.getValue();
        routineLoadJob.afterCommitted(transactionState, true);

        Assert.assertEquals(RoutineLoadJob.JobState.RUNNING, routineLoadJob.getState());
        Assert.assertEquals(Long.valueOf(1), Deencapsulation.getField(routineLoadJob, "committedTaskNum"));

        Assert.assertEquals(Long.valueOf(prevValue + 1), entity.counterRoutineLoadCommittedTasksTotal.getValue());
    }

    @Test
    public void testPulsarGetShowInfo() {
        {
            // PAUSE state
            PulsarRoutineLoadJob routineLoadJob = new PulsarRoutineLoadJob();
            Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
            ErrorReason errorReason = new ErrorReason(InternalErrorCode.INTERNAL_ERR,
                    TxnStatusChangeReason.OFFSET_OUT_OF_RANGE.toString());
            Deencapsulation.setField(routineLoadJob, "pauseReason", errorReason);

            List<String> showInfo = routineLoadJob.getShowInfo();
            Assert.assertTrue(showInfo.stream().filter(entity -> !Strings.isNullOrEmpty(entity))
                    .anyMatch(entity -> entity.equals(errorReason.toString())));

            TRoutineLoadJobInfo loadJobInfo = routineLoadJob.toThrift();
            Assert.assertTrue(loadJobInfo.getReasons_of_state_changed().equals(errorReason.toString()));
        }

        {
            // PAUSE state
            PulsarRoutineLoadJob routineLoadJob = new PulsarRoutineLoadJob(
                    1L, "task1", 1, 1, "http://url", "task-1", "sub-1");
            Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
            ErrorReason errorReason = new ErrorReason(InternalErrorCode.INTERNAL_ERR,
                    TxnStatusChangeReason.OFFSET_OUT_OF_RANGE.toString());
            Deencapsulation.setField(routineLoadJob, "pauseReason", errorReason);

            List<String> showInfo = routineLoadJob.getShowInfo();
            Assert.assertTrue(showInfo.stream().filter(entity -> !Strings.isNullOrEmpty(entity))
                    .anyMatch(entity -> entity.equals(errorReason.toString())));

            TRoutineLoadJobInfo loadJobInfo = routineLoadJob.toThrift();
            Assert.assertTrue(loadJobInfo.getReasons_of_state_changed().equals(errorReason.toString()));
        }
    }

    @Test
    public void testKafkaGetShowInfo() throws StarRocksException {
        {
            // PAUSE state
            KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
            Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
            ErrorReason errorReason = new ErrorReason(InternalErrorCode.INTERNAL_ERR,
                    TxnStatusChangeReason.OFFSET_OUT_OF_RANGE.toString());
            Deencapsulation.setField(routineLoadJob, "pauseReason", errorReason);

            List<String> showInfo = routineLoadJob.getShowInfo();
            Assert.assertEquals(true, showInfo.stream().filter(entity -> !Strings.isNullOrEmpty(entity))
                    .anyMatch(entity -> entity.equals(errorReason.toString())));

            TRoutineLoadJobInfo loadJobInfo = routineLoadJob.toThrift();
            Assert.assertTrue(loadJobInfo.getReasons_of_state_changed().equals(errorReason.toString()));
        }

        {
            // Progress
            KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();

            Map<Integer, Long> partitionOffsets = Maps.newHashMap();
            partitionOffsets.put(Integer.valueOf(0), Long.valueOf(1234));
            KafkaProgress kafkaProgress = new KafkaProgress(partitionOffsets);
            Deencapsulation.setField(routineLoadJob, "progress", kafkaProgress);

            Map<Integer, Long> partitionOffsetTimestamps = Maps.newHashMap();
            partitionOffsetTimestamps.put(Integer.valueOf(0), Long.valueOf(1701411708410L));
            KafkaProgress kafkaTimestampProgress = new KafkaProgress(partitionOffsetTimestamps);
            Deencapsulation.setField(routineLoadJob, "timestampProgress", kafkaTimestampProgress);
            ((Map<String, String>) Deencapsulation.getField(routineLoadJob, "jobProperties"))
                    .put("pause_on_fatal_parse_error", "true");
            routineLoadJob.setPartitionOffset(0, 12345);

            List<String> showInfo = routineLoadJob.getShowInfo();
            Assert.assertEquals("{\"0\":\"12345\"}", showInfo.get(20));
            //The displayed value is the actual value - 1
            Assert.assertEquals("{\"0\":\"1233\"}", showInfo.get(14));
            Assert.assertEquals("{\"0\":\"1701411708409\"}", showInfo.get(15));
            Assert.assertTrue(showInfo.get(10).contains("\"pause_on_fatal_parse_error\":\"true\""));

            
            TRoutineLoadJobInfo loadJobInfo = routineLoadJob.toThrift();
            Assert.assertEquals("{\"0\":\"12345\"}", loadJobInfo.getLatest_source_position());
            //The displayed value is the actual value - 1
            Assert.assertEquals("{\"0\":\"1233\"}", loadJobInfo.getProgress());
        }

        {
            // UNSTABLE substate
            KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();

            Map<Integer, Long> partitionOffsetTimestamps = Maps.newHashMap();
            partitionOffsetTimestamps.put(Integer.valueOf(0), Long.valueOf(1701411708410L));
            KafkaProgress kafkaTimestampProgress = new KafkaProgress(partitionOffsetTimestamps);
            Deencapsulation.setField(routineLoadJob, "timestampProgress", kafkaTimestampProgress);
            ((Map<String, String>) Deencapsulation.getField(routineLoadJob, "jobProperties"))
                    .put("pause_on_fatal_parse_error", "false");

            routineLoadJob.updateState(RoutineLoadJob.JobState.RUNNING, null, false);
            // The job is set unstable due to the progress is too slow.
            routineLoadJob.updateSubstate();
            Assert.assertTrue(routineLoadJob.isUnstable());

            List<String> showInfo = routineLoadJob.getShowInfo();
            Assert.assertEquals("UNSTABLE", showInfo.get(7));
            // The lag [xxx] of partition [0] exceeds Config.routine_load_unstable_threshold_second [3600]
            Assert.assertTrue(showInfo.get(16).contains(
                    "partition [0] exceeds Config.routine_load_unstable_threshold_second [3600]"));
        
            TRoutineLoadJobInfo loadJobInfo = routineLoadJob.toThrift();
            Assert.assertEquals("RUNNING", loadJobInfo.getState());
            Assert.assertEquals("", loadJobInfo.getReasons_of_state_changed());

            partitionOffsetTimestamps.put(Integer.valueOf(0), Long.valueOf(System.currentTimeMillis()));
            kafkaTimestampProgress = new KafkaProgress(partitionOffsetTimestamps);
            Deencapsulation.setField(routineLoadJob, "timestampProgress", kafkaTimestampProgress);
            // The job is set stable due to the progress is kept up.
            routineLoadJob.updateSubstate();
            showInfo = routineLoadJob.getShowInfo();
            Assert.assertEquals("RUNNING", showInfo.get(7));
            Assert.assertEquals("", showInfo.get(16));

            loadJobInfo = routineLoadJob.toThrift();
            Assert.assertEquals("RUNNING", loadJobInfo.getState());
            Assert.assertEquals("", loadJobInfo.getReasons_of_state_changed());

            // The job is set stable.
            routineLoadJob.updateSubstateStable();
            showInfo = routineLoadJob.getShowInfo();
            Assert.assertEquals("RUNNING", showInfo.get(7));
            Assert.assertEquals("", showInfo.get(16));
            Assert.assertTrue(showInfo.get(10).contains("\"pause_on_fatal_parse_error\":\"false\""));


            loadJobInfo = routineLoadJob.toThrift();
            Assert.assertEquals("RUNNING", loadJobInfo.getState());
            Assert.assertEquals("", loadJobInfo.getReasons_of_state_changed());
        }
    }

    @Test
    public void testGetShowInfoSharedData(@Mocked GlobalStateMgr globalStateMgr,
                                          @Mocked WarehouseManager warehouseManager) throws StarRocksException {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new Expectations() {
            {
                globalStateMgr.getWarehouseMgr();
                result = warehouseManager;
                warehouseManager.getWarehouse(0L);
                result = new DefaultWarehouse(0, "default_warehouse");
                warehouseManager.getWarehouse(1L);
                result = new Exception("Warehouse id: 1 not exist");
            }
        };

        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJob.setWarehouseId(0L);
        List<String> showInfo = routineLoadJob.getShowInfo();
        Assert.assertEquals(23, showInfo.size());
        Assert.assertEquals("default_warehouse", showInfo.get(20));

        routineLoadJob.setWarehouseId(1L);
        showInfo = routineLoadJob.getShowInfo();
        Assert.assertEquals("Warehouse id: 1 not exist", showInfo.get(20));
    }

    @Test
    public void testUpdateWhileDbDeleted(@Mocked GlobalStateMgr globalStateMgr) throws StarRocksException {
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = null;
            }
        };

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJob.update();

        Assert.assertEquals(RoutineLoadJob.JobState.CANCELLED, routineLoadJob.getState());
    }

    @Test
    public void testUpdateWhileTableDeleted(@Mocked GlobalStateMgr globalStateMgr,
                                            @Injectable Database database) throws StarRocksException {
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = database;
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), anyLong);
                minTimes = 0;
                result = null;
            }
        };
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJob.update();

        Assert.assertEquals(RoutineLoadJob.JobState.CANCELLED, routineLoadJob.getState());
    }

    @Test
    public void testUpdateWhilePartitionChanged(@Mocked GlobalStateMgr globalStateMgr,
                                                @Injectable Database database,
                                                @Injectable Table table,
                                                @Injectable KafkaProgress kafkaProgress) throws StarRocksException {

        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = database;
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), anyLong);
                minTimes = 0;
                result = table;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties,
                                                       long warehouseId) throws StarRocksException {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        new MockUp<EditLog>() {
            @Mock
            public void logOpRoutineLoadJob(RoutineLoadOperation routineLoadOperation) {

            }
        };

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "progress", kafkaProgress);
        routineLoadJob.update();

        Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
    }

    @Test
    public void testUpdateNumOfDataErrorRowMoreThanMax(@Mocked GlobalStateMgr globalStateMgr) {
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "maxErrorNum", 0);
        Deencapsulation.setField(routineLoadJob, "maxBatchRows", 0);
        Deencapsulation.invoke(routineLoadJob, "updateNumOfData", 1L, 1L, 0L, 1L, 1L, false);

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
        ErrorReason reason = routineLoadJob.pauseReason;
        Assert.assertEquals(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR, reason.getCode());
        Assert.assertEquals(
                "Current error rows: 1 is more than max error num: 0. Check the 'TrackingSQL' field for detailed information. " +
                        "If you are sure that the data has many errors, you can set 'max_error_number' property " +
                        "to a greater value through ALTER ROUTINE LOAD and RESUME the job",
                reason.getMsg());
    }

    @Test
    public void testPartialUpdateMode(@Mocked GlobalStateMgr globalStateMgr) {
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Assert.assertEquals(routineLoadJob.getPartialUpdateMode(), "row");
    }

    @Test
    public void testUpdateTotalMoreThanBatch() {
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "maxErrorNum", 10);
        Deencapsulation.setField(routineLoadJob, "maxBatchRows", 10);
        Deencapsulation.setField(routineLoadJob, "currentErrorRows", 1);
        Deencapsulation.setField(routineLoadJob, "currentTotalRows", 99);
        Deencapsulation.invoke(routineLoadJob, "updateNumOfData", 2L, 0L, 0L, 1L, 1L, false);

        Assert.assertEquals(RoutineLoadJob.JobState.RUNNING, Deencapsulation.getField(routineLoadJob, "state"));
        Assert.assertEquals(new Long(0), Deencapsulation.getField(routineLoadJob, "currentErrorRows"));
        Assert.assertEquals(new Long(0), Deencapsulation.getField(routineLoadJob, "currentTotalRows"));

    }

    @Test
    public void testModifyJobProperties() throws Exception {
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        // alter job properties
        String desiredConcurrentNumber = "3";
        String maxBatchInterval = "60";
        String maxErrorNumber = "10000";
        String maxFilterRatio = "0.3";
        String maxBatchRows = "200000";
        String strictMode = "true";
        String timeZone = "UTC";
        String jsonPaths = "[\\\"$.category\\\",\\\"$.author\\\",\\\"$.price\\\",\\\"$.timestamp\\\"]";
        String stripOuterArray = "true";
        String jsonRoot = "$.RECORDS";
        String taskTimeout = "20";
        String taskConsumeTime = "3";
        String pauseOnFatalParseError = "false";
        String originStmt = "alter routine load for db.job1 " +
                "properties (" +
                "   \"desired_concurrent_number\" = \"" + desiredConcurrentNumber + "\"," +
                "   \"max_batch_interval\" = \"" + maxBatchInterval + "\"," +
                "   \"max_error_number\" = \"" + maxErrorNumber + "\"," +
                "   \"max_filter_ratio\" = \"" + maxFilterRatio + "\"," +
                "   \"max_batch_rows\" = \"" + maxBatchRows + "\"," +
                "   \"task_consume_second\" = \"" + taskConsumeTime + "\"," +
                "   \"task_timeout_second\" = \"" + taskTimeout + "\"," +
                "   \"strict_mode\" = \"" + strictMode + "\"," +
                "   \"pause_on_fatal_parse_error\" = \"" + pauseOnFatalParseError + "\"," +
                "   \"timezone\" = \"" + timeZone + "\"," +
                "   \"jsonpaths\" = \"" + jsonPaths + "\"," +
                "   \"strip_outer_array\" = \"" + stripOuterArray + "\"," +
                "   \"json_root\" = \"" + jsonRoot + "\"" +
                ")";
        AlterRoutineLoadStmt stmt = (AlterRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        for (String key : stmt.getAnalyzedJobProperties().keySet()) {
            System.out.println("Key: " + key);
            System.out.println("Value: " + stmt.getAnalyzedJobProperties().get(key));
        }
        routineLoadJob.modifyJob(stmt.getRoutineLoadDesc(), stmt.getAnalyzedJobProperties(),
                stmt.getDataSourceProperties(), new OriginStatement(originStmt, 0), true);
        Assert.assertEquals(Integer.parseInt(desiredConcurrentNumber),
                (int) Deencapsulation.getField(routineLoadJob, "desireTaskConcurrentNum"));
        Assert.assertEquals(Long.parseLong(maxBatchInterval),
                (long) Deencapsulation.getField(routineLoadJob, "taskSchedIntervalS"));
        Assert.assertEquals(Long.parseLong(maxErrorNumber),
                (long) Deencapsulation.getField(routineLoadJob, "maxErrorNum"));
        Assert.assertEquals(Double.parseDouble(maxFilterRatio),
                (double) Deencapsulation.getField(routineLoadJob, "maxFilterRatio"), 0.01);
        Assert.assertEquals(Long.parseLong(taskTimeout),
                (long) Deencapsulation.getField(routineLoadJob, "taskTimeoutSecond"));
        Assert.assertEquals(Long.parseLong(taskConsumeTime),
                (long) Deencapsulation.getField(routineLoadJob, "taskConsumeSecond"));
        Assert.assertEquals(Long.parseLong(maxBatchRows),
                (long) Deencapsulation.getField(routineLoadJob, "maxBatchRows"));
        Assert.assertEquals(Boolean.parseBoolean(strictMode), routineLoadJob.isStrictMode());
        Assert.assertEquals(timeZone, routineLoadJob.getTimezone());
        Assert.assertEquals(jsonPaths.replace("\\", ""), routineLoadJob.getJsonPaths());
        Assert.assertEquals(Boolean.parseBoolean(stripOuterArray), routineLoadJob.isStripOuterArray());
        Assert.assertEquals(jsonRoot, routineLoadJob.getJsonRoot());
        Assert.assertEquals(Boolean.parseBoolean(pauseOnFatalParseError), routineLoadJob.isPauseOnFatalParseError());
    }

    @Test
    public void testModifyDataSourceProperties() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        //alter data source custom properties
        String groupId = "group1";
        String clientId = "client1";
        String defaultOffsets = "OFFSET_BEGINNING";
        String originStmt = "alter routine load for db.job1 " +
                "FROM KAFKA (" +
                "   \"property.group.id\" = \"" + groupId + "\"," +
                "   \"property.client.id\" = \"" + clientId + "\"," +
                "   \"property.kafka_default_offsets\" = \"" + defaultOffsets + "\"" +
                ")";
        routineLoadJob.setOrigStmt(new OriginStatement(originStmt, 0));
        AlterRoutineLoadStmt stmt = (AlterRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        routineLoadJob.modifyJob(stmt.getRoutineLoadDesc(), stmt.getAnalyzedJobProperties(),
                stmt.getDataSourceProperties(), new OriginStatement(originStmt, 0), true);
        routineLoadJob.convertCustomProperties(true);
        Map<String, String> properties = routineLoadJob.getConvertedCustomProperties();
        Assert.assertEquals(groupId, properties.get("group.id"));
        Assert.assertEquals(clientId, properties.get("client.id"));
        Assert.assertEquals(-2L,
                (long) Deencapsulation.getField(routineLoadJob, "kafkaDefaultOffSet"));
    }

    @Test
    public void testModifyLoadDesc() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        //alter load desc
        String originStmt = "alter routine load for db.job1 " +
                "COLUMNS (a, b, c, d=a), " +
                "WHERE a = 1," +
                "COLUMNS TERMINATED BY \",\"," +
                "PARTITION(p1, p2, p3)," +
                "ROWS TERMINATED BY \"A\"";
        routineLoadJob.setOrigStmt(new OriginStatement(originStmt, 0));
        AlterRoutineLoadStmt stmt = (AlterRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        routineLoadJob.modifyJob(stmt.getRoutineLoadDesc(), stmt.getAnalyzedJobProperties(),
                stmt.getDataSourceProperties(), new OriginStatement(originStmt, 0), true);
        Assert.assertEquals("a,b,c,d=a", Joiner.on(",").join(routineLoadJob.getColumnDescs()));
        Assert.assertEquals("`a` = 1", routineLoadJob.getWhereExpr().toSql());
        Assert.assertEquals("','", routineLoadJob.getColumnSeparator().toString());
        Assert.assertEquals("'A'", routineLoadJob.getRowDelimiter().toString());
        Assert.assertEquals("p1,p2,p3", Joiner.on(",").join(routineLoadJob.getPartitions().getPartitionNames()));
    }

    @Test
    public void testMergeLoadDescToOriginStatement() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "job",
                2L, 3L, "192.168.1.2:10000", "topic");
        String originStmt = "CREATE ROUTINE LOAD job ON unknown " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")";
        routineLoadJob.setOrigStmt(new OriginStatement(originStmt, 0));

        // alter columns terminator
        RoutineLoadDesc loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "COLUMNS TERMINATED BY ';'", 0), null);
        routineLoadJob.mergeLoadDescToOriginStatement(loadDesc);
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                "COLUMNS TERMINATED BY ';' " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", routineLoadJob.getOrigStmt().originStmt);

        // alter rows terminator
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "ROWS TERMINATED BY '\n'", 0), null);
        routineLoadJob.mergeLoadDescToOriginStatement(loadDesc);
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                "COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n' " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", routineLoadJob.getOrigStmt().originStmt);

        // alter columns
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "COLUMNS(`a`, `b`, `c`=1)", 0), null);
        routineLoadJob.mergeLoadDescToOriginStatement(loadDesc);
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                "COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c` = 1) " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", routineLoadJob.getOrigStmt().originStmt);

        // alter partition
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "TEMPORARY PARTITION(`p1`, `p2`)", 0), null);
        routineLoadJob.mergeLoadDescToOriginStatement(loadDesc);
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                "COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c` = 1), " +
                "TEMPORARY PARTITION(`p1`, `p2`) " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", routineLoadJob.getOrigStmt().originStmt);

        // alter where
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "WHERE a = 1", 0), null);
        routineLoadJob.mergeLoadDescToOriginStatement(loadDesc);
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                "COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c` = 1), " +
                "TEMPORARY PARTITION(`p1`, `p2`), " +
                "WHERE `a` = 1 " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", routineLoadJob.getOrigStmt().originStmt);

        // alter columns terminator again
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "COLUMNS TERMINATED BY '\t'", 0), null);
        routineLoadJob.mergeLoadDescToOriginStatement(loadDesc);
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                "COLUMNS TERMINATED BY '\t', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c` = 1), " +
                "TEMPORARY PARTITION(`p1`, `p2`), " +
                "WHERE `a` = 1 " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", routineLoadJob.getOrigStmt().originStmt);

        // alter rows terminator again
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "ROWS TERMINATED BY 'a'", 0), null);
        routineLoadJob.mergeLoadDescToOriginStatement(loadDesc);
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                "COLUMNS TERMINATED BY '\t', " +
                "ROWS TERMINATED BY 'a', " +
                "COLUMNS(`a`, `b`, `c` = 1), " +
                "TEMPORARY PARTITION(`p1`, `p2`), " +
                "WHERE `a` = 1 " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", routineLoadJob.getOrigStmt().originStmt);

        // alter columns again
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "COLUMNS(`a`)", 0), null);
        routineLoadJob.mergeLoadDescToOriginStatement(loadDesc);
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                "COLUMNS TERMINATED BY '\t', " +
                "ROWS TERMINATED BY 'a', " +
                "COLUMNS(`a`), " +
                "TEMPORARY PARTITION(`p1`, `p2`), " +
                "WHERE `a` = 1 " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", routineLoadJob.getOrigStmt().originStmt);
        // alter partition again
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        " PARTITION(`p1`, `p2`)", 0), null);
        routineLoadJob.mergeLoadDescToOriginStatement(loadDesc);
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                "COLUMNS TERMINATED BY '\t', " +
                "ROWS TERMINATED BY 'a', " +
                "COLUMNS(`a`), " +
                "PARTITION(`p1`, `p2`), " +
                "WHERE `a` = 1 " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", routineLoadJob.getOrigStmt().originStmt);

        // alter where again
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "WHERE a = 5", 0), null);
        routineLoadJob.mergeLoadDescToOriginStatement(loadDesc);
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                "COLUMNS TERMINATED BY '\t', " +
                "ROWS TERMINATED BY 'a', " +
                "COLUMNS(`a`), " +
                "PARTITION(`p1`, `p2`), " +
                "WHERE `a` = 5 " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", routineLoadJob.getOrigStmt().originStmt);

        // alter where again
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "WHERE a = 5 and b like 'c1%' and c between 1 and 100 and substring(d,1,5) = 'cefd' ", 0), null);
        routineLoadJob.mergeLoadDescToOriginStatement(loadDesc);
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                "COLUMNS TERMINATED BY '\t', " +
                "ROWS TERMINATED BY 'a', " +
                "COLUMNS(`a`), " +
                "PARTITION(`p1`, `p2`), " +
                "WHERE (((`a` = 5) " +
                "AND (`b` LIKE 'c1%')) " +
                "AND (`c` BETWEEN 1 AND 100)) " +
                "AND (substring(`d`, 1, 5) = 'cefd') " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", routineLoadJob.getOrigStmt().originStmt);
    }

    @Test
    public void testPauseOnFatalParseError(@Mocked GlobalStateMgr globalStateMgr, @Injectable TransactionState transactionState,
                                                       @Injectable RoutineLoadTaskInfo routineLoadTaskInfo)
            throws StarRocksException {
        long txnId = 1L;
        new Expectations() {
            {
                transactionState.getTransactionId();
                minTimes = 0;
                result = txnId;
                transactionState.getTxnCommitAttachment();
                minTimes = 0;
                result = null;
                routineLoadTaskInfo.getTxnId();
                minTimes = 0;
                result = txnId;
                routineLoadTaskInfo.getId();
                minTimes = 0;
                result = UUID.randomUUID();
            }
        };

        new MockUp<KafkaRoutineLoadJob>() {
            @Mock
            void writeUnlock() {
            }

            @Mock
            RoutineLoadTaskInfo unprotectRenewTask(long timeToExecuteMs, RoutineLoadTaskInfo routineLoadTaskInfo) {
                return routineLoadTaskInfo;
            }
        };

        // pauseOnFatalParseError = false
        {
            new MockUp<GlobalStateMgr>() {
                @Mock
                public RoutineLoadTaskScheduler getRoutineLoadTaskScheduler() {
                    return new RoutineLoadTaskScheduler();
                }
            };

            List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
            routineLoadTaskInfoList.add(routineLoadTaskInfo);
            RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
            Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
            Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
            routineLoadJob.afterAborted(transactionState, true,
                    TxnStatusChangeReason.PARSE_ERROR.toString());
            System.out.println(routineLoadJob.getPauseReason());
            Assert.assertEquals(RoutineLoadJob.JobState.RUNNING, routineLoadJob.getState());
        }

        // pauseOnFatalParseError = true
        {
            List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
            routineLoadTaskInfoList.add(routineLoadTaskInfo);
            RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
            Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
            Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
            ((Map<String, String>) Deencapsulation.getField(routineLoadJob, "jobProperties"))
                    .put("pause_on_fatal_parse_error", "true");
            routineLoadJob.afterAborted(transactionState, true,
                    TxnStatusChangeReason.PARSE_ERROR.toString());
            Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
            String errorMsg =
                    "ErrorReason{errCode = 5611, msg='parse error. Check the 'TrackingSQL' field for detailed information.'}";
            Assert.assertEquals(errorMsg, routineLoadJob.getPauseReason());
        }
    }
}
