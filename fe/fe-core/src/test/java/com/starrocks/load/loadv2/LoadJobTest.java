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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/loadv2/LoadJobTest.java

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
//

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.task.LeaderTask;
import com.starrocks.task.LeaderTaskExecutor;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.RunningTxnExceedException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;

public class LoadJobTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private WarehouseManager warehouseManager;

    @Mocked
    private Warehouse warehouse;

    @BeforeAll
    public static void start() {
        MetricRepo.init();
    }

    @Test
    public void testGetDbNotExists() {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "dbId", 1L);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getLocalMetastore().getDb(1L);
                minTimes = 0;
                result = null;
            }
        };

        try {
            loadJob.getDb();
            Assertions.fail();
        } catch (MetaNotFoundException e) {
        }
    }

    @Test
    public void testSetJobPropertiesWithErrorTimeout() {
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(LoadStmt.TIMEOUT_PROPERTY, "abc");
        LoadJob loadJob = new BrokerLoadJob();
        try {
            loadJob.setJobProperties(jobProperties);
            Assertions.fail();
        } catch (DdlException e) {
        }
    }

    @Test
    public void testSetJobProperties() {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouse(anyLong);
                minTimes = 0;
                result = warehouse;

                warehouse.getId();
                minTimes = 0;
                result = 1001L;
            }
        };

        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(LoadStmt.TIMEOUT_PROPERTY, "1000");
        jobProperties.put(LoadStmt.MAX_FILTER_RATIO_PROPERTY, "0.1");
        jobProperties.put(LoadStmt.LOAD_MEM_LIMIT, "1024");
        jobProperties.put(LoadStmt.STRICT_MODE, "True");

        LoadJob loadJob = new BrokerLoadJob();
        try {
            loadJob.setJobProperties(jobProperties);
            Assertions.assertEquals(1000, (long) Deencapsulation.getField(loadJob, "timeoutSecond"));
            Assertions.assertEquals(0.1, Deencapsulation.getField(loadJob, "maxFilterRatio"), 0);
            Assertions.assertEquals(1024, (long) Deencapsulation.getField(loadJob, "loadMemLimit"));
            Assertions.assertTrue((Boolean) Deencapsulation.getField(loadJob, "strictMode"));
        } catch (DdlException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testSetJobPropertiesForWarehouse() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouse(anyLong);
                minTimes = 0;
                result = warehouse;

                warehouse.getId();
                minTimes = 0;
                result = 1001L;
            }
        };

        ConnectContext context = new ConnectContext(null);
        new Expectations(context) {
            {
                ConnectContext.get();
                result = context;

                context.getCurrentWarehouseId();
                result = 1000L;
            }
        };

        try {
            // normal, jobProperties set
            LoadJob loadJob1 = new BrokerLoadJob();
            Map<String, String> jobProperties1 = Maps.newHashMap();
            jobProperties1.put(PropertyAnalyzer.PROPERTIES_WAREHOUSE, "test_warehouse");
            loadJob1.setJobProperties(jobProperties1);
            Assertions.assertEquals(1001L, (long) Deencapsulation.getField(loadJob1, "warehouseId"));

            // with jobProperties set, but no warehouse property,
            LoadJob loadJob2 = new BrokerLoadJob();
            Map<String, String> jobProperties2 = Maps.newHashMap();
            loadJob2.setJobProperties(jobProperties2);
            Assertions.assertEquals(1000L, (long) Deencapsulation.getField(loadJob2, "warehouseId"));

            // no jobProperties provided
            LoadJob loadJob3 = new BrokerLoadJob();
            loadJob3.setJobProperties(null);
            Assertions.assertEquals(1000L, (long) Deencapsulation.getField(loadJob3, "warehouseId"));
        } catch (DdlException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testExecute(@Mocked GlobalTransactionMgr globalTransactionMgr,
                            @Mocked LeaderTaskExecutor leaderTaskExecutor)
            throws LabelAlreadyUsedException, RunningTxnExceedException, AnalysisException, DuplicatedRequestException {
        // BrokerLoadJob now defers beginTxn + the PENDING -> LOADING transition
        // out of unprotectedExecute and into createLoadingTask (after the
        // pre-split hook returns). execute() only submits the BrokerLoadPendingTask;
        // state stays PENDING and transactionId stays 0 until the pending task
        // finishes and createLoadingTask opens the load txn under the write lock.
        LoadJob loadJob = new BrokerLoadJob();
        new Expectations() {
            {
                globalTransactionMgr.beginTransaction(anyLong, Lists.newArrayList(), anyString, (TUniqueId) any,
                        (TransactionState.TxnCoordinator) any,
                        (TransactionState.LoadJobSourceType) any, anyLong, anyLong, (ComputeResource) any);
                minTimes = 0;
                result = 1;
                leaderTaskExecutor.submit((LeaderTask) any);
                minTimes = 0;
                result = true;
            }
        };

        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        new MockUp<EditLog>() {
            @Mock
            public void logSaveNextId(long nextId) {

            }
        };

        try {
            loadJob.execute();
        } catch (LoadException e) {
            Assertions.fail(e.getMessage());
        }
        Assertions.assertEquals(JobState.PENDING, loadJob.getState());
        Assertions.assertEquals(0L, loadJob.getTransactionId());
        Map<Long, LoadTask> idToTasks = Deencapsulation.getField(loadJob, "idToTasks");
        Assertions.assertEquals(1, idToTasks.size(),
                "execute() submits a BrokerLoadPendingTask before deferring beginTxn");
    }

    @Test
    public void testProcessTimeoutWithCompleted() {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "state", JobState.FINISHED);

        loadJob.processTimeout();
        Assertions.assertEquals(JobState.FINISHED, loadJob.getState());
    }

    @Test
    public void testProcessTimeoutWithIsCommitting() {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "isCommitting", true);
        Deencapsulation.setField(loadJob, "state", JobState.LOADING);

        loadJob.processTimeout();
        Assertions.assertEquals(JobState.LOADING, loadJob.getState());
    }

    @Test
    public void testProcessTimeoutWithLongTimeoutSecond() {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "createTimestamp", System.currentTimeMillis());
        Deencapsulation.setField(loadJob, "timeoutSecond", 1000L);

        loadJob.processTimeout();
        Assertions.assertEquals(JobState.PENDING, loadJob.getState());
    }

    @Test
    public void testProcessTimeout(@Mocked EditLog editLog) {
        new FakeEditLog();
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "timeoutSecond", 0);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        loadJob.processTimeout();
        Assertions.assertEquals(JobState.CANCELLED, loadJob.getState());
    }

    @Test
    public void testUpdateStateToLoading() {
        LoadJob loadJob = new BrokerLoadJob();
        loadJob.updateState(JobState.LOADING);
        Assertions.assertEquals(JobState.LOADING, loadJob.getState());
    }

    @Test
    public void testUpdateStateToFinished(@Mocked MetricRepo metricRepo,
                                          @Injectable LoadTask loadTask1,
                                          @Mocked LongCounterMetric longCounterMetric) {

        MetricRepo.COUNTER_LOAD_FINISHED = longCounterMetric;
        LoadJob loadJob = new BrokerLoadJob();
        loadJob.idToTasks.put(1L, loadTask1);

        // TxnStateCallbackFactory factory = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getCallbackFactory();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        GlobalTransactionMgr mgr = new GlobalTransactionMgr(globalStateMgr);
        Deencapsulation.setField(globalStateMgr, "globalTransactionMgr", mgr);
        Assertions.assertEquals(1, loadJob.idToTasks.size());
        loadJob.updateState(JobState.FINISHED);
        Assertions.assertEquals(JobState.FINISHED, loadJob.getState());
        Assertions.assertNotEquals(-1, (long) Deencapsulation.getField(loadJob, "finishTimestamp"));
        Assertions.assertEquals(100, (int) Deencapsulation.getField(loadJob, "progress"));
        Assertions.assertEquals(0, loadJob.idToTasks.size());
    }

    @Test
    public void testGetShowInfo() throws DdlException {
        TimeZone tz = TimeZone.getTimeZone(ZoneId.of("Asia/Shanghai"));
        new MockUp<TimeUtils>() {
            @Mock
            public TimeZone getTimeZone() {
                return tz;
            }
        };

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouseAllowNull(anyLong);
                minTimes = 0;
                result = warehouse;

                warehouse.getName();
                minTimes = 0;
                result = "test_wh";
            }
        };

        LoadJob loadJob = new BrokerLoadJob();
        List<Comparable> showInfo = loadJob.getShowInfo();
        Assertions.assertNotNull(showInfo);
        Comparable result = showInfo.get(showInfo.size() - 1);
        Assertions.assertEquals("test_wh", result);

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_NOTHING;
            }
        };

        showInfo = loadJob.getShowInfo();
        Assertions.assertNotNull(showInfo);
        result = showInfo.get(showInfo.size() - 1);
        Assertions.assertEquals("", result);
    }

    @Test
    public void testGetShowInfoMissingWarehouse() throws DdlException {
        TimeZone tz = TimeZone.getTimeZone(ZoneId.of("Asia/Shanghai"));
        new MockUp<TimeUtils>() {
            @Mock
            public TimeZone getTimeZone() {
                return tz;
            }
        };

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouseAllowNull(anyLong);
                minTimes = 0;
                result = null;
            }
        };

        LoadJob loadJob = new BrokerLoadJob();
        loadJob.setWarehouseId(1L);

        List<Comparable> showInfo = loadJob.getShowInfo();
        Assertions.assertEquals("Warehouse id: 1 not exist.", showInfo.get(showInfo.size() - 1));

        TLoadInfo loadInfo = loadJob.toThrift();
        Assertions.assertEquals("Warehouse id: 1 not exist.", loadInfo.getWarehouse());
    }

    @Test
    public void testToThrift() {
        LoadJob loadJob = new BrokerLoadJob();

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouseAllowNull(anyLong);
                minTimes = 0;
                result = warehouse;

                warehouse.getName();
                minTimes = 0;
                result = "test_wh";
            }
        };

        TLoadInfo loadInfo = loadJob.toThrift();
        Assertions.assertEquals("test_wh", loadInfo.getWarehouse());

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_NOTHING;
            }
        };

        loadInfo = loadJob.toThrift();
        Assertions.assertEquals("", loadInfo.getWarehouse());
    }

    @Test
    public void testToThrift_timestampMsFields() {
        // Regression coverage: BE materializes information_schema.loads DATETIME
        // columns from the *_ms fields. If a future change ever drops a setter,
        // the column silently falls back to the legacy UTC+8 string and loads in
        // non-Asia/Shanghai sessions go missing again.
        long createMs = 1_700_000_000_000L;
        long startMs = createMs + 1_000;
        long commitMs = createMs + 2_000;
        long finishMs = createMs + 3_000;

        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "createTimestamp", createMs);
        Deencapsulation.setField(loadJob, "loadStartTimestamp", startMs);
        Deencapsulation.setField(loadJob, "loadCommittedTimestamp", commitMs);
        Deencapsulation.setField(loadJob, "finishTimestamp", finishMs);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouseAllowNull(anyLong);
                minTimes = 0;
                result = null;
            }
        };

        TLoadInfo info = loadJob.toThrift();
        Assertions.assertEquals(createMs, info.getCreate_time_ms());
        Assertions.assertEquals(startMs, info.getLoad_start_time_ms());
        Assertions.assertEquals(commitMs, info.getLoad_commit_time_ms());
        Assertions.assertEquals(finishMs, info.getLoad_finish_time_ms());
        // Legacy strings must still be set so old BEs in a rolling upgrade keep working.
        Assertions.assertEquals(TimeUtils.longToTimeString(createMs), info.getCreate_time());
        Assertions.assertEquals(TimeUtils.longToTimeString(startMs), info.getLoad_start_time());
        Assertions.assertEquals(TimeUtils.longToTimeString(commitMs), info.getLoad_commit_time());
        Assertions.assertEquals(TimeUtils.longToTimeString(finishMs), info.getLoad_finish_time());

        // Sentinel path: unset timestamps must leave both the string and the ms
        // field unset so BE can fall back to NULL via its !__isset branch
        // instead of materializing the epoch.
        LoadJob unset = new BrokerLoadJob();
        TLoadInfo unsetInfo = unset.toThrift();
        Assertions.assertFalse(unsetInfo.isSetCreate_time_ms());
        Assertions.assertFalse(unsetInfo.isSetLoad_start_time_ms());
        Assertions.assertFalse(unsetInfo.isSetLoad_commit_time_ms());
        Assertions.assertFalse(unsetInfo.isSetLoad_finish_time_ms());
    }

    @Test
    public void testToThrift_jobColumns(@Mocked Coordinator coordinator, @Mocked DeleteStmt deleteStmt,
                                        @Mocked InsertStmt insertStmt, @Mocked GlobalTransactionMgr txnMgr) {
        TransactionState txnState = new TransactionState();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouseAllowNull(anyLong);
                minTimes = 0;
                result = null;

                globalStateMgr.getGlobalTransactionMgr();
                minTimes = 0;
                result = txnMgr;

                txnMgr.getTransactionState(anyLong, anyLong);
                minTimes = 0;
                result = txnState;
            }
        };

        // A broker load that has not started: no transaction yet, no table ids, no statement,
        // no client, nothing visible.
        LoadJob brokerJob = new BrokerLoadJob();
        TLoadInfo info = brokerJob.toThrift();
        Assertions.assertFalse(info.isSetTxn_id());
        Assertions.assertFalse(info.isSetTable_id());
        Assertions.assertFalse(info.isSetStatement());
        Assertions.assertFalse(info.isSetClient_ip());
        Assertions.assertFalse(info.isSetVisible_time_ms());
        Assertions.assertFalse(info.isSetBe_ids());
        Assertions.assertFalse(info.isSetError_code());
        Assertions.assertFalse(info.isSetCpu_ns());
        Assertions.assertFalse(info.isSetPeak_mem_bytes());
        Assertions.assertEquals(0L, info.getSink_bytes());

        // Once the job ran on two backends and its transaction became visible.
        brokerJob.initLoadProgress(new TUniqueId(1, 2), Sets.newHashSet(new TUniqueId(3, 4)), Lists.newArrayList(2L, 1L));
        Deencapsulation.setField(brokerJob, "state", JobState.FINISHED);
        Deencapsulation.setField(brokerJob, "visibleTimestamp", 1_700_000_000_000L);
        info = brokerJob.toThrift();
        Assertions.assertEquals("1,2", info.getBe_ids());
        Assertions.assertEquals(1_700_000_000_000L, info.getVisible_time_ms());

        // A FINISHED job restored from an edit log that predates visibleTimestamp only reports a
        // visible time when the transaction itself is VISIBLE: an INSERT whose publish wait timed
        // out is FINISHED while its transaction is still COMMITTED.
        Deencapsulation.setField(brokerJob, "visibleTimestamp", -1L);
        Deencapsulation.setField(brokerJob, "finishTimestamp", 1_700_000_000_500L);
        txnState.setTransactionStatus(TransactionStatus.COMMITTED);
        Assertions.assertFalse(brokerJob.toThrift().isSetVisible_time_ms());
        txnState.setTransactionStatus(TransactionStatus.VISIBLE);
        txnState.setFinishTime(1_700_000_000_400L);
        Assertions.assertEquals(1_700_000_000_400L, brokerJob.toThrift().getVisible_time_ms());
        txnState.setFinishTime(0L);
        Assertions.assertEquals(1_700_000_000_500L, brokerJob.toThrift().getVisible_time_ms());

        // INSERT jobs carry the target table, the statement kind and the submitting client.
        InsertLoadJob insertJob = new InsertLoadJob("insert_label", 1L, 7L, 9L, "load_id", "root",
                System.currentTimeMillis(), 10L, 0L, coordinator, null, "UPDATE", "10.0.0.1");
        info = insertJob.toThrift();
        Assertions.assertEquals(7L, info.getTable_id());
        Assertions.assertEquals(9L, info.getTxn_id());
        Assertions.assertEquals("UPDATE", info.getStatement());
        Assertions.assertEquals("10.0.0.1", info.getClient_ip());
        Assertions.assertFalse(info.isSetVisible_time_ms());

        // Statement classification used when the insert job is registered.
        Assertions.assertEquals("DELETE", InsertLoadJob.statementKindOf(deleteStmt, null));
        new Expectations() {
            {
                insertStmt.getQueryStatement();
                minTimes = 0;
                result = null;
            }
        };
        Assertions.assertEquals("INSERT_SELECT", InsertLoadJob.statementKindOf(insertStmt, null));
    }

    @Test
    public void testJoinForLoadsColumn() {
        Assertions.assertEquals("", LoadJob.joinForLoadsColumn(", ", Lists.newArrayList()));
        Assertions.assertEquals("1,2", LoadJob.joinForLoadsColumn(",", Lists.newArrayList(1L, 2L)));

        // 53 load ids of 36 characters fit a varchar(2048) column; further ids are left out
        // as a whole instead of being cut in the middle.
        String loadId = "0a1b2c3d-0000-0000-0000-00000000000f";
        String fits = LoadJob.joinForLoadsColumn(", ", Collections.nCopies(53, loadId));
        Assertions.assertEquals(53 * 36 + 52 * 2, fits.length());
        String capped = LoadJob.joinForLoadsColumn(", ", Collections.nCopies(80, loadId));
        Assertions.assertEquals(fits, capped);
        Assertions.assertTrue(capped.length() <= LoadJob.LOADS_VARCHAR_COLUMN_LENGTH);
        Assertions.assertTrue(capped.endsWith(loadId));

        // A broker load with that many load ids still exposes a value that fits.
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouseAllowNull(anyLong);
                minTimes = 0;
                result = null;
            }
        };
        LoadJob brokerJob = new BrokerLoadJob();
        Deencapsulation.setField(brokerJob, "loadIds", Lists.newArrayList(Collections.nCopies(80, loadId)));
        Assertions.assertEquals(fits, brokerJob.toThrift().getLoad_id());

        // A retry starts under load ids of its own.
        brokerJob.reset();
        Assertions.assertFalse(brokerJob.toThrift().isSetLoad_id());
    }

    @Test
    public void testCommitThrottleIsKeptOnTheJob() {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouseAllowNull(anyLong);
                minTimes = 0;
                result = null;
            }
        };
        LoadJob brokerJob = new BrokerLoadJob();

        TransactionState txnState = new TransactionState();
        txnState.setWriteEndTimeMs(1_700_000_000_000L);
        txnState.setAllowCommitTimeMs(1_700_000_000_300L);
        brokerJob.captureCommitThrottle(txnState);
        // The job does not find its transaction any more: it has been purged.
        Assertions.assertEquals(300L, brokerJob.toThrift().getCommit_throttle_ms());

        // A transaction the limiter never evaluated does not overwrite the value.
        brokerJob.captureCommitThrottle(new TransactionState());
        Assertions.assertEquals(300L, brokerJob.toThrift().getCommit_throttle_ms());
    }

    @Test
    public void testJsonOptionsEnvelope() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(LoadStmt.ENVELOPE, LoadStmt.ENVELOPE_DEBEZIUM);

        LoadJob loadJob = new BrokerLoadJob();
        loadJob.setJobProperties(properties);
        Assertions.assertEquals(LoadStmt.ENVELOPE_DEBEZIUM, loadJob.jsonOptions.envelope);

        // Mutually exclusive: json_root and envelope
        properties.put(LoadStmt.JSONROOT, "$.root");
        Assertions.assertThrows(DdlException.class, () -> loadJob.setJobProperties(properties));

        // Mutually exclusive: strip_outer_array and envelope
        properties.remove(LoadStmt.JSONROOT);
        properties.put(LoadStmt.STRIP_OUTER_ARRAY, "true");
        Assertions.assertThrows(DdlException.class, () -> loadJob.setJobProperties(properties));
    }
}
