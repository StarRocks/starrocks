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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.task.LeaderTask;
import com.starrocks.task.LeaderTaskExecutor;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.BeginTransactionException;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class LoadJobTest {

    @BeforeClass
    public static void start() {
        MetricRepo.init();
    }

    @Test
    public void testGetDbNotExists(@Mocked GlobalStateMgr globalStateMgr) {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "dbId", 1L);
        new Expectations() {
            {
                globalStateMgr.getDb(1L);
                minTimes = 0;
                result = null;
            }
        };

        try {
            loadJob.getDb();
            Assert.fail();
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
            Assert.fail();
        } catch (DdlException e) {
        }
    }

    @Test
    public void testSetJobProperties() {
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(LoadStmt.TIMEOUT_PROPERTY, "1000");
        jobProperties.put(LoadStmt.MAX_FILTER_RATIO_PROPERTY, "0.1");
        jobProperties.put(LoadStmt.LOAD_MEM_LIMIT, "1024");
        jobProperties.put(LoadStmt.STRICT_MODE, "True");

        LoadJob loadJob = new BrokerLoadJob();
        try {
            loadJob.setJobProperties(jobProperties);
            Assert.assertEquals(1000, (long) Deencapsulation.getField(loadJob, "timeoutSecond"));
            Assert.assertEquals(0.1, Deencapsulation.getField(loadJob, "maxFilterRatio"), 0);
            Assert.assertEquals(1024, (long) Deencapsulation.getField(loadJob, "loadMemLimit"));
            Assert.assertTrue(Deencapsulation.getField(loadJob, "strictMode"));
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testExecute(@Mocked GlobalTransactionMgr globalTransactionMgr,
                            @Mocked LeaderTaskExecutor leaderTaskExecutor)
            throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException, DuplicatedRequestException {
        LoadJob loadJob = new BrokerLoadJob();
        new Expectations() {
            {
                globalTransactionMgr.beginTransaction(anyLong, Lists.newArrayList(), anyString, (TUniqueId) any,
                        (TransactionState.TxnCoordinator) any,
                        (TransactionState.LoadJobSourceType) any, anyLong, anyLong);
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
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals(JobState.LOADING, loadJob.getState());
        Assert.assertEquals(1, loadJob.getTransactionId());

    }

    @Test
    public void testProcessTimeoutWithCompleted() {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "state", JobState.FINISHED);

        loadJob.processTimeout();
        Assert.assertEquals(JobState.FINISHED, loadJob.getState());
    }

    @Test
    public void testProcessTimeoutWithIsCommitting() {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "isCommitting", true);
        Deencapsulation.setField(loadJob, "state", JobState.LOADING);

        loadJob.processTimeout();
        Assert.assertEquals(JobState.LOADING, loadJob.getState());
    }

    @Test
    public void testProcessTimeoutWithLongTimeoutSecond() {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "createTimestamp", System.currentTimeMillis());
        Deencapsulation.setField(loadJob, "timeoutSecond", 1000L);

        loadJob.processTimeout();
        Assert.assertEquals(JobState.PENDING, loadJob.getState());
    }

    @Test
    public void testProcessTimeout(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog) {
        LoadJob loadJob = new BrokerLoadJob();
        Deencapsulation.setField(loadJob, "timeoutSecond", 0);
        new Expectations() {
            {
                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        loadJob.processTimeout();
        Assert.assertEquals(JobState.CANCELLED, loadJob.getState());
    }

    @Test
    public void testUpdateStateToLoading() {
        LoadJob loadJob = new BrokerLoadJob();
        loadJob.updateState(JobState.LOADING);
        Assert.assertEquals(JobState.LOADING, loadJob.getState());
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
        Assert.assertEquals(1, loadJob.idToTasks.size());
        loadJob.updateState(JobState.FINISHED);
        Assert.assertEquals(JobState.FINISHED, loadJob.getState());
        Assert.assertNotEquals(-1, (long) Deencapsulation.getField(loadJob, "finishTimestamp"));
        Assert.assertEquals(100, (int) Deencapsulation.getField(loadJob, "progress"));
        Assert.assertEquals(0, loadJob.idToTasks.size());
    }
}
