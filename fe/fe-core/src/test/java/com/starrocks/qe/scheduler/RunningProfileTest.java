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
package com.starrocks.qe.scheduler;

import com.starrocks.common.FeConstants;
import com.starrocks.common.util.Counter;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RunningProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.thrift.TCounterAggregateType;
import com.starrocks.thrift.TCounterMergeType;
import com.starrocks.thrift.TCounterStrategy;
import com.starrocks.thrift.TFragmentProfile;
import com.starrocks.thrift.TRuntimeProfileTree;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

public class RunningProfileTest extends SchedulerTestBase {
    private static int runtimeProfileReportInterval;

    @BeforeAll
    public static void beforeClass() throws Exception {
        SchedulerTestBase.beforeClass();
        connectContext.getSessionVariable().setEnableQueryProfile(true);
        runtimeProfileReportInterval = connectContext.getSessionVariable().getRuntimeProfileReportInterval();
        connectContext.getSessionVariable().setRuntimeProfileReportInterval(0);
        FeConstants.runningUnitTest = false;
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableQueryProfile(false);
        connectContext.getSessionVariable().setRuntimeProfileReportInterval(runtimeProfileReportInterval);
        SchedulerTestBase.afterClass();
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testTriggerRunningProfile() throws Exception {
        int oldValue = connectContext.getSessionVariable().getRuntimeProfileReportInterval();
        connectContext.getSessionVariable().setRuntimeProfileReportInterval(0);
        String sql = "SELECT COUNT(*)  FROM lineitem JOIN orders ON l_orderkey * 2 = o_orderkey + 1 " +
                "GROUP BY l_shipmode, l_shipinstruct, o_orderdate, o_orderstatus;";
        DefaultCoordinator scheduler = startScheduling(sql);
        List<Integer> executionIndexes = scheduler.getExecutionDAG().getExecutions().stream()
                .map(FragmentInstanceExecState::getIndexInJob)
                .collect(Collectors.toList());
        int fragmentInstanceCount = executionIndexes.size();

        final int threadCountPerInstance = 1;
        ExecutorService executor = Executors.newFixedThreadPool(threadCountPerInstance * fragmentInstanceCount);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < fragmentInstanceCount * threadCountPerInstance; i++) {
            final int indexInJob = i % fragmentInstanceCount;
            TUniqueId instanceId = scheduler.getFragmentInstanceInfos().get(indexInJob).getInstanceId();
            TUniqueId queryId = scheduler.getQueryId();
            final boolean isDone = false;
            futures.add(executor.submit(() -> {
                TFragmentProfile request = new TFragmentProfile();
                request.setDone(isDone);
                request.setProfile(generateRuntimeProfile(isDone));
                request.setFragment_instance_id(instanceId);
                request.setQuery_id(queryId);
                TStatus status = RunningProfileManager.getInstance().asyncProfileReport(request);
                Assertions.assertEquals(status.status_code, TStatusCode.OK);
            }));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();
        String queryId = DebugUtil.printId(scheduler.getQueryId());
        String profile = ProfileManager.getInstance().getProfile(queryId);
        Assertions.assertNotNull(profile);
        Assertions.assertFalse(profile.contains("IsProfileAsync"));
        Assertions.assertTrue(profile.contains(" Fragment 0:"));
        Assertions.assertTrue(profile.contains(" Fragment 1:"));
        connectContext.getSessionVariable().setRuntimeProfileReportInterval(oldValue);
    }

    @Test
    public void testUpdateProfileConcurrently() throws Exception {
        String sql = "select * from lineitem";
        DefaultCoordinator scheduler = startScheduling(sql);
        connectContext.getExecutor().setCoord(scheduler);
        connectContext.getExecutor().tryProcessProfileAsync(null, 0);

        List<Integer> executionIndexes = scheduler.getExecutionDAG().getExecutions().stream()
                .map(FragmentInstanceExecState::getIndexInJob)
                .collect(Collectors.toList());
        int fragmentInstanceCount = executionIndexes.size();
        // threads for each fragment instance to mock duplicated updateFragmentExecStatus and some of them are not EOS.
        final int threadCountPerInstance = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCountPerInstance * fragmentInstanceCount);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < fragmentInstanceCount * threadCountPerInstance; i++) {
            final int indexInJob = i % fragmentInstanceCount;
            TUniqueId instanceId = scheduler.getFragmentInstanceInfos().get(indexInJob).getInstanceId();
            TUniqueId queryId = connectContext.getExecutionId();
            // only the last 1/4 is isDone
            final boolean isDone = (i > fragmentInstanceCount * threadCountPerInstance * 3 / 4);
            futures.add(executor.submit(() -> {
                TFragmentProfile request = new TFragmentProfile();
                request.setDone(isDone);
                request.setProfile(generateRuntimeProfile(isDone));
                request.setFragment_instance_id(instanceId);
                request.setQuery_id(queryId);
                TStatus status = RunningProfileManager.getInstance().asyncProfileReport(request);
            }));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();
        String queryId = DebugUtil.printId(connectContext.getExecutionId());
        String profile = null;
        for (int i = 0; i < 3; i++) {
            profile = ProfileManager.getInstance().getProfile(queryId);
            if (profile == null) {
                sleep(500);
            }
        }
        Assertions.assertNotNull(profile);
        Assertions.assertTrue(profile.contains("IsProfileAsync"));
    }

    @Test
    public void testUpdateProfileFail() throws Exception {
        String sql = "select * from lineitem";
        DefaultCoordinator scheduler = startScheduling(sql);
        connectContext.getExecutor().setCoord(scheduler);

        TUniqueId instanceId = scheduler.getFragmentInstanceInfos().get(0).getInstanceId();
        TUniqueId queryId = new TUniqueId(1, 3);

        TFragmentProfile request = new TFragmentProfile();
        request.setDone(false);
        request.setProfile(generateRuntimeProfile(false));
        request.setQuery_id(queryId);
        TStatus status = RunningProfileManager.getInstance().asyncProfileReport(request);
        Assertions.assertEquals(status.status_code, TStatusCode.NOT_FOUND);

        request.setQuery_id(connectContext.getExecutionId());
        request.setFragment_instance_id(new TUniqueId(4, 5));
        status = RunningProfileManager.getInstance().asyncProfileReport(request);
        Assertions.assertEquals(status.status_code, TStatusCode.NOT_FOUND);
    }

    private TRuntimeProfileTree generateRuntimeProfile(boolean isfinal) {
        long value = isfinal ? 5000000000L : 1000000000L;
        RuntimeProfile profile1 = new RuntimeProfile("profile");
        {
            TCounterStrategy strategy1 = new TCounterStrategy();
            strategy1.aggregate_type = TCounterAggregateType.AVG;
            strategy1.merge_type = TCounterMergeType.MERGE_ALL;

            Counter time1 = profile1.addCounter("time1", TUnit.TIME_NS, strategy1);
            time1.setValue(value);
        }

        return profile1.toThrift();
    }
}
