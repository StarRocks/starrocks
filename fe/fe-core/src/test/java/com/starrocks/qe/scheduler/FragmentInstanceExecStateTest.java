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

import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TUniqueId;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wildfly.common.Assert;

public class FragmentInstanceExecStateTest {
    private FragmentInstanceExecState state;
    private ComputeNode computeNode;
    private long startTime;
    private long lastHeartbeatMissTime;

    @Mocked
    private JobSpec jobSpec;

    @BeforeEach
    public void setUp() {
        new Expectations() {
            {
                jobSpec.getLoadJobId();
                result = 1000L;
            }
        };

        startTime = System.currentTimeMillis() - 10 * 60 * 60 * 1000;
        lastHeartbeatMissTime = System.currentTimeMillis() - 3 * 5000;

        computeNode = new ComputeNode(1L, "127.0.0.1", 80);
        computeNode.setLastStartTime(startTime);
        computeNode.setLastMissingHeartbeatTime(lastHeartbeatMissTime);
        TExecPlanFragmentParams request = new TExecPlanFragmentParams();
        TPlanFragmentExecParams params = new com.starrocks.thrift.TPlanFragmentExecParams();
        params.fragment_instance_id = new TUniqueId(0x33, 0x0);
        request.setParams(params);

        state = FragmentInstanceExecState.createExecution(jobSpec, null, 0, request, computeNode);
    }

    @Test
    public void testIsBackendStateHealthy() {
        // everything is ok
        computeNode.setLastStartTime(startTime);
        computeNode.setLastMissingHeartbeatTime(lastHeartbeatMissTime);
        Assert.assertTrue(state.isBackendStateHealthy());

        // mock restart of backend
        computeNode.setLastStartTime(System.currentTimeMillis());
        computeNode.setLastMissingHeartbeatTime(lastHeartbeatMissTime);
        Assert.assertFalse(state.isBackendStateHealthy());

        // mock missing heartbeat
        computeNode.setLastStartTime(startTime);
        computeNode.setLastMissingHeartbeatTime(System.currentTimeMillis());
        state.getWorker().setLastMissingHeartbeatTime(System.currentTimeMillis());
        Assert.assertFalse(state.isBackendStateHealthy());
    }
}
