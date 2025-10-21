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

import com.starrocks.proto.PExecBatchPlanFragmentsResult;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.PExecBatchPlanFragmentsRequest;
import com.starrocks.rpc.RpcException;
import com.starrocks.thrift.TNetworkAddress;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.starrocks.utframe.MockedBackend.MockPBackendService;

public class SingleNodeScheduleTest extends SchedulerTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        singleNodeTest = true;
        SchedulerTestBase.beforeClass();
    }

    @AfterAll
    public static void afterClass() {
        SchedulerTestBase.afterClass();
        singleNodeTest = false;
    }

    @Test
    public void testBasicSingleNodeSchedule() throws Exception {
        AtomicBoolean usedBatchInterface = new AtomicBoolean(false);

        setBackendService(address -> new MockPBackendService() {
            @Override
            public Future<PExecBatchPlanFragmentsResult> execBatchPlanFragmentsAsync(
                    PExecBatchPlanFragmentsRequest request) {
                usedBatchInterface.set(true);
                return super.execBatchPlanFragmentsAsync(request);
            }
        });

        String sql = "select count(1) from lineitem";
        DefaultCoordinator coordinator = startScheduling(sql);

        Assertions.assertTrue(coordinator.getExecStatus().ok());

        Assertions.assertTrue(usedBatchInterface.get(),
                "single node scheduler should call execBatchPlanFragmentsAsync");
    }

    @Test
    public void testThrowRpcException() throws Exception {
        new MockUp<BackendServiceClient>() {
            @Mock
            Future<PExecBatchPlanFragmentsResult> execBatchPlanFragmentsAsync(TNetworkAddress address,
                                                                              byte[] serializedRequest, String protocol)
                    throws RpcException {
                throw new RpcException("test rpc exeception");
            }
        };

        String sql = "select * from lineitem l join orders o on l.l_orderkey = o.o_orderkey";
        Assertions.assertThrows(RpcException.class, () -> startScheduling(sql), " should throw RpcException for retry");
    }

}

