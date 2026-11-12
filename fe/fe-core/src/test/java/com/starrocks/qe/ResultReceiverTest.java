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

package com.starrocks.qe;

import com.starrocks.common.Status;
import com.starrocks.proto.PFetchDataResult;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.PBackendService;
import com.starrocks.rpc.PFetchDataRequest;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;

import static com.starrocks.utframe.MockedBackend.MockPBackendService;

public class ResultReceiverTest {
    private static final long BACKEND_ID = 10003L;
    private static final TNetworkAddress ADDRESS = new TNetworkAddress("127.0.0.1", 9050);

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Test
    public void testFetchFailureStartedBeforeRecoveryIsIgnored(@Mocked SystemInfoService systemInfoService)
            throws Exception {
        ComputeNode node = mockNode(systemInfoService);
        mockRpcFailure();
        // The restart is observed after this request was sent, so the failure it reports is stale.
        node.setLastRecoveryTime(System.currentTimeMillis() + 60_000);
        try {
            new ResultReceiver(new TUniqueId(), BACKEND_ID, ADDRESS, 10_000).getNext(new Status());
            Assertions.assertFalse(SimpleScheduler.isInBlocklist(BACKEND_ID));
        } finally {
            SimpleScheduler.removeFromBlocklist(BACKEND_ID);
        }
    }

    @Test
    public void testFetchFailureStartedAfterRecoveryIsRecorded(@Mocked SystemInfoService systemInfoService)
            throws Exception {
        ComputeNode node = mockNode(systemInfoService);
        mockRpcFailure();
        node.setLastRecoveryTime(System.currentTimeMillis() - 60_000);
        try {
            new ResultReceiver(new TUniqueId(), BACKEND_ID, ADDRESS, 10_000).getNext(new Status());
            Assertions.assertTrue(SimpleScheduler.isInBlocklist(BACKEND_ID));
        } finally {
            SimpleScheduler.removeFromBlocklist(BACKEND_ID);
        }
    }

    private ComputeNode mockNode(SystemInfoService systemInfoService) {
        ComputeNode node = new ComputeNode(BACKEND_ID, ADDRESS.hostname, ADDRESS.port);
        node.setAlive(true);
        new Expectations() {
            {
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = systemInfoService;
                systemInfoService.getBackendOrComputeNode(BACKEND_ID);
                result = node;
            }
        };
        return node;
    }

    private void mockRpcFailure() {
        new MockUp<BrpcProxy>() {
            @Mock
            public PBackendService getBackendService(TNetworkAddress address) {
                return new MockPBackendService() {
                    @Override
                    public Future<PFetchDataResult> fetchDataAsync(PFetchDataRequest request) {
                        return submit(() -> {
                            throw new RpcException("connection refused");
                        });
                    }
                };
            }
        };
    }
}
