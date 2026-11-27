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

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.scheduler.DefaultWorkerProvider;
import com.starrocks.qe.scheduler.LazyWorkerProvider;
import com.starrocks.qe.scheduler.LocationRestrictedWorkerProvider;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Hits CoordinatorPreprocessor#getBrpcAddress success return and #captureWorkerProvider body
 * for FE incremental coverage (avoids relying on cluster worker maps / method-ref jacoco gaps).
 */
class CoordinatorPreprocessorCaptureWorkerProviderTest {

    @BeforeAll
    static void beforeAll() {
        UtFrameUtils.createMinStarRocksCluster();
    }

    private static ConnectContext newCtx() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        // mockJobSpec requires a non-null execution id
        ctx.setExecutionId(new TUniqueId(0xdeadbeef, 0xdeadbeef));
        return ctx;
    }

    private static DefaultWorkerProvider providerWithBackend(long id) {
        Backend be = new Backend(id, "127.0.0.1", 9050);
        be.setAlive(true);
        be.setBrpcPort(8060);
        be.setHttpPort(8040);
        ImmutableMap<Long, ComputeNode> map = ImmutableMap.of(id, be);
        return new DefaultWorkerProvider(map, ImmutableMap.of(), map, ImmutableMap.of(), false,
                WarehouseManager.DEFAULT_RESOURCE);
    }

    @Test
    void testGetBrpcAddressSuccessAndMissing() {
        final long workerId = 42L;
        DefaultWorkerProvider fakeProvider = providerWithBackend(workerId);

        ConnectContext ctx = newCtx();
        CoordinatorPreprocessor prep =
                new CoordinatorPreprocessor(Collections.emptyList(), Collections.emptyList(), ctx);
        Deencapsulation.setField(prep, "lazyWorkerProvider", LazyWorkerProvider.of(() -> fakeProvider));

        TNetworkAddress brpc = prep.getBrpcAddress(workerId);
        Assertions.assertNotNull(brpc);
        Assertions.assertEquals("127.0.0.1", brpc.getHostname());
        Assertions.assertEquals(8060, brpc.getPort());

        Assertions.assertNotNull(prep.getBrpcIpAddress(workerId));
        Assertions.assertNotNull(prep.getAddress(workerId));

        Assertions.assertThrows(NullPointerException.class, () -> prep.getBrpcAddress(-1L));
    }

    @Test
    void testCaptureWorkerProviderDirectInvoke() {
        ConnectContext ctx = newCtx();
        CoordinatorPreprocessor prep =
                new CoordinatorPreprocessor(Collections.emptyList(), Collections.emptyList(), ctx);

        WorkerProvider provider = prep.captureWorkerProvider();
        Assertions.assertNotNull(provider);
        Assertions.assertFalse(provider instanceof LocationRestrictedWorkerProvider);
    }

    @Test
    void testCaptureWorkerProviderWhenRestrictionDisabled() {
        boolean saved = Config.enable_connector_location_worker_restriction;
        Config.enable_connector_location_worker_restriction = false;
        try {
            ConnectContext ctx = newCtx();
            CoordinatorPreprocessor prep =
                    new CoordinatorPreprocessor(Collections.emptyList(), Collections.emptyList(), ctx);
            WorkerProvider provider = prep.captureWorkerProvider();
            Assertions.assertNotNull(provider);
            Assertions.assertFalse(provider instanceof LocationRestrictedWorkerProvider);
        } finally {
            Config.enable_connector_location_worker_restriction = saved;
        }
    }

    @Test
    void testCaptureWorkerProviderSkipsRestrictionInSharedDataMode() {
        ConnectContext ctx = newCtx();
        CoordinatorPreprocessor prep =
                new CoordinatorPreprocessor(Collections.emptyList(), Collections.emptyList(), ctx);
        new MockUp<RunMode>() {
            @Mock
            public static boolean isSharedDataMode() {
                return true;
            }
        };
        WorkerProvider provider = prep.captureWorkerProvider();
        Assertions.assertNotNull(provider);
        Assertions.assertFalse(provider instanceof LocationRestrictedWorkerProvider);
    }
}
