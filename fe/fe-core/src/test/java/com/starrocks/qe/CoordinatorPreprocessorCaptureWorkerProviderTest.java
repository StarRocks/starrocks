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

import com.starrocks.common.Config;
import com.starrocks.qe.scheduler.LocationRestrictedWorkerProvider;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Covers CoordinatorPreprocessor address getters and captureWorkerProvider restriction wiring.
 */
class CoordinatorPreprocessorCaptureWorkerProviderTest {

    @BeforeAll
    static void beforeAll() {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Test
    void testGetWorkerAddressesAndMissingWorker() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        CoordinatorPreprocessor prep =
                new CoordinatorPreprocessor(Collections.emptyList(), Collections.emptyList(), ctx);

        WorkerProvider provider = prep.getWorkerProvider();
        Assertions.assertNotNull(provider);
        Assertions.assertFalse(provider.getAllWorkers().isEmpty());

        ComputeNode worker = provider.getAllWorkers().iterator().next();
        long workerId = worker.getId();

        // Hits getBrpcAddress / getBrpcIpAddress / getAddress success paths (incl. line 194).
        TNetworkAddress brpc = prep.getBrpcAddress(workerId);
        TNetworkAddress brpcIp = prep.getBrpcIpAddress(workerId);
        TNetworkAddress address = prep.getAddress(workerId);
        Assertions.assertNotNull(brpc);
        Assertions.assertNotNull(brpcIp);
        Assertions.assertNotNull(address);

        Assertions.assertThrows(NullPointerException.class, () -> prep.getBrpcAddress(-1L));
        Assertions.assertThrows(NullPointerException.class, () -> prep.getBrpcIpAddress(-1L));
        Assertions.assertThrows(NullPointerException.class, () -> prep.getAddress(-1L));
    }

    @Test
    void testCaptureWorkerProviderWhenRestrictionDisabled() {
        boolean saved = Config.enable_connector_location_worker_restriction;
        Config.enable_connector_location_worker_restriction = false;
        try {
            ConnectContext ctx = UtFrameUtils.createDefaultCtx();
            CoordinatorPreprocessor prep =
                    new CoordinatorPreprocessor(Collections.emptyList(), Collections.emptyList(), ctx);
            // Forces captureWorkerProvider() (lines around 242–257).
            WorkerProvider provider = prep.getWorkerProvider();
            Assertions.assertNotNull(provider);
            Assertions.assertFalse(provider instanceof LocationRestrictedWorkerProvider);
        } finally {
            Config.enable_connector_location_worker_restriction = saved;
        }
    }

    @Test
    void testCaptureWorkerProviderWithRestrictionEnabledEmptyScans() {
        boolean saved = Config.enable_connector_location_worker_restriction;
        Config.enable_connector_location_worker_restriction = true;
        try {
            ConnectContext ctx = UtFrameUtils.createDefaultCtx();
            CoordinatorPreprocessor prep =
                    new CoordinatorPreprocessor(Collections.emptyList(), Collections.emptyList(), ctx);
            WorkerProvider provider = prep.getWorkerProvider();
            Assertions.assertNotNull(provider);
            // No connector locations → unrestricted (not wrapped).
            Assertions.assertFalse(provider instanceof LocationRestrictedWorkerProvider);
        } finally {
            Config.enable_connector_location_worker_restriction = saved;
        }
    }

    @Test
    void testCaptureWorkerProviderSkipsRestrictionInSharedDataMode() {
        // Construct with shared-nothing factory, then mock RunMode so captureWorkerProvider
        // takes the shared-data early-return without needing a shared-data cluster.
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        CoordinatorPreprocessor prep =
                new CoordinatorPreprocessor(Collections.emptyList(), Collections.emptyList(), ctx);
        new MockUp<RunMode>() {
            @Mock
            public static boolean isSharedDataMode() {
                return true;
            }
        };
        WorkerProvider provider = prep.getWorkerProvider();
        Assertions.assertNotNull(provider);
        Assertions.assertFalse(provider instanceof LocationRestrictedWorkerProvider);
    }
}
