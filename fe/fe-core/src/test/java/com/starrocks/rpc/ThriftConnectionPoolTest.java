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

package com.starrocks.rpc;

import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TNetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;

public class ThriftConnectionPoolTest {
    // Only TCP connects are needed, and the kernel completes those from the backlog without an
    // accept(), so a bare listening socket stands in for the peer.
    private ServerSocket peer;
    private TNetworkAddress address;

    @BeforeEach
    public void setUp() throws Exception {
        peer = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        address = new TNetworkAddress(peer.getInetAddress().getHostAddress(), peer.getLocalPort());
    }

    @AfterEach
    public void tearDown() throws Exception {
        peer.close();
    }

    private static boolean isOpen(FrontendService.Client client) {
        return client.getOutputProtocol().getTransport().isOpen();
    }

    @Test
    public void testFrontendMaxIdleLeavesAMarginBeforeTheServerTimeout() {
        Assertions.assertEquals(4000L, ThriftConnectionPool.frontendMaxIdleMs(5000));
        Assertions.assertEquals(2880000L, ThriftConnectionPool.frontendMaxIdleMs(3600000));
        // A server that never times out idle connections needs no limit.
        Assertions.assertEquals(0L, ThriftConnectionPool.frontendMaxIdleMs(0));
        Assertions.assertEquals(0L, ThriftConnectionPool.frontendMaxIdleMs(-1));
    }

    @Test
    public void testConnectionIdlePastTheLimitIsReplacedOnBorrow() throws Exception {
        ThriftConnectionPool<FrontendService.Client> pool =
                new ThriftConnectionPool<>("FrontendService", ThriftConnectionPool.backendConfig, 1000, 1);
        try {
            FrontendService.Client first = pool.borrowObject(address);
            pool.returnObject(address, first);
            // Sleeping can only make the connection idler, so this cannot flake.
            Thread.sleep(50);

            FrontendService.Client second = pool.borrowObject(address);
            Assertions.assertNotSame(first, second);
            Assertions.assertFalse(isOpen(first), "the stale connection must be closed, not leaked");
            Assertions.assertTrue(isOpen(second));
            pool.returnObject(address, second);
        } finally {
            pool.clearPool(address);
        }
    }

    @Test
    public void testConnectionIsReusedWithoutAnIdleLimit() throws Exception {
        ThriftConnectionPool<FrontendService.Client> pool =
                new ThriftConnectionPool<>("FrontendService", ThriftConnectionPool.backendConfig, 1000);
        try {
            FrontendService.Client first = pool.borrowObject(address);
            pool.returnObject(address, first);
            Thread.sleep(50);

            Assertions.assertSame(first, pool.borrowObject(address));
            pool.returnObject(address, first);
        } finally {
            pool.clearPool(address);
        }
    }

    @Test
    public void testIdleLimitDoesNotLeakIntoTheSharedConfig() {
        new ThriftConnectionPool<FrontendService.Client>("FrontendService", ThriftConnectionPool.backendConfig, 1000,
                1);
        // backendPool shares this config and its peers (BEs) do not close idle connections.
        Assertions.assertFalse(ThriftConnectionPool.backendConfig.getTestOnBorrow());
    }
}
