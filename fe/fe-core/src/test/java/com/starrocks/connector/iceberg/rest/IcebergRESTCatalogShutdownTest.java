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

package com.starrocks.connector.iceberg.rest;

import com.google.common.collect.ImmutableMap;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class IcebergRESTCatalogShutdownTest {
    private final List<Object> initialized = new CopyOnWriteArrayList<>();
    private final List<Object> closed = new CopyOnWriteArrayList<>();
    private final AtomicInteger loads = new AtomicInteger();
    private volatile CountDownLatch rebuildStarted;
    private volatile CountDownLatch releaseRebuild;

    @BeforeEach
    public void setUp() {
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return new Frontend(FrontendNodeType.FOLLOWER, "fe1", "127.0.0.1", 9010);
            }
        };
        new MockUp<RESTSessionCatalog>() {
            @Mock
            public void initialize(Invocation invocation, String name, Map<String, String> properties)
                    throws InterruptedException {
                initialized.add(invocation.getInvokedInstance());
                if (initialized.size() > 1 && rebuildStarted != null) {
                    rebuildStarted.countDown();
                    Assertions.assertTrue(releaseRebuild.await(10, TimeUnit.SECONDS));
                }
            }

            @Mock
            public Map<String, String> loadNamespaceMetadata(SessionCatalog.SessionContext context, Namespace ns) {
                if (loads.getAndIncrement() == 0) {
                    throw new NotAuthorizedException("Not authorized: %s", "token expired");
                }
                return ImmutableMap.of("location", "s3://bucket/db1");
            }

            @Mock
            public void close(Invocation invocation) {
                closed.add(invocation.getInvokedInstance());
            }
        };
    }

    private static IcebergRESTCatalog newCatalog() {
        return new IcebergRESTCatalog("rest_catalog", new Configuration(), ImmutableMap.of(
                "iceberg.catalog.security", "oauth2",
                "iceberg.catalog.oauth2.credential", "id:secret"));
    }

    @Test
    public void testShutdownClosesSessionCatalogReplacedByAuthRecovery() {
        IcebergRESTCatalog catalog = newCatalog();
        Assertions.assertEquals("s3://bucket/db1", catalog.getDB(new ConnectContext(), "db1").getLocation());
        Assertions.assertEquals(2, initialized.size());
        Assertions.assertTrue(closed.isEmpty(), "the replaced session catalog stays open while the catalog lives");

        catalog.shutdown();

        Assertions.assertEquals(initialized, closed);
    }

    @Test
    public void testShutdownWaitsForInProgressRebuildAndClosesIt() throws Exception {
        rebuildStarted = new CountDownLatch(1);
        releaseRebuild = new CountDownLatch(1);
        IcebergRESTCatalog catalog = newCatalog();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> query = pool.submit(() -> catalog.getDB(new ConnectContext(), "db1"));
            Assertions.assertTrue(rebuildStarted.await(10, TimeUnit.SECONDS));
            AtomicReference<Thread> dropThread = new AtomicReference<>();
            Future<?> drop = pool.submit(() -> {
                dropThread.set(Thread.currentThread());
                catalog.shutdown();
            });
            // Deterministic: shutdown must block on the catalog monitor held by the in-progress rebuild.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while ((dropThread.get() == null || dropThread.get().getState() != Thread.State.BLOCKED)
                    && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Assertions.assertEquals(Thread.State.BLOCKED, dropThread.get().getState(),
                    "shutdown must wait for the rebuild instead of racing it");
            Assertions.assertFalse(drop.isDone());
            Assertions.assertTrue(closed.isEmpty());

            releaseRebuild.countDown();
            query.get(10, TimeUnit.SECONDS);
            drop.get(10, TimeUnit.SECONDS);

            Assertions.assertEquals(2, initialized.size());
            Assertions.assertEquals(initialized, closed);
        } finally {
            releaseRebuild.countDown();
            pool.shutdownNow();
        }
    }
}
