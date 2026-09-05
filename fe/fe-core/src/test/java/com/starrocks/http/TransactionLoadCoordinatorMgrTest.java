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

package com.starrocks.http;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.rest.TransactionLoadCoordinatorMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.starrocks.http.TransactionLoadActionTest.newTxnStateWithCoordinator;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestMethodOrder(MethodName.class)
public class TransactionLoadCoordinatorMgrTest {
    private static long testDbId = 100L;
    private static String DB_NAME = "testDb";
    @Mocked
    private GlobalTransactionMgr globalTransactionMgr;
    private GlobalStateMgr globalStateMgr;
    private Database db;

    @BeforeEach
    public void setUp() throws Exception {
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        db = new Database(testDbId, DB_NAME);
        NodeMgr nodeMgr = new NodeMgr();
        SystemInfoService systemInfoService = new SystemInfoService();
        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;

                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };

        new Expectations(nodeMgr) {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        // init default warehouse
        globalStateMgr.getWarehouseMgr().initDefaultWarehouse();
        Backend backend1 = new Backend(1234, "localhost", 8040);
        backend1.setBePort(9300);
        backend1.setAlive(true);
        backend1.setHttpPort(9301);
        backend1.setDisks(new ImmutableMap.Builder<String, DiskInfo>().put("1", new DiskInfo("")).build());
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend1);
        // newInstance skips the constructor, so wire the mocked transaction mgr in explicitly:
        // getNodeFromTransactionState and transactionOwnedBy reach it via getGlobalTransactionMgr().
        Deencapsulation.setField(globalStateMgr, "globalTransactionMgr", globalTransactionMgr);
    }

    @Test
    public void transactionLoadCoordinatorMgrTest() throws Exception {
        String label = "label_transactionLoadLabelCacheTest";

        new Expectations(globalStateMgr) {
            {

                globalStateMgr.getLocalMetastore().getDb(DB_NAME);
                minTimes = 0;
                result = db;

                globalStateMgr.getLocalMetastore().getDb(anyString);
                minTimes = 0;
                result = null;
            }
        };

        new Expectations() {
            {
                globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                times = 1;
                result = newTxnStateWithCoordinator(-1, label, TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                        TransactionStatus.UNKNOWN, "localhost", 1234);
            }
        };

        TransactionLoadCoordinatorMgr cache = new TransactionLoadCoordinatorMgr();
        long value = 1234L;
        cache.put(label, value);
        assertEquals(value, cache.get(label, DB_NAME).getId());
        cache.remove(label);
        assertEquals(value, cache.get(label, DB_NAME).getId());

        try {
            cache.get(label, "empty_db");
        } catch (StarRocksException e) {
            Assertions.assertTrue(e.getMessage().contains("Can't find db[empty_db] " +
                    "for label[label_transactionLoadLabelCacheTest]. The db may be dropped."));
        }
    }


    @Test
    public void testAllocateRejectsRetryWhenTransactionLive() throws Exception {
        // A label whose FE transaction is still live (PREPARE/PREPARED/COMMITTED/VISIBLE/UNKNOWN)
        // must fail the retried BEGIN with the everyday LABEL_ALREADY_EXISTS error instead of
        // being sent to a new coordinator (which would orphan the original transaction's
        // LOAD/COMMIT routing) or bounced back to its unavailable one (a 307 loop). The cache
        // is left untouched, so later LOAD/COMMIT still resolve to the original owner.
        Backend backend2 = new Backend(5678, "otherhost", 8040);
        backend2.setBePort(9300);
        backend2.setAlive(false);
        backend2.setHttpPort(9301);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend2);
        // MockUp intercepts the real LocalMetastore instance returned by setUp's
        // getLocalMetastore expectation; a chained getDb record in an Expectations block would
        // be shadowed by that expectation and never match.
        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return DB_NAME.equals(dbName) ? db : null;
            }
        };
        // Build the states outside the Expectations block: the helper reaches
        // GlobalStateMgr.getCurrentState(), which is itself mocked, and recording it inside
        // would swallow the getLabelTransactionState result.
        TransactionState prepareTxn = newTxnStateWithCoordinator(1, "label_prepare",
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, TransactionStatus.PREPARE,
                "otherhost", 5678);
        TransactionState preparedTxn = newTxnStateWithCoordinator(2, "label_prepared",
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, TransactionStatus.PREPARED,
                "otherhost", 5678);
        TransactionState committedTxn = newTxnStateWithCoordinator(3, "label_committed",
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, TransactionStatus.COMMITTED,
                "otherhost", 5678);
        TransactionState visibleTxn = newTxnStateWithCoordinator(4, "label_visible",
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, TransactionStatus.VISIBLE,
                "otherhost", 5678);
        TransactionState unknownTxn = newTxnStateWithCoordinator(5, "label_unknown",
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, TransactionStatus.UNKNOWN,
                "otherhost", 5678);
        new Expectations() {
            {
                globalTransactionMgr.getLabelTransactionState(testDbId, "label_prepare");
                minTimes = 0;
                result = prepareTxn;

                globalTransactionMgr.getLabelTransactionState(testDbId, "label_prepared");
                minTimes = 0;
                result = preparedTxn;

                globalTransactionMgr.getLabelTransactionState(testDbId, "label_committed");
                minTimes = 0;
                result = committedTxn;

                globalTransactionMgr.getLabelTransactionState(testDbId, "label_visible");
                minTimes = 0;
                result = visibleTxn;

                globalTransactionMgr.getLabelTransactionState(testDbId, "label_unknown");
                minTimes = 0;
                result = unknownTxn;
            }
        };

        TransactionLoadCoordinatorMgr cache = new TransactionLoadCoordinatorMgr();

        // Cached unavailable coordinator + live transaction: terminal error, cache kept.
        for (String label : new String[] {"label_prepare", "label_prepared", "label_committed",
                "label_visible", "label_unknown"}) {
            cache.put(label, 5678L);
            Assertions.assertThrows(LabelAlreadyUsedException.class,
                    () -> cache.allocate(label, "default_warehouse", DB_NAME));
            Assertions.assertEquals(5678L, cache.getIfPresentForTest(label));
            // Still rejected on the next retry: no new coordinator was written.
            Assertions.assertThrows(LabelAlreadyUsedException.class,
                    () -> cache.allocate(label, "default_warehouse", DB_NAME));
        }

        // Cache miss + live transaction: rejected, nothing written.
        cache.remove("label_prepare");
        Assertions.assertNull(cache.getIfPresentForTest("label_prepare"));
        Assertions.assertThrows(LabelAlreadyUsedException.class,
                () -> cache.allocate("label_prepare", "default_warehouse", DB_NAME));
        Assertions.assertNull(cache.getIfPresentForTest("label_prepare"));

        // Cached node vanished from the cluster + live transaction: rejected, entry kept.
        cache.put("label_prepare", 99999L);
        Assertions.assertThrows(LabelAlreadyUsedException.class,
                () -> cache.allocate("label_prepare", "default_warehouse", DB_NAME));
        Assertions.assertEquals(99999L, cache.getIfPresentForTest("label_prepare"));
    }

    @Test
    public void testAllocateReassignsWhenTransactionTerminalOrAbsent() throws Exception {
        // Only ABORTED or absent transactions free the label: allocate drops the stale cache
        // entry and selects a live node. A cached alive node short-circuits without any
        // transaction lookup.
        Backend backend2 = new Backend(5678, "otherhost", 8040);
        backend2.setBePort(9300);
        backend2.setAlive(false);
        backend2.setHttpPort(9301);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend2);
        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return DB_NAME.equals(dbName) ? db : null;
            }
        };
        TransactionState abortedTxn = newTxnStateWithCoordinator(1, "label_aborted",
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, TransactionStatus.ABORTED,
                "otherhost", 5678);
        new Expectations() {
            {
                globalTransactionMgr.getLabelTransactionState(testDbId, "label_aborted");
                minTimes = 0;
                result = abortedTxn;

                globalTransactionMgr.getLabelTransactionState(testDbId, "label_gone");
                minTimes = 0;
                result = null; // no transaction for the label
            }
        };

        TransactionLoadCoordinatorMgr cache = new TransactionLoadCoordinatorMgr();

        // Cached alive node is returned as-is (no transaction lookup happens for it).
        cache.put("label_alive", 1234L);
        assertEquals(1234L, cache.allocate("label_alive", "default_warehouse", DB_NAME).getId());

        // ABORTED transaction on an unavailable coordinator: reassign and replace the entry.
        cache.put("label_aborted", 5678L);
        assertEquals(1234L, cache.allocate("label_aborted", "default_warehouse", DB_NAME).getId());
        assertEquals(1234L, cache.allocate("label_aborted", "default_warehouse", DB_NAME).getId());

        // Missing cached node with no transaction: reassign.
        cache.put("label_gone", 99999L);
        assertEquals(1234L, cache.allocate("label_gone", "default_warehouse", DB_NAME).getId());
    }

    @Test
    public void multiThreadWriteTransactionLoadCoordinatorMgrTest() throws Exception {
        TransactionLoadCoordinatorMgr cache = new TransactionLoadCoordinatorMgr();
        ExecutorService executor = Executors.newFixedThreadPool(5);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            final int threadId = i;
            final long value = (long) i;
            futures.add(executor.submit(() -> {
                String label = "label_" + threadId;
                cache.put(label, value);
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }
        executor.shutdown();

        new MockUp<TransactionLoadCoordinatorMgr>() {
            @Mock
            public @NonNull ComputeNode getNodeFromId(Long nodeId) {
                return new ComputeNode(nodeId, "", 0);
            }
        };

        for (int i = 0; i < 5; i++) {
            String label = "label_" + i;
            long expectedValue = (long) i;
            assertEquals(expectedValue, cache.get(label, "").getId());
        }
    }

}
