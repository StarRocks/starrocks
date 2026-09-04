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
    public void testAllocateReallocatesWhenCachedNodeUnavailable() throws Exception {
        // A shutting-down BE reports SHUTDOWN in its heartbeat, so FE marks it not alive and
        // isAvailable() turns false. allocate() must drop the stale cache entry and pick another
        // node instead of routing the BEGIN back to the shutting-down coordinator — but only
        // once the label's transaction is no longer live on that BE.
        Backend backend2 = new Backend(5678, "otherhost", 8040);
        backend2.setBePort(9300);
        backend2.setAlive(false);
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
        // Build the state outside the Expectations block: the helper reaches
        // GlobalStateMgr.getCurrentState(), which is itself mocked, and recording it inside
        // would swallow the getLabelTransactionState result.
        // ABORTED/COMMITTED/VISIBLE/UNKNOWN all mean the cached BE no longer serves the
        // label (PREPARED+ removed its stream context there), so allocate must reassign.
        TransactionState abortedTxn = newTxnStateWithCoordinator(1, "label_unavailable",
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, TransactionStatus.ABORTED,
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
                globalTransactionMgr.getLabelTransactionState(testDbId, "label_unavailable");
                minTimes = 0;
                result = abortedTxn;


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

        // Cached alive node is returned as-is (no transaction lookup).
        cache.put("label_alive", 1234L);
        assertEquals(1234L, cache.allocate("label_alive", "default_warehouse", DB_NAME).getId());

        // Cached unavailable node with no live transaction is dropped and a new node is allocated.
        cache.put("label_unavailable", 5678L);
        assertEquals(1234L, cache.allocate("label_unavailable", "default_warehouse", DB_NAME).getId());
        // The stale entry has been replaced by the new allocation.
        assertEquals(1234L, cache.allocate("label_unavailable", "default_warehouse", DB_NAME).getId());

        // Terminal/unknown states reassign too.
        cache.put("label_committed", 5678L);
        assertEquals(1234L, cache.allocate("label_committed", "default_warehouse", DB_NAME).getId());
        cache.put("label_visible", 5678L);
        assertEquals(1234L, cache.allocate("label_visible", "default_warehouse", DB_NAME).getId());
        cache.put("label_unknown", 5678L);
        assertEquals(1234L, cache.allocate("label_unknown", "default_warehouse", DB_NAME).getId());
    }

    @Test
    public void testAllocateReassignsWhenCachedNodeMissing() throws Exception {
        // A cached node that vanished from the cluster cannot even be looked up; the entry
        // must be treated as stale and a live node allocated instead.
        TransactionLoadCoordinatorMgr cache = new TransactionLoadCoordinatorMgr();
        cache.put("label_missing", 99999L);
        assertEquals(1234L, cache.allocate("label_missing", "default_warehouse", DB_NAME).getId());
        assertEquals(1234L, cache.allocate("label_missing", "default_warehouse", DB_NAME).getId());
    }

    @Test
    public void testAllocateKeepsUnavailableNodeWhileTransactionLive() throws Exception {
        // A retried BEGIN for a label that already began on the (now unavailable) BE must keep
        // routing there: the retry is served idempotently by the BE that owns the stream
        // context, and re-allocating would misroute the retry and later LOAD/COMMIT to a BE
        // without the context. The entry is only reassigned once the transaction is aborted.
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
        // Build the state outside the Expectations block: the helper reaches
        // GlobalStateMgr.getCurrentState(), which is itself mocked, and recording it inside
        // would swallow the getLabelTransactionState result.
        TransactionState liveTxn = newTxnStateWithCoordinator(2, "label_live",
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, TransactionStatus.PREPARE,
                "otherhost", 5678);
        new Expectations() {
            {
                globalTransactionMgr.getLabelTransactionState(testDbId, "label_live");
                times = 2; // allocate is called twice; each lookup must hit the live txn
                result = liveTxn;
            }
        };

        TransactionLoadCoordinatorMgr cache = new TransactionLoadCoordinatorMgr();
        cache.put("label_live", 5678L);
        // The transaction is live on the unavailable BE: keep the cached coordinator.
        assertEquals(5678L, cache.allocate("label_live", "default_warehouse", DB_NAME).getId());
        assertEquals(5678L, cache.allocate("label_live", "default_warehouse", DB_NAME).getId());
    }

    @Test
    public void testAllocateReassignsWhenTransactionPrepared() throws Exception {
        // PREPARED and beyond no longer rely on the stream context: the BE removes it in
        // _commit_transaction even when prepare=true, and retries are txn-id based, so the
        // cached coordinator is reassigned like any other non-PREPARE state.
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
        TransactionState preparedTxn = newTxnStateWithCoordinator(6, "label_prepared",
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, TransactionStatus.PREPARED,
                "otherhost", 5678);
        new Expectations() {
            {
                globalTransactionMgr.getLabelTransactionState(testDbId, "label_prepared");
                minTimes = 0;
                result = preparedTxn;
            }
        };

        TransactionLoadCoordinatorMgr cache = new TransactionLoadCoordinatorMgr();
        cache.put("label_prepared", 5678L);
        assertEquals(1234L, cache.allocate("label_prepared", "default_warehouse", DB_NAME).getId());
        assertEquals(1234L, cache.allocate("label_prepared", "default_warehouse", DB_NAME).getId());
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
