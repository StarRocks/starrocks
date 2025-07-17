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
