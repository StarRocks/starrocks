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
import com.starrocks.http.rest.TransactionLoadLabelCache;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
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
public class TransactionLoadLabelCacheTest {
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
    public void transactionLoadLabelCacheTest() throws Exception {
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
        TransactionLoadLabelCache cache = new TransactionLoadLabelCache();
        String label = "label_transactionLoadLabelCacheTest";
        long value = 1234L;
        cache.put(label, value);
        assertEquals(value, cache.get(label));
        cache.remove(label);
        assertEquals(null, cache.get(label));

        new Expectations() {
            {
                globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                times = 1;
                result = newTxnStateWithCoordinator(-1,
                        label, TransactionState.LoadJobSourceType.BACKEND_STREAMING, TransactionStatus.UNKNOWN, "localhost");
            }
        };
        assertEquals(value, cache.getOrResolveCoordinator(label, DB_NAME));
        try {
            cache.getOrResolveCoordinator(label, "empty_db");
        } catch (StarRocksException e) {
            Assertions.assertTrue(e.getMessage().contains("The DB passed in by the Transaction with db[empty_db] " +
                    "and label[label_transactionLoadLabelCacheTest] has been deleted."));
        }

        //test shared data mode
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new Expectations() {
            {
                globalTransactionMgr.getLabelTransactionState(anyLong, anyString);
                times = 1;
                result = newTxnStateWithCoordinator(-1,
                        label, TransactionState.LoadJobSourceType.BACKEND_STREAMING, TransactionStatus.UNKNOWN, "localhost_cn");
            }
        };

        ComputeNode computeNode = new ComputeNode(1234, "localhost_cn", 8040);
        computeNode.setBePort(9300);
        computeNode.setAlive(true);
        computeNode.setHttpPort(9301);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNode(computeNode);
        assertEquals(value, cache.getOrResolveCoordinator(label, DB_NAME));

    }

    @Test
    public void multiThreadWriteTransactionLoadLabelCacheTest() throws Exception {
        TransactionLoadLabelCache cache = new TransactionLoadLabelCache();
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

        for (int i = 0; i < 5; i++) {
            String label = "label_" + i;
            long expectedValue = (long) i;
            assertEquals(expectedValue, cache.get(label));
        }
    }

}
