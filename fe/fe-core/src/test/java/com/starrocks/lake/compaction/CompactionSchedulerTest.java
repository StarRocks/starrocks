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

package com.starrocks.lake.compaction;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.DatabaseTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CompactionSchedulerTest {

    private final long dbId = 9000L;
    private final long transactionId = 12345L;

    @Mocked
    private DatabaseTransactionMgr dbTransactionMgr;

    @Before
    public void setUp() {
    }

    @Test
    public void testBeginTransactionSucceedWithSmallerStreamLoadTimeout() {
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().addDatabaseTransactionMgr(dbId);
        new Expectations() {
            {
                try {
                    dbTransactionMgr.beginTransaction(
                            (List<Long>) any, anyString, (TUniqueId) any, (TransactionState.TxnCoordinator) any,
                            (TransactionState.LoadJobSourceType) any, anyLong, anyLong, anyLong
                    );
                } catch (Exception e) {
                    // skip
                }
                result = transactionId;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(String warehouseName) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }

            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }

            @Mock
            public List<Long> getAllComputeNodeIds(long warehouseId) {
                return Lists.newArrayList(1L);
            }

            @Mock
            public Long getComputeNodeId(String warehouseName, LakeTablet tablet) {
                return 1L;
            }

            @Mock
            public Long getComputeNodeId(Long warehouseId, LakeTablet tablet) {
                return 1L;
            }

            @Mock
            public ComputeNode getAllComputeNodeIdsAssignToTablet(Long warehouseId, LakeTablet tablet) {
                return new ComputeNode(1L, "127.0.0.1", 9030);
            }

            @Mock
            public ComputeNode getAllComputeNodeIdsAssignToTablet(String warehouseName, LakeTablet tablet) {
                return null;
            }

            @Mock
            public ImmutableMap<Long, ComputeNode> getComputeNodesFromWarehouse(long warehouseId) {
                return ImmutableMap.of(1L, new ComputeNode(1L, "127.0.0.1", 9030));
            }
        };

        // default value
        Config.lake_compaction_default_timeout_second = 86400;
        // value smaller than `lake_compaction_default_timeout_second`
        // expect not affect lake compaction's  transaction operation
        Config.max_stream_load_timeout_second = 64800;
        CompactionMgr compactionManager = new CompactionMgr();
        CompactionScheduler compactionScheduler =
                new CompactionScheduler(compactionManager, GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr(), GlobalStateMgr.getCurrentState());
        PartitionIdentifier partitionIdentifier = new PartitionIdentifier(dbId, 2, 3);
        try {
            assertEquals(transactionId, compactionScheduler.beginTransaction(partitionIdentifier));
        } catch (Exception e) {
            Assert.fail("Transaction failed for lake compaction");
        }
    }
}
