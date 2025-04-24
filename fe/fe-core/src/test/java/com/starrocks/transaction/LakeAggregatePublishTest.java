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


package com.starrocks.transaction;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.DefaultWarehouse;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
//import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LakeAggregatePublishTest {
    private static final Logger LOG = LogManager.getLogger(LakeAggregatePublishTest.class);

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private TransactionState.TxnCoordinator transactionSource =
            new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localfe");
    
    private static final String DB = "db_for_test";
    private static final String TABLE = "table_for_test";

    @BeforeClass
    public static void setUp() throws Exception {
        new MockUp<PublishVersionDaemon>() {
            @Mock
            public void runOneCycle() {

            }
        };

        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB);

        String sql = "create table " + TABLE +
                " (dt date NOT NULL, pk bigint NOT NULL, v0 string not null) DUPLICATE KEY (dt, pk) " +
                "PARTITION BY RANGE(`dt`) (\n" +
                "    PARTITION p20210820 VALUES [('2021-08-20'), ('2021-08-21')),\n" +
                "    PARTITION p20210821 VALUES [('2021-08-21'), ('2021-08-22')),\n" +
                "    PARTITION p20210929 VALUES [('2021-09-29'), ('2021-09-30')),\n" +
                "    PARTITION p20210930 VALUES [('2021-09-30'), ('2021-10-01'))\n" +
                ")" +
                "DISTRIBUTED BY HASH(pk) BUCKETS 3" +
                " PROPERTIES(\"replication_num\" = \"" + 1 +
                "\", \"storage_medium\" = \"SSD\", \"enable_partition_aggregation\" = \"true\")";
        starRocksAssert.withTable(sql);
    }    

    @Test
    public void testNormal() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), TABLE);
        List<TabletCommitInfo> transTablets1 = Lists.newArrayList();

        for (Partition partition : table.getPartitions()) {
            MaterializedIndex baseIndex = partition.getDefaultPhysicalPartition().getBaseIndex();
            for (Long tabletId : baseIndex.getTabletIdsInOrder()) {
                for (Long backendId : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds()) {
                    TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tabletId, backendId);
                    transTablets1.add(tabletCommitInfo);
                }
            }
        }

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long transactionId1 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        "label1",
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second,
                        -1);
        // commit a transaction
        VisibleStateWaiter waiter1 = globalTransactionMgr.commitTransaction(db.getId(), transactionId1, transTablets1,
                Lists.newArrayList(), null);

        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();

        Assert.assertTrue(waiter1.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testNoComputeNode() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), TABLE);
    
        List<Tablet> tablets = Lists.newArrayList();
        for (Partition partition : table.getPartitions()) {
            MaterializedIndex baseIndex = partition.getDefaultPhysicalPartition().getBaseIndex();
            tablets.addAll(baseIndex.getTablets());
        }

        WarehouseManager originalManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        try {
            Field warehouseMgrField = GlobalStateMgr.class.getDeclaredField("warehouseMgr");
            warehouseMgrField.setAccessible(true);
        
            WarehouseManager mockManager = mock(WarehouseManager.class);
            when(mockManager.warehouseExists(anyLong())).thenReturn(true);
            when(mockManager.getComputeNodeAssignedToTablet(anyLong(), any(LakeTablet.class)))
                    .thenReturn(null);
            when(mockManager.getBackgroundWarehouse()).thenReturn(new DefaultWarehouse(100, "test"));
            warehouseMgrField.set(GlobalStateMgr.getCurrentState(), mockManager);

        
            Assert.assertThrows(NoAliveBackendException.class, () -> {
                Utils.aggregatePublishVersion(tablets, null, 1, 2, null, null, -1, null);
            });
        } finally {
            Field warehouseMgrField = GlobalStateMgr.class.getDeclaredField("warehouseMgr");
            warehouseMgrField.setAccessible(true);
            warehouseMgrField.set(GlobalStateMgr.getCurrentState(), originalManager);
        }
    }

}