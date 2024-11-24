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

package com.starrocks.server;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mv.MVRepairHandler;
import com.starrocks.qe.ConnectContext;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class MVRepairHandlerTest {
    private static Database database;
    private static Table table;
    private static Partition partition;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.t1(k1 int, k2 int, k3 int) " +
                        "distributed by hash(k1) buckets 3 properties('replication_num' = '1');")
                .withMaterializedView("CREATE MATERIALIZED VIEW test.mv1 " +
                        "distributed by hash(k1) buckets 3 refresh async as select k1 from test.t1");

        database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t1");
        Assert.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        partition = olapTable.getPartition("t1");
    }

    @Test
    public void testMVRepairHandler() {
        TransactionState txnState = new TransactionState(database.getId(), Lists.newArrayList(table.getId()), 100,
                "test_label", null, TransactionState.LoadJobSourceType.LAKE_COMPACTION, null, 0, 100);
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partition.getId(), 100, 100);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(table.getId());
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
        txnState.putIdToTableCommitInfo(table.getId(), tableCommitInfo);

        MVRepairHandler mvRepairHandler = new MVRepairHandler() {
            @Override
            public void handleMVRepair(Database db, Table table, List<PartitionRepairInfo> partitionRepairInfos) {
                Assert.assertEquals(1, partitionRepairInfos.size());
                PartitionRepairInfo partitionRepairInfo = partitionRepairInfos.get(0);
                Assert.assertEquals(partition.getId(), partitionRepairInfo.getPartitionId());
                Assert.assertEquals(partition.getName(), partitionRepairInfo.getPartitionName());
                Assert.assertEquals(100, partitionRepairInfo.getNewVersion());
                Assert.assertEquals(100, partitionRepairInfo.getNewVersionTime());
            }
        };

        mvRepairHandler.handleMVRepair(txnState);
    }
}
