// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. // You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.common.proc;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class LakeTabletsProcNodeTest {

    @Mocked
    private ConnectContext connectContext;

    public LakeTabletsProcNodeTest() {
        connectContext = new ConnectContext(null);
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testFetchResult(@Mocked GlobalStateMgr globalStateMgr, @Mocked WarehouseManager agent) throws
            StarRocksException {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long physicalPartitionId = 6L;
        long tablet1Id = 10L;
        long tablet2Id = 11L;

        // Schema
        List<Column> columns = Lists.newArrayList();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        // Tablet
        Tablet tablet1 = new LakeTablet(tablet1Id);
        Tablet tablet2 = new LakeTablet(tablet2Id);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getWarehouseMgr();
                result = agent;

                agent.getAllComputeNodeIdsAssignToTablet(0L, (LakeTablet) tablet1);
                result = Lists.newArrayList(10000, 10001);

                agent.getAllComputeNodeIdsAssignToTablet(0L, (LakeTablet) tablet2);
                result = Lists.newArrayList(10001, 10002);
            }
        };

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.HDD, true);
        index.addTablet(tablet1, tabletMeta);
        index.addTablet(tablet2, tabletMeta);

        // Partition
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(partitionId, (short) 3);
        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", index, distributionInfo);

        // Lake table
        LakeTable table = new LakeTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        // Db
        Database db = new Database(dbId, "test_db");
        db.registerTableUnlocked(table);

        // Check
        LakeTabletsProcDir procDir = new LakeTabletsProcDir(db, table, index);
        List<List<Comparable>> result = procDir.fetchComparableResult();
        Assert.assertEquals(2, result.size());
        {
            Assert.assertEquals((long) result.get(0).get(0), tablet1Id);
            String backendIds = (String) result.get(0).get(1);
            Assert.assertTrue(backendIds.contains("10000") && backendIds.contains("10001"));
        }
        {
            Assert.assertEquals((long) result.get(1).get(0), tablet2Id);
            String backendIds = (String) result.get(1).get(1);
            Assert.assertTrue(backendIds.contains("10001") && backendIds.contains("10002"));
        }

        { // check show single tablet with tablet id
            ProcNodeInterface procNode = procDir.lookup(String.valueOf(tablet1Id));
            ProcResult res = procNode.fetchResult();
            Assert.assertEquals(1L, res.getRows().size());
            List<String> row = res.getRows().get(0);
            Assert.assertEquals(String.valueOf(tablet1.getId()), row.get(0));

            Assert.assertEquals(new Gson().toJson(tablet1.getBackendIds()), row.get(1));
            Assert.assertEquals(new ByteSizeValue(tablet1.getDataSize(true)).toString(), row.get(2));
            Assert.assertEquals(String.valueOf(tablet1.getRowCount(0L)), row.get(3));
        }

        { // error case
            // invalid integer
            Assert.assertThrows(AnalysisException.class, () -> procDir.lookup("a123"));
            // non-exist tablet id
            Assert.assertThrows(AnalysisException.class, () -> procDir.lookup("123456789"));
        }
    }
}
