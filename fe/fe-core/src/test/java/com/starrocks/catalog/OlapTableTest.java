// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/OlapTableTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.IndexDef;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.FastByteArrayOutputStream;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UnitTestUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OlapTableTest {

    @Test
    public void testTableWithLocalTablet() throws IOException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            int getCurrentStateJournalVersion() {
                return FeConstants.meta_version;
            }
        };

        Database db = UnitTestUtil.createDb(1, 2, 3, 4, 5, 6, 7);
        List<Table> tables = db.getTables();

        for (Table table : tables) {
            if (table.getType() != TableType.OLAP) {
                continue;
            }
            OlapTable tbl = (OlapTable) table;
            tbl.setIndexes(Lists.newArrayList(new Index("index", Lists.newArrayList("col"), IndexDef.IndexType.BITMAP
                    , "xxxxxx")));
            System.out.println("orig table id: " + tbl.getId());

            FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
            tbl.write(out);

            out.flush();
            out.close();

            DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream());
            Table copiedTbl = OlapTable.read(in);
            System.out.println("copied table id: " + copiedTbl.getId());

            Assert.assertTrue(copiedTbl instanceof OlapTable);
            Partition partition = ((OlapTable) copiedTbl).getPartition(3L);
            Assert.assertFalse(partition.isUseStarOS());
            MaterializedIndex newIndex = partition.getIndex(4L);
            Assert.assertFalse(newIndex.isUseStarOS());
            for (Tablet tablet : newIndex.getTablets()) {
                Assert.assertTrue(tablet instanceof LocalTablet);
            }
            tbl.addRelatedMaterializedView(10l);
            tbl.addRelatedMaterializedView(20l);
            tbl.addRelatedMaterializedView(30l);
            Assert.assertEquals(Sets.newHashSet(10l, 20l, 30l), tbl.getRelatedMaterializedViews());
            tbl.removeRelatedMaterializedView(10l);
            tbl.removeRelatedMaterializedView(20l);
            Assert.assertEquals(Sets.newHashSet(30l), tbl.getRelatedMaterializedViews());
            tbl.removeRelatedMaterializedView(30l);
            Assert.assertEquals(Sets.newHashSet(), tbl.getRelatedMaterializedViews());
        }
    }

    @Test
    public void testTableWithStarOSTablet() throws IOException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            int getCurrentStateJournalVersion() {
                return FeConstants.meta_version;
            }
        };

        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tablet1Id = 10L;
        long tablet2Id = 11L;

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        // Tablet
        Tablet tablet1 = new StarOSTablet(tablet1Id, 0L);
        Tablet tablet2 = new StarOSTablet(tablet2Id, 1L);

        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(TStorageMedium.S3));
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
        partitionInfo.setReplicationNum(partitionId, (short) 3);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        index.setUseStarOS(partitionInfo.isUseStarOS(partitionId));
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.S3);
        index.addTablet(tablet1, tabletMeta);
        index.addTablet(tablet2, tabletMeta);

        // Partition
        Partition partition = new Partition(partitionId, "p1", index, distributionInfo);
        partition.setPartitionInfo(partitionInfo);

        // Table
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        // Serialize
        FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(byteArrayOutputStream)) {
            table.write(out);
            out.flush();
        }

        // Deserialize
        Table newTable = null;
        try (DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream())) {
            newTable = Table.read(in);
        }
        byteArrayOutputStream.close();

        // Check
        Assert.assertTrue(newTable instanceof OlapTable);
        OlapTable newOlapTable = (OlapTable) newTable;
        Partition p1 = newOlapTable.getPartition(partitionId);
        Assert.assertTrue(p1.isUseStarOS());
        MaterializedIndex newIndex = p1.getBaseIndex();
        Assert.assertTrue(newIndex.isUseStarOS());
        long expectedShardId = 0L;
        for (Tablet tablet : newIndex.getTablets()) {
            Assert.assertTrue(tablet instanceof StarOSTablet);
            StarOSTablet starOSTablet = (StarOSTablet) tablet;
            Assert.assertEquals(expectedShardId++, starOSTablet.getShardId());
        }
    }
}
