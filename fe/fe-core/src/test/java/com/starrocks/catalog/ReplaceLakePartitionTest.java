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
package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.staros.client.StarClientException;
import com.staros.proto.FilePathInfo;
import com.staros.proto.ShardInfo;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

public class ReplaceLakePartitionTest {
    long dbId = 9010;
    long tableId = 9011;
    long partitionId = 9012;
    long indexId = 9013;
    long[] tabletId = {9014, 90015};
    long tempPartitionId = 9020;
    long nextTxnId = 20000;
    String partitionName = "p0";
    String tempPartitionName = "temp_" + partitionName;
    long[] newTabletId = {9030, 90031};
    long newPartitionId = 9040;

    LakeTable tbl = null;
    private final ShardInfo shardInfo;

    @Mocked
    private StarOSAgent starOSAgent;

    @Mocked
    private EditLog editLog;

    public ReplaceLakePartitionTest() {
        shardInfo = ShardInfo.newBuilder().setFilePath(FilePathInfo.newBuilder().setFullPath("oss://1/2")).build();
    }

    @Before
    public void setUp() {
        UtFrameUtils.mockInitWarehouseEnv();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public ShardInfo getShardInfo(long shardId, long workerGroupId) throws StarClientException {
                return shardInfo;
            }
        };

        new MockUp<EditLog>() {
            @Mock
            public void logErasePartition(long partitionId) {
                return;
            }
        };
    }

    LakeTable buildLakeTableWithTempPartition(PartitionType partitionType) {
        MaterializedIndex index = new MaterializedIndex(indexId);
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (long id : tabletId) {
            TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, 0, 0, TStorageMedium.HDD, true);
            invertedIndex.addTablet(id, tabletMeta);
            index.addTablet(new LakeTablet(id), tabletMeta);
        }
        Partition partition = new Partition(partitionId, partitionId + 100L, partitionName, index, null);
        Partition tempPartition = new Partition(tempPartitionId, tempPartitionId + 100L, tempPartitionName, index, null);

        PartitionInfo partitionInfo = null;
        if (partitionType == PartitionType.UNPARTITIONED) {
            partitionInfo = new PartitionInfo(partitionType);
        } else if (partitionType == PartitionType.LIST) {
            partitionInfo = new ListPartitionInfo(PartitionType.LIST, Lists.newArrayList(new Column("c0", Type.BIGINT)));
            List<String> values = Lists.newArrayList();
            values.add("123");
            ((ListPartitionInfo) partitionInfo).setValues(partitionId, values);
        } else if (partitionType == PartitionType.RANGE) {
            PartitionKey partitionKey = new PartitionKey();
            Range<PartitionKey> range = Range.closedOpen(partitionKey, partitionKey);
            partitionInfo = new RangePartitionInfo(Lists.newArrayList(new Column("c0", Type.BIGINT)));
            ((RangePartitionInfo) partitionInfo).setRange(partitionId, false, range);
        }

        partitionInfo.setReplicationNum(partitionId, (short) 1);
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(true, false));

        LakeTable table = new LakeTable(
                tableId, "t0",
                Lists.newArrayList(new Column("c0", Type.BIGINT)),
                KeysType.DUP_KEYS, partitionInfo, null);
        table.addPartition(partition);
        table.addTempPartition(tempPartition);
        return table;
    }

    Partition buildPartitionForTruncateTable() {
        MaterializedIndex index = new MaterializedIndex(indexId);
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (long id : newTabletId) {
            TabletMeta tabletMeta = new TabletMeta(dbId, tableId, newPartitionId, 0, 0, TStorageMedium.HDD, true);
            invertedIndex.addTablet(id, tabletMeta);
            index.addTablet(new LakeTablet(id), tabletMeta);
        }

        return new Partition(newPartitionId, newPartitionId + 100L, partitionName, index, null);
    }

    private void erasePartitionOrTableAndUntilFinished(long id) {
        while (GlobalStateMgr.getCurrentState().getRecycleBin().getRecyclePartitionInfo(id) != null) {
            ExceptionChecker.expectThrowsNoException(()
                    -> GlobalStateMgr.getCurrentState().getRecycleBin().erasePartition(Long.MAX_VALUE));
            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
        }

        while (GlobalStateMgr.getCurrentState().getRecycleBin().getRecycleTableInfo(id) != null) {
            ExceptionChecker.expectThrowsNoException(()
                    -> GlobalStateMgr.getCurrentState().getRecycleBin().eraseTable(Long.MAX_VALUE));
            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
        }
    }

    @Test
    public void testUnPartitionedLakeTableReplacePartition() {
        LakeTable tbl = buildLakeTableWithTempPartition(PartitionType.UNPARTITIONED);
        tbl.replacePartition(partitionName, tempPartitionName);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecyclePartitionInfo(partitionId) != null);
        erasePartitionOrTableAndUntilFinished(partitionId);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecyclePartitionInfo(partitionId) == null);
    }

    @Test
    public void testUnPartitionedLakeTableReplacePartitionForTruncateTable() {
        LakeTable tbl = buildLakeTableWithTempPartition(PartitionType.UNPARTITIONED);
        Partition newPartition = buildPartitionForTruncateTable();
        tbl.replacePartition(newPartition);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecyclePartitionInfo(partitionId) != null);
        erasePartitionOrTableAndUntilFinished(partitionId);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecyclePartitionInfo(partitionId) == null);
    }

    @Test
    public void testListPartitionedLakeTableReplacePartitionForTruncateTable() {
        LakeTable tbl = buildLakeTableWithTempPartition(PartitionType.LIST);
        Partition newPartition = buildPartitionForTruncateTable();
        tbl.replacePartition(newPartition);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecyclePartitionInfo(partitionId) != null);
        erasePartitionOrTableAndUntilFinished(partitionId);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecyclePartitionInfo(partitionId) == null);
    }

    @Test
    public void testRangePartitionedLakeTableReplacePartitionForTruncateTable() {
        LakeTable tbl = buildLakeTableWithTempPartition(PartitionType.RANGE);
        Partition newPartition = buildPartitionForTruncateTable();
        tbl.replacePartition(newPartition);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecyclePartitionInfo(partitionId) != null);
        erasePartitionOrTableAndUntilFinished(partitionId);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecyclePartitionInfo(partitionId) == null);
    }

    @Test
    public void testLakeTableDeleteFromRecycleBin() {
        {
            LakeTable tbl = buildLakeTableWithTempPartition(PartitionType.RANGE);
            new MockUp<LakeTable>() {
                @Mock
                public Collection<PhysicalPartition> getAllPhysicalPartitions() {
                    return Lists.newArrayList();
                }
            };
            tbl.delete(dbId, false);
            Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecycleTableInfo(tableId) != null);
            erasePartitionOrTableAndUntilFinished(tableId);
            Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecycleTableInfo(tableId) == null);
        }

        {
            LakeTable tbl = buildLakeTableWithTempPartition(PartitionType.LIST);
            new MockUp<LakeTable>() {
                @Mock
                public Collection<PhysicalPartition> getAllPhysicalPartitions() {
                    return Lists.newArrayList();
                }
            };
            tbl.delete(dbId, false);
            Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecycleTableInfo(tableId) != null);
            erasePartitionOrTableAndUntilFinished(tableId);
            Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecycleTableInfo(tableId) == null);
        }

        {
            LakeTable tbl = buildLakeTableWithTempPartition(PartitionType.UNPARTITIONED);
            new MockUp<LakeTable>() {
                @Mock
                public Collection<PhysicalPartition> getAllPhysicalPartitions() {
                    return Lists.newArrayList();
                }
            };
            tbl.delete(dbId, false);
            Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecycleTableInfo(tableId) != null);
            erasePartitionOrTableAndUntilFinished(tableId);
            Assert.assertTrue(GlobalStateMgr.getCurrentState().getRecycleBin().getRecycleTableInfo(tableId) == null);
        }
    }
}
