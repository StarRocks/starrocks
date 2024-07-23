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
package com.starrocks.persist;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import org.junit.Test;

public class ReplacePartitionTest {
    long dbId = 9010;
    long tableId = 9011;
    long partitionId = 9012;
    long indexId = 9013;
    long[] tabletId = {9014, 90015};
    long tempPartitionId = 9020;
    long nextTxnId = 20000;
    String partitionName = "p0";
    String tempPartitionName = "temp_" + partitionName;

    LakeTable tbl = null;

    LakeTable buildLakeTableWithTempPartition(PartitionType partitionType) {
        MaterializedIndex index = new MaterializedIndex(indexId);
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (long id : tabletId) {
            TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, 0, 0, TStorageMedium.HDD, true);
            invertedIndex.addTablet(id, tabletMeta);
            index.addTablet(new LakeTablet(id), tabletMeta);
        }
        Partition partition = new Partition(partitionId, partitionName, index, null);
        Partition tempPartition = new Partition(tempPartitionId, tempPartitionName, index, null);

        PartitionInfo partitionInfo = new PartitionInfo(partitionType);
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

    @Test
    public void testUnPartitionedLakeTableReplacePartition() {
        LakeTable tbl = buildLakeTableWithTempPartition(PartitionType.UNPARTITIONED);
        tbl.replacePartition(partitionName, tempPartitionName);
    }
}
