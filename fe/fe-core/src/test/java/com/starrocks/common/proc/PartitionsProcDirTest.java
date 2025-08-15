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

package com.starrocks.common.proc;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.clone.BalanceStat;
import com.starrocks.common.AnalysisException;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class PartitionsProcDirTest {

    @Test
    public void testFetchResultForCloudNativeTable() throws AnalysisException {
        Database db = new Database(10000L, "PartitionsProcDirTestDB");

        List<Column> col = Lists.newArrayList(new Column("province", Type.VARCHAR));
        PartitionInfo listPartition = new ListPartitionInfo(PartitionType.LIST, col);
        DataCacheInfo dataCache = new DataCacheInfo(true, false);
        long partitionId = 1025;
        listPartition.setDataCacheInfo(partitionId, dataCache);
        LakeTable cloudNativeTable = new LakeTable(1024L, "cloud_native_table", col, null, listPartition, null);
        MaterializedIndex index = new MaterializedIndex(1000L, IndexState.NORMAL);
        Map<String, Long> indexNameToId = cloudNativeTable.getIndexNameToId();
        indexNameToId.put("index1", index.getId());
        cloudNativeTable.addPartition(new Partition(partitionId, 1035, "p1", index, new RandomDistributionInfo(10)));

        db.registerTableUnlocked(cloudNativeTable);

        BaseProcResult result = (BaseProcResult) new PartitionsProcDir(db, cloudNativeTable, false).fetchResult();
        List<List<String>> rows = result.getRows();
        List<String> list1 = rows.get(0);
        Assertions.assertEquals("1035", list1.get(0));
        Assertions.assertEquals("p1", list1.get(1));
        Assertions.assertEquals("0", list1.get(2));
        Assertions.assertEquals("1", list1.get(3));
        Assertions.assertEquals("2", list1.get(4));
        Assertions.assertEquals("NORMAL", list1.get(5));
        Assertions.assertEquals("province", list1.get(6));
        Assertions.assertEquals("0", list1.get(21));
    }

    @Test
    public void testFetchResultForOlapTable() throws AnalysisException {
        Database db = new Database(10000L, "PartitionsProcDirTestDB");

        List<Column> col = Lists.newArrayList(new Column("province", Type.VARCHAR));
        PartitionInfo listPartition = new ListPartitionInfo(PartitionType.LIST, col);
        long partitionId = 1025;
        listPartition.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        listPartition.setIsInMemory(partitionId, false);
        listPartition.setReplicationNum(partitionId, (short) 1);
        OlapTable olapTable = new OlapTable(1024L, "olap_table", col, null, listPartition, null);
        MaterializedIndex index = new MaterializedIndex(1000L, IndexState.NORMAL);
        index.setBalanceStat(BalanceStat.BALANCED_STAT);
        Map<String, Long> indexNameToId = olapTable.getIndexNameToId();
        indexNameToId.put("index1", index.getId());
        olapTable.addPartition(new Partition(partitionId, 1035, "p1", index, new RandomDistributionInfo(10)));

        db.registerTableUnlocked(olapTable);

        BaseProcResult result = (BaseProcResult) new PartitionsProcDir(db, olapTable, false).fetchResult();
        List<List<String>> rows = result.getRows();
        List<String> list1 = rows.get(0);
        Assertions.assertEquals("1035", list1.get(0));
        Assertions.assertEquals("p1", list1.get(1));
        Assertions.assertEquals("1", list1.get(2)); // visible version
        Assertions.assertEquals("NORMAL", list1.get(5));
        Assertions.assertEquals("province", list1.get(6));
        Assertions.assertEquals("true", list1.get(21)); // tablet balanced
    }
}
