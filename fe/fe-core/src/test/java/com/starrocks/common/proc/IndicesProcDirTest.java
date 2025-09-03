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
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.clone.BalanceStat;
import com.starrocks.common.AnalysisException;
import com.starrocks.thrift.TStorageMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class IndicesProcDirTest {

    @Test
    public void testFetchResult() throws AnalysisException {
        Database db = new Database(10000L, "IndicesProcDirTestDB");

        List<Column> col = Lists.newArrayList(new Column("province", Type.VARCHAR));
        PartitionInfo listPartition = new ListPartitionInfo(PartitionType.LIST, col);
        long partitionId = 1025;
        listPartition.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        listPartition.setIsInMemory(partitionId, false);
        listPartition.setReplicationNum(partitionId, (short) 1);
        OlapTable olapTable = new OlapTable(1024L, "olap_table", col, null, listPartition, null);
        MaterializedIndex index = new MaterializedIndex(1000L, IndexState.NORMAL);
        index.setBalanceStat(BalanceStat.createClusterTabletBalanceStat(1L, 2L, 9L, 1L));
        Map<String, Long> indexNameToId = olapTable.getIndexNameToId();
        indexNameToId.put("index1", index.getId());
        TabletMeta tabletMeta = new TabletMeta(db.getId(), olapTable.getId(), partitionId, index.getId(), TStorageMedium.HDD);
        index.addTablet(new LocalTablet(1010L), tabletMeta);
        index.addTablet(new LocalTablet(1011L), tabletMeta);
        Partition partition = new Partition(partitionId, 1035, "p1", index, new RandomDistributionInfo(2));
        olapTable.addPartition(partition);

        db.registerTableUnlocked(olapTable);

        IndicesProcDir indicesProcDir = new IndicesProcDir(db, olapTable, partition.getDefaultPhysicalPartition());
        BaseProcResult result = (BaseProcResult) indicesProcDir.fetchResult();
        List<List<String>> rows = result.getRows();
        Assertions.assertEquals(1, rows.size());
        List<String> row = rows.get(0);
        Assertions.assertEquals("1000", row.get(0));
        Assertions.assertEquals("index1", row.get(1));
        Assertions.assertEquals("NORMAL", row.get(2));
        Assertions.assertEquals("\\N", row.get(3)); // last consistency check time
        Assertions.assertEquals(
                "{\"maxTabletNum\":9,\"minTabletNum\":1,\"maxBeId\":1,\"minBeId\":2," +
                        "\"type\":\"INTER_NODE_TABLET_DISTRIBUTION\",\"balanced\":false}",
                row.get(4)); // tablet balance stat
        Assertions.assertEquals("2", row.get(5)); // virtual buckets
        Assertions.assertEquals("2", row.get(6)); // tablets
    }
}
