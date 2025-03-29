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
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;


public class PartitionsProcDirTest {
    private Database db;
    private LakeTable cloudNativTable;

    @Before
    public void setUp() throws DdlException, AnalysisException {
        db = new Database(10000L, "PartitionsProcDirTestDB");
        Map<String, Long> indexNameToId = Maps.newHashMap();
        indexNameToId.put("index1", 1000L);

        List<Column> col = Lists.newArrayList(new Column("province", Type.VARCHAR));
        PartitionInfo listPartition = new ListPartitionInfo(PartitionType.LIST, col);
        DataCacheInfo dataCache = new DataCacheInfo(true, false);
        long partitionId = 1025;
        listPartition.setDataCacheInfo(partitionId, dataCache);
        cloudNativTable = new LakeTable(1024L, "cloud_native_table", col, null, listPartition, null);
        MaterializedIndex index = new MaterializedIndex(1000L, IndexState.NORMAL);
        cloudNativTable.addPartition(new Partition(partitionId, 1035,
                "p1", index, new RandomDistributionInfo(10)));

        db.registerTableUnlocked(cloudNativTable);
    }

    @Test
    public void testFetchResult() throws AnalysisException {
        BaseProcResult result = (BaseProcResult) new PartitionsProcDir(db, cloudNativTable, false).fetchResult();
        List<List<String>> rows = result.getRows();
        List<String> list1 = rows.get(0);
        Assert.assertEquals("1035", list1.get(0));
        Assert.assertEquals("p1", list1.get(1));
        Assert.assertEquals("0", list1.get(2));
        Assert.assertEquals("1", list1.get(3));
        Assert.assertEquals("2", list1.get(4));
        Assert.assertEquals("NORMAL", list1.get(5));
        Assert.assertEquals("province", list1.get(6));
    }
}
