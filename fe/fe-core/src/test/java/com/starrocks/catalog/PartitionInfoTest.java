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
import com.starrocks.common.AnalysisException;
import com.starrocks.lake.DataCacheInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

public class PartitionInfoTest {
    private final long partitionId = 10086;
    private final short replicationNum = 3;
    private final boolean inMemory = false;
    private final DataProperty dataProperty = DataProperty.DEFAULT_DATA_PROPERTY;
    private final DataCacheInfo dataCacheInfo = new DataCacheInfo(true, false);

    void validatePartitionInfo(PartitionInfo info, long id) {
        Assertions.assertEquals(replicationNum, info.getReplicationNum(id));
        Assertions.assertEquals(inMemory, info.getIsInMemory(id));
        Assertions.assertEquals(dataProperty, info.getDataProperty(id));
        Assertions.assertEquals(dataCacheInfo, info.getDataCacheInfo(id));
        Assertions.assertEquals(1L, info.idToStorageCacheInfo.size());

        info.dropPartition(id);
        Assertions.assertTrue(info.idToStorageCacheInfo.isEmpty());
    }

    @Test
    public void testAddDropPartitionPartitionInfo() throws AnalysisException {
        { // ListPartitionInfo
            ListPartitionInfo info =
                    new ListPartitionInfo(PartitionType.LIST, Lists.newArrayList(new Column("c0", Type.BIGINT)));
            info.addPartition(null, partitionId, dataProperty, replicationNum, inMemory, dataCacheInfo, null, null);
            validatePartitionInfo(info, partitionId);
        }
        { // SinglePartitionInfo
            SinglePartitionInfo info = new SinglePartitionInfo();
            info.addPartition(partitionId, dataProperty, replicationNum, inMemory, dataCacheInfo);
            validatePartitionInfo(info, partitionId);
        }
        { // RangePartitionInfo
            RangePartitionInfo info = new RangePartitionInfo(Lists.newArrayList(new Column("c0", Type.BIGINT)));
            PartitionKey partitionKey = new PartitionKey();
            Range<PartitionKey> range = Range.closedOpen(partitionKey, partitionKey);
            info.addPartition(partitionId, false, range, dataProperty, replicationNum, inMemory, dataCacheInfo);
            validatePartitionInfo(info, partitionId);
        }
    }

    @Test
    public void testCleanupStaledIdToDataCacheInfo() throws IOException {
        RangePartitionInfo info = new RangePartitionInfo(Lists.newArrayList(new Column("c0", Type.BIGINT)));
        PartitionKey partitionKey = new PartitionKey();
        Range<PartitionKey> range = Range.closedOpen(partitionKey, partitionKey);
        info.addPartition(partitionId, false, range, dataProperty, replicationNum, inMemory, dataCacheInfo);

        info.idToStorageCacheInfo.put(10087L, dataCacheInfo);
        Assertions.assertEquals(2L, info.idToStorageCacheInfo.size());
        // with gsonPostProcess, the invalid partition id will be removed from idToStorageCacheInfo
        info.gsonPostProcess();
        Assertions.assertEquals(1L, info.idToStorageCacheInfo.size());
    }

    @Test
    public void testGetIsInMemoryDefaultFalseWhenMissing() {
        PartitionInfo info = new SinglePartitionInfo();
        Assertions.assertFalse(info.getIsInMemory(partitionId));

        info.idToInMemory = null;
        Assertions.assertFalse(info.getIsInMemory(partitionId));
    }

    @Test
    public void testGsonPostProcessBackfillIdToInMemory() throws IOException {
        SinglePartitionInfo info = new SinglePartitionInfo();
        info.addPartition(partitionId, dataProperty, replicationNum, true, dataCacheInfo);
        long partitionId2 = partitionId + 1;
        info.addPartition(partitionId2, dataProperty, replicationNum, true, dataCacheInfo);

        info.idToInMemory = new HashMap<>();
        info.setIsInMemory(partitionId2, true);
        info.gsonPostProcess();
        Assertions.assertEquals(false, info.getIsInMemory(partitionId));
        Assertions.assertEquals(true, info.getIsInMemory(partitionId2));

        info.idToInMemory = null;
        info.gsonPostProcess();
        Assertions.assertEquals(false, info.getIsInMemory(partitionId));
        Assertions.assertEquals(false, info.getIsInMemory(partitionId2));
    }
}
