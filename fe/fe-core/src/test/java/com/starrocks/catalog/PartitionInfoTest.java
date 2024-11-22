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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PartitionInfoTest {
    private final long partitionId = 10086;
    private final short replicationNum = 3;
    private final boolean inMemory = false;
    private final DataProperty dataProperty = DataProperty.DEFAULT_DATA_PROPERTY;
    private final DataCacheInfo dataCacheInfo = new DataCacheInfo(true, false);

    void validatePartitionInfo(PartitionInfo info, long id) {
        Assert.assertEquals(replicationNum, info.getReplicationNum(id));
        Assert.assertEquals(inMemory, info.getIsInMemory(id));
        Assert.assertEquals(dataProperty, info.getDataProperty(id));
        Assert.assertEquals(dataCacheInfo, info.getDataCacheInfo(id));
        Assert.assertEquals(1L, info.idToStorageCacheInfo.size());

        info.dropPartition(id);
        Assert.assertTrue(info.idToStorageCacheInfo.isEmpty());
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
        Assert.assertEquals(2L, info.idToStorageCacheInfo.size());
        // with gsonPostProcess, the invalid partition id will be removed from idToStorageCacheInfo
        info.gsonPostProcess();
        Assert.assertEquals(1L, info.idToStorageCacheInfo.size());
    }

    @Test
    public void testCouldUseExternalCoolDownPartition() throws ParseException {
        RangePartitionInfo info = new RangePartitionInfo(Lists.newArrayList(new Column("c0", Type.BIGINT)));
        PartitionKey partitionKey = new PartitionKey();
        Range<PartitionKey> range = Range.closedOpen(partitionKey, partitionKey);
        info.addPartition(partitionId, false, range, dataProperty, replicationNum, inMemory, dataCacheInfo);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date time1 = dateFormat.parse("2024-10-23 12:00:00");
        Date time2 = dateFormat.parse("2024-10-23 16:00:00");
        Date time3 = dateFormat.parse("2024-10-23 18:00:00");

        Partition partition = new Partition(partitionId, "p0", null);
        PhysicalPartition physicalPartition = new PhysicalPartition(
                1, "", 1, null);
        partition.addSubPartition(physicalPartition);
        partition.getDefaultPhysicalPartition().updateVisibleVersion(2, time2.getTime());

        info.setExternalCoolDownSyncedTimeMs(partitionId, time2.getTime());
        info.setExternalCoolDownConsistencyCheckTimeMs(partitionId, time3.getTime());
        info.setCoolDownConsistencyCheckDifference(partitionId, 0L);

        Assert.assertTrue(info.couldUseExternalCoolDownPartition(partition));
        Assert.assertTrue(info.couldUseExternalCoolDownPartition(partition, time1.getTime()));

        partition.getDefaultPhysicalPartition().updateVisibleVersion(3, time2.getTime());
        Assert.assertTrue(info.couldUseExternalCoolDownPartition(partition));
        Assert.assertTrue(info.couldUseExternalCoolDownPartition(partition, time1.getTime()));

        String infoString = info.toString();
        Assert.assertTrue(infoString.contains("external cool down time"));
        Assert.assertTrue(infoString.contains("external cool down consistency check time"));
        Assert.assertTrue(infoString.contains("external cool down consistency check result"));
    }
}
