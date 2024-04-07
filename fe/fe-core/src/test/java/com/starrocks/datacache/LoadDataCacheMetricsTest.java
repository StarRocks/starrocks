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

package com.starrocks.datacache;

import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.monitor.unit.TimeValue;
import com.starrocks.thrift.TDataCacheMetrics;
import com.starrocks.thrift.TDataCacheStatus;
import com.starrocks.thrift.TLoadDataCacheMetrics;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class LoadDataCacheMetricsTest {

    private final long megabyte = 1024 * 1024L;
    private final long gigabyte = 1024 * megabyte;

    private final long second = 1000000000L;

    @Test
    public void testCreateFromThrift() {
        TLoadDataCacheMetrics tLoadDataCacheMetrics = new TLoadDataCacheMetrics();
        tLoadDataCacheMetrics.setRead_bytes(megabyte);
        tLoadDataCacheMetrics.setRead_time_ns(second);
        tLoadDataCacheMetrics.setWrite_bytes(gigabyte);
        tLoadDataCacheMetrics.setWrite_time_ns(10 * second);

        LoadDataCacheMetrics metrics = LoadDataCacheMetrics.buildFromThrift(tLoadDataCacheMetrics);
        Assert.assertEquals(new ByteSizeValue(megabyte), metrics.getReadBytes());
        Assert.assertEquals(new ByteSizeValue(gigabyte), metrics.getWriteBytes());
        Assert.assertEquals(new TimeValue(1, TimeUnit.SECONDS), metrics.getReadTimeNs());
        Assert.assertEquals(new TimeValue(10, TimeUnit.SECONDS), metrics.getWriteTimeNs());
    }

    @Test
    public void testMerge() {
        TLoadDataCacheMetrics before = new TLoadDataCacheMetrics();
        before.setRead_bytes(megabyte);
        before.setRead_time_ns(second);
        before.setWrite_bytes(gigabyte);
        before.setWrite_time_ns(10 * second);

        TLoadDataCacheMetrics after = new TLoadDataCacheMetrics();
        after.setRead_bytes(megabyte);
        after.setRead_time_ns(second);
        after.setWrite_bytes(gigabyte);
        after.setWrite_time_ns(10 * second);

        TDataCacheMetrics dataCacheMetrics = new TDataCacheMetrics();
        dataCacheMetrics.setMem_quota_bytes(gigabyte);
        dataCacheMetrics.setDisk_quota_bytes(gigabyte);
        dataCacheMetrics.setMem_used_bytes(0);
        dataCacheMetrics.setDisk_used_bytes(0);
        dataCacheMetrics.setStatus(TDataCacheStatus.NORMAL);

        after.setMetrics(dataCacheMetrics);

        LoadDataCacheMetrics beforeMetrics = LoadDataCacheMetrics.buildFromThrift(before);
        LoadDataCacheMetrics afterMetrics = LoadDataCacheMetrics.buildFromThrift(after);
        LoadDataCacheMetrics mergedMetrics = LoadDataCacheMetrics.mergeMetrics(beforeMetrics, afterMetrics);
        Assert.assertEquals(new ByteSizeValue(2 * megabyte), mergedMetrics.getReadBytes());
        Assert.assertEquals(new ByteSizeValue(2 * gigabyte), mergedMetrics.getWriteBytes());
        Assert.assertEquals(new TimeValue(2, TimeUnit.SECONDS), mergedMetrics.getReadTimeNs());
        Assert.assertEquals(new TimeValue(20, TimeUnit.SECONDS), mergedMetrics.getWriteTimeNs());
        Assert.assertEquals("Status: Normal, DiskUsed: 0B, MemUsed: 0B, DiskQuota: 1GB, MemQuota: 1GB",
                mergedMetrics.getLastDataCacheMetrics().toString());
    }
}
