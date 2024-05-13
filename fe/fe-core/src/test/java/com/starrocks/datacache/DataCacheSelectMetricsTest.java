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

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TDataCacheMetrics;
import com.starrocks.thrift.TDataCacheStatus;
import com.starrocks.thrift.TLoadDataCacheMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class DataCacheSelectMetricsTest {
    private final long megabyte = 1024 * 1024L;
    private final long gigabyte = 1024 * megabyte;
    private final long second = 1000000000L;
    private final long be1Id = 77778L;
    private final long be2Id = 77779L;
    private final long cn1Id = 77780L;

    @Before
    public void setup() {
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                .addBackend(new Backend(be1Id, "127.0.0.2", 7777));
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                .addBackend(new Backend(be2Id, "127.0.0.3", 7777));
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                .addComputeNode(new ComputeNode(cn1Id, "127.0.0.4", 7777));
    }

    @Test
    public void testCacheSelectMetrics() {
        DataCacheSelectMetrics dataCacheSelectMetrics = new DataCacheSelectMetrics();

        TLoadDataCacheMetrics be1 = new TLoadDataCacheMetrics();
        be1.setRead_bytes(megabyte);
        be1.setRead_time_ns(second);
        be1.setWrite_bytes(gigabyte);
        be1.setWrite_time_ns(10 * second);
        be1.setCount(1);

        TDataCacheMetrics dataCacheMetrics = new TDataCacheMetrics();
        dataCacheMetrics.setMem_quota_bytes(gigabyte);
        dataCacheMetrics.setDisk_quota_bytes(gigabyte);
        dataCacheMetrics.setMem_used_bytes(gigabyte);
        dataCacheMetrics.setDisk_used_bytes(0);
        dataCacheMetrics.setStatus(TDataCacheStatus.NORMAL);

        be1.setMetrics(dataCacheMetrics);
        LoadDataCacheMetrics be1Metrics = LoadDataCacheMetrics.buildFromThrift(be1);

        TLoadDataCacheMetrics be2 = new TLoadDataCacheMetrics();
        be2.setRead_bytes(2 * megabyte);
        be2.setRead_time_ns(2 * second);
        be2.setWrite_bytes(2 * gigabyte);
        be2.setWrite_time_ns(35 * second);
        be2.setCount(1);

        be2.setMetrics(dataCacheMetrics);
        LoadDataCacheMetrics be2Metrics = LoadDataCacheMetrics.buildFromThrift(be2);

        TLoadDataCacheMetrics cn1 = new TLoadDataCacheMetrics();
        cn1.setRead_bytes(3 * megabyte);
        cn1.setRead_time_ns(3 * second);
        cn1.setWrite_bytes(3 * gigabyte);
        cn1.setWrite_time_ns(15 * second);
        cn1.setCount(1);

        cn1.setMetrics(dataCacheMetrics);
        LoadDataCacheMetrics cn1Metrics = LoadDataCacheMetrics.buildFromThrift(cn1);

        dataCacheSelectMetrics.updateLoadDataCacheMetrics(be1Id, be1Metrics);
        dataCacheSelectMetrics.updateLoadDataCacheMetrics(be2Id, be2Metrics);
        dataCacheSelectMetrics.updateLoadDataCacheMetrics(cn1Id, cn1Metrics);

        List<List<String>> rows = dataCacheSelectMetrics.getShowResultSet(false).getResultRows();
        Assert.assertEquals("6MB,6GB,20s,50.00%", String.join(",", rows.get(0)));
        rows = dataCacheSelectMetrics.getShowResultSet(true).getResultRows();
        for (List<String> row : rows) {
            if (row.get(0).equals("127.0.0.2")) {
                Assert.assertEquals("127.0.0.2,1MB,1s,1GB,10s,50.00%", String.join(",", row));
            }
            if (row.get(0).equals("127.0.0.3")) {
                Assert.assertEquals("127.0.0.3,2MB,2s,2GB,35s,50.00%", String.join(",", row));
            }
            if (row.get(0).equals("127.0.0.4")) {
                Assert.assertEquals("127.0.0.4,3MB,3s,3GB,15s,50.00%", String.join(",", row));
            }
        }

        Assert.assertEquals(
                "ReadCacheSize: 6MB, AvgReadCacheTime: 2s, WriteCacheSize: 6GB, AvgWriteCacheTime: 20s, " +
                        "TotalCacheUsage: 50.00%", dataCacheSelectMetrics.debugString(false));

        Assert.assertEquals(
                "[IP: 127.0.0.3, ReadCacheSize: 2MB, AvgReadCacheTime: 2s, WriteCacheSize: 2GB, " +
                        "AvgWriteCacheTime: 35s, TotalCacheUsage: 50.00%], [IP: 127.0.0.2, ReadCacheSize: 1MB, " +
                        "AvgReadCacheTime: 1s, WriteCacheSize: 1GB, AvgWriteCacheTime: 10s, TotalCacheUsage: 50.00%], " +
                        "[IP: 127.0.0.4, ReadCacheSize: 3MB, AvgReadCacheTime: 3s, WriteCacheSize: 3GB, " +
                        "AvgWriteCacheTime: 15s, TotalCacheUsage: 50.00%]",
                dataCacheSelectMetrics.debugString(true));
    }
}
