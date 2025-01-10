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

import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
// import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PartitionTest {
    private FakeGlobalStateMgr fakeGlobalStateMgr;

    private GlobalStateMgr globalStateMgr;

    @Before
    public void setUp() {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);

        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
    }

    @Test
    public void testPartitionVacuum() throws Exception {
        LakeTablet tablet = new LakeTablet(100);
        TabletMeta meta = new TabletMeta(1, 1, 2, 1, 1, TStorageMedium.HDD, true);

        MaterializedIndex index = new MaterializedIndex();
        index.addTablet(tablet, meta);

        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        Partition p = new Partition(1, 2, "testTbl", index, distributionInfo);

        // last vacuumed 1 sec ago
        // p.setLastSuccVacuumTime(System.currentTimeMillis() - 1000);
        // Assert.assertFalse(p.shouldVacuum());

        // ensure last vacuum have past long enough
        // but storage size is not so large
        // p.setLastSuccVacuumTime(System.currentTimeMillis() - Config.lake_autovacuum_partition_naptime_seconds * 1000 - 1000);
        tablet.setDataSize(100L * 1024 * 1024);
        tablet.setStorageSize(100L * 1024 * 1024 + Config.lake_enable_vacuum_storage_size_exceeds_mb - 1L * 1024 * 1024);
        // Assert.assertFalse(p.shouldVacuum());

        tablet.setStorageSize(100L * 1024 * 1024 + Config.lake_enable_vacuum_storage_size_exceeds_mb * 1024L * 1024
                              + 1L * 1024 * 1024);
        // Assert.assertTrue(p.shouldVacuum());
    }
}
