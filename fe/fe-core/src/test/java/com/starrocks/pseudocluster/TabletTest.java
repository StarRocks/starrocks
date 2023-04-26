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

package com.starrocks.pseudocluster;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TabletTest {
    Tablet buildTabletWithVersions(Integer... versions) throws Exception {
        Tablet ret = new Tablet(1, 1, 1, 1, true);
        long txnId = 1;
        for (Integer v : versions) {
            ret.commitRowset(new Rowset(txnId, "rowset" + txnId, 100, 100000), v);
            txnId++;
        }
        return ret;
    }

    @Test
    public void testCloneDiscontinuous() throws Exception {
        Tablet src = buildTabletWithVersions(2, 3, 4, 5, 6);
        Tablet dest = buildTabletWithVersions(2, 4, 6);
        Assert.assertEquals(Lists.newArrayList(3L, 5L, 7L), dest.getMissingVersions());
        dest.cloneFrom(src, 0);
        Assert.assertEquals(6, dest.maxContinuousVersion());
    }

    @Test
    public void testCloneContinuous() throws Exception {
        Tablet src = buildTabletWithVersions(2, 3, 4, 5, 6);
        Tablet dest = buildTabletWithVersions(2, 3, 4);
        Assert.assertEquals(Lists.newArrayList(5L), dest.getMissingVersions());
        dest.cloneFrom(src, 0);
        Assert.assertEquals(6, dest.maxContinuousVersion());
    }

    @Test
    public void testFullClone() throws Exception {
        Tablet src = buildTabletWithVersions(2, 3, 4, 5, 6);
        long oldVersionExpireSec = Tablet.versionExpireSec;
        Tablet.versionExpireSec = 1;
        try {
            Thread.sleep(2000);
            src.versionGC();
        } finally {
            Tablet.versionExpireSec = oldVersionExpireSec;
        }
        Tablet dest = buildTabletWithVersions(2, 3, 4);
        Assert.assertEquals(Lists.newArrayList(5L), dest.getMissingVersions());
        dest.cloneFrom(src, 0);
        Assert.assertEquals(6, dest.minVersion());
    }

    @Test
    public void testVersionGC() throws Exception {
        long oldVersionExpireSec = Tablet.versionExpireSec;
        Tablet.versionExpireSec = 1;
        try {
            Tablet ret = new Tablet(1, 1, 1, 1, true);
            for (int i = 2; i < 16; i++) {
                ret.commitRowset(new Rowset(i, "rowset" + i, 100, 100000), i);
                Thread.sleep(100);
            }
            long oldMinVersion = ret.minVersion();
            ret.versionGC();
            long newMinVersion = ret.minVersion();
            Assert.assertTrue(oldMinVersion < newMinVersion);
        } finally {
            Tablet.versionExpireSec = oldVersionExpireSec;
        }
    }

    @Test
    public void testCompaction() throws Exception {
        long oldCompactionIntervalMs = Tablet.compactionIntervalMs;
        Tablet.compactionIntervalMs = 100;
        try {
            Tablet ret = new Tablet(1, 1, 1, 1, true);
            for (int i = 2; i < 16; i++) {
                ret.commitRowset(new Rowset(i, "rowset" + i, 100, 100000), i);
            }
            Assert.assertEquals(15, ret.getVersionCount());
            Assert.assertEquals(14, ret.getRowsetCount());
            ret.lastCompactionMs = 0;
            ret.doCompaction();
            Assert.assertEquals(16, ret.getVersionCount());
            Assert.assertEquals(1, ret.getRowsetCount());
            ret.doCompaction();
            Assert.assertEquals(16, ret.getVersionCount());
            Assert.assertEquals(1, ret.getRowsetCount());
        } finally {
            Tablet.compactionIntervalMs = oldCompactionIntervalMs;
        }
    }

    @Test
    public void testConvertFrom() throws Exception {
        Tablet baseTablet = buildTabletWithVersions(2, 3, 4, 5, 6);
        Tablet newTablet = buildTabletWithVersions(5, 6);
        newTablet.convertFrom(baseTablet, 4);
        Assert.assertEquals(4, newTablet.minVersion());
        Assert.assertEquals(6, newTablet.maxContinuousVersion());
    }
}
