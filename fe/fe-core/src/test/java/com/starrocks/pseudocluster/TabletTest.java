// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.pseudocluster;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TabletTest {
    Tablet buildTabletWithVersions(Integer... versions) throws Exception {
        Tablet ret = new Tablet(1, 1, 1, 1, true);
        long txnId = 1;
        for (Integer v : versions) {
            ret.commitRowset(new Rowset(txnId, "rowset" + txnId), v);
            txnId++;
        }
        return ret;
    }

    @Test
    public void testCloneDiscontinuous() throws Exception {
        Tablet src = buildTabletWithVersions(2, 3, 4, 5, 6);
        Tablet dest = buildTabletWithVersions(2, 4, 6);
        Assert.assertEquals(Lists.newArrayList(3L, 5L, 7L), dest.getMissingVersions());
        dest.cloneFrom(src);
        Assert.assertEquals(6, dest.maxContinuousVersion());
    }

    @Test
    public void testCloneContinuous() throws Exception {
        Tablet src = buildTabletWithVersions(2, 3, 4, 5, 6);
        Tablet dest = buildTabletWithVersions(2, 3, 4);
        Assert.assertEquals(Lists.newArrayList(5L), dest.getMissingVersions());
        dest.cloneFrom(src);
        Assert.assertEquals(6, dest.maxContinuousVersion());
    }

    @Test
    public void testVersionGC() throws Exception {
        long oldVersionExpireSec = Tablet.versionExpireSec;
        Tablet.versionExpireSec = 1;
        try {
            Tablet ret = new Tablet(1, 1, 1, 1, true);
            for (int i = 2; i < 16; i++) {
                ret.commitRowset(new Rowset(i, "rowset" + i), i);
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
}
