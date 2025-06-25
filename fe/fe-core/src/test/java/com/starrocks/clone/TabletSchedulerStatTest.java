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

package com.starrocks.clone;

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.TimeUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TabletSchedulerStatTest {

    @Test
    public void testGetBrief() {
        TabletSchedulerStat stat = new TabletSchedulerStat();

        // update
        stat.counterCloneTask.incrementAndGet();
        stat.counterCloneTaskSucceeded.incrementAndGet();
        Assert.assertEquals(1L, stat.counterCloneTask.get());
        Assert.assertEquals(1L, stat.counterCloneTaskSucceeded.get());
        Assert.assertEquals(0L, stat.counterCloneTaskFailed.get());
        Assert.assertNull(stat.getLastSnapshot());
        long lastSnapShotTime = ((AtomicLong) Deencapsulation.getField(stat, "lastSnapshotTime")).get();
        Assert.assertEquals(-1L, lastSnapShotTime);

        // snapshot
        Deencapsulation.invoke(stat, "snapshot");
        Assert.assertNotNull(stat.getLastSnapshot());
        lastSnapShotTime = ((AtomicLong) Deencapsulation.getField(stat, "lastSnapshotTime")).get();
        Assert.assertTrue(lastSnapShotTime > 0);

        // update
        stat.counterCloneTask.incrementAndGet();
        stat.counterCloneTaskFailed.incrementAndGet();
        Assert.assertEquals(2L, stat.counterCloneTask.get());
        Assert.assertEquals(1L, stat.counterCloneTaskSucceeded.get());
        Assert.assertEquals(1L, stat.counterCloneTaskFailed.get());

        // test getBrief
        List<List<String>> infos = stat.getBrief();
        Assert.assertEquals(27, infos.size());
        for (List<String> info : infos) {
            if (info.get(0).equals("num of clone task")) {
                Assert.assertEquals("1", info.get(1));
                Assert.assertEquals("1", info.get(2));
            } else if (info.get(0).equals("num of clone task succeeded")) {
                Assert.assertEquals("1", info.get(1));
                Assert.assertEquals("0", info.get(2));
            } else if (info.get(0).equals("num of clone task failed")) {
                Assert.assertEquals("0", info.get(1));
                Assert.assertEquals("1", info.get(2));
            } else if (info.get(0).equals("last snapshot time")) {
                Assert.assertEquals(TimeUtils.longToTimeString(lastSnapShotTime), info.get(1));
                String increase = info.get(2);
                Assert.assertTrue(Long.parseLong(increase.substring(0, increase.length() - 1)) >= 0);
            }
        }
    }
}
