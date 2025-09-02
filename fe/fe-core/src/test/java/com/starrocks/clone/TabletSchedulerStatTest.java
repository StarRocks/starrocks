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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TabletSchedulerStatTest {

    @Test
    public void testGetBrief() {
        TabletSchedulerStat stat = new TabletSchedulerStat();

        // update
        stat.counterCloneTask.incrementAndGet();
        stat.counterCloneTaskSucceeded.incrementAndGet();
        stat.counterCloneTaskInterNodeCopyBytes.addAndGet(100L);
        stat.counterCloneTaskIntraNodeCopyBytes.addAndGet(101L);
        stat.counterCloneTaskInterNodeCopyDurationMs.addAndGet(10L);
        stat.counterCloneTaskIntraNodeCopyDurationMs.addAndGet(11L);
        Assertions.assertEquals(1L, stat.counterCloneTask.get());
        Assertions.assertEquals(1L, stat.counterCloneTaskSucceeded.get());
        Assertions.assertEquals(0L, stat.counterCloneTaskFailed.get());
        Assertions.assertEquals(100L, stat.counterCloneTaskInterNodeCopyBytes.get());
        Assertions.assertEquals(101L, stat.counterCloneTaskIntraNodeCopyBytes.get());
        Assertions.assertEquals(10L, stat.counterCloneTaskInterNodeCopyDurationMs.get());
        Assertions.assertEquals(11L, stat.counterCloneTaskIntraNodeCopyDurationMs.get());
        Assertions.assertNull(stat.getLastSnapshot());
        long lastSnapShotTime = ((AtomicLong) Deencapsulation.getField(stat, "lastSnapshotTime")).get();
        Assertions.assertEquals(-1L, lastSnapShotTime);

        // snapshot
        Deencapsulation.invoke(stat, "snapshot");
        Assertions.assertNotNull(stat.getLastSnapshot());
        lastSnapShotTime = ((AtomicLong) Deencapsulation.getField(stat, "lastSnapshotTime")).get();
        Assertions.assertTrue(lastSnapShotTime > 0);

        // update
        stat.counterCloneTask.incrementAndGet();
        stat.counterCloneTaskFailed.incrementAndGet();
        Assertions.assertEquals(2L, stat.counterCloneTask.get());
        Assertions.assertEquals(1L, stat.counterCloneTaskSucceeded.get());
        Assertions.assertEquals(1L, stat.counterCloneTaskFailed.get());

        // test getBrief
        List<List<String>> infos = stat.getBrief();
        Assertions.assertEquals(30, infos.size());
        for (List<String> info : infos) {
            if (info.get(0).equals("num of clone task")) {
                Assertions.assertEquals("1", info.get(1));
                Assertions.assertEquals("1", info.get(2));
            } else if (info.get(0).equals("num of clone task succeeded")) {
                Assertions.assertEquals("1", info.get(1));
                Assertions.assertEquals("0", info.get(2));
            } else if (info.get(0).equals("num of clone task failed")) {
                Assertions.assertEquals("0", info.get(1));
                Assertions.assertEquals("1", info.get(2));
            } else if (info.get(0).equals("last snapshot time")) {
                Assertions.assertEquals(TimeUtils.longToTimeString(lastSnapShotTime), info.get(1));
                String increase = info.get(2);
                Assertions.assertTrue(Long.parseLong(increase.substring(0, increase.length() - 1)) >= 0);
            }
        }
    }
}
