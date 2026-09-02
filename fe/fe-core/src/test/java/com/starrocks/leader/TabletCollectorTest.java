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

package com.starrocks.leader;

import com.starrocks.leader.TabletCollector.CollectStat;
<<<<<<< HEAD
=======
import com.starrocks.system.Backend;
import com.starrocks.thrift.TGetTabletsInfoResult;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
>>>>>>> 9f3ccca ([BugFix] Fix FE max tablet compaction score collection (#78496))
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.PriorityQueue;

public class TabletCollectorTest {

    @Test
    public void testCollectStat() {
        PriorityQueue<CollectStat> queue = new PriorityQueue();
        queue.add(new CollectStat(1L, 1L));
        queue.add(new CollectStat(2L, 2L));
        queue.add(new CollectStat(3L, 3L));
        Assertions.assertEquals(1L, queue.poll().lastCollectTime);
        Assertions.assertEquals(2L, queue.poll().lastCollectTime);
        Assertions.assertEquals(3L, queue.poll().lastCollectTime);
    }
<<<<<<< HEAD
=======

    @Test
    public void testOnStoppedClearsLeaderSessionState() throws Exception {
        TabletCollector collector = new TabletCollector();

        @SuppressWarnings("unchecked")
        PriorityQueue<CollectStat> queue =
                (PriorityQueue<CollectStat>) FieldUtils.readField(collector, "collectQueue", true);
        @SuppressWarnings("unchecked")
        Set<Long> queuedBeIds = (Set<Long>) FieldUtils.readField(collector, "queuedBeIds", true);

        queue.add(new CollectStat(7L, 100L));
        queue.add(new CollectStat(8L, 200L));
        queuedBeIds.add(7L);
        queuedBeIds.add(8L);

        Assertions.assertEquals(2, queue.size(), "precondition: queue populated");
        Assertions.assertEquals(2, queuedBeIds.size(), "precondition: queuedBeIds populated");

        // Trigger LeaderDaemon's cleanup hook directly. After demotion the next leader rebuilds
        // the queue from scratch via updateQueue(); leaving stale entries here would suppress
        // collection from BEs whose ids stayed in queuedBeIds.
        MethodUtils.invokeMethod(collector, true, "onStopped");

        Assertions.assertTrue(queue.isEmpty(), "collectQueue should be cleared on demotion");
        Assertions.assertTrue(queuedBeIds.isEmpty(), "queuedBeIds should be cleared on demotion");
    }

    @Test
    public void testUpdateTabletMaxCompactionScore() {
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setTabletMaxCompactionScore(10L);

        TGetTabletsInfoResult oldBeResult = new TGetTabletsInfoResult(new TStatus(TStatusCode.OK));
        TabletCollector.updateTabletMaxCompactionScore(backend, oldBeResult);
        Assertions.assertEquals(10L, backend.getTabletMaxCompactionScore());

        TGetTabletsInfoResult newBeResult = new TGetTabletsInfoResult(new TStatus(TStatusCode.OK));
        newBeResult.setTablet_max_compaction_score(42L);
        TabletCollector.updateTabletMaxCompactionScore(backend, newBeResult);
        Assertions.assertEquals(42L, backend.getTabletMaxCompactionScore());
    }
>>>>>>> 9f3ccca ([BugFix] Fix FE max tablet compaction score collection (#78496))
}
