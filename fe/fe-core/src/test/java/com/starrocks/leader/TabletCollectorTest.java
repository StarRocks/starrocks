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
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.PriorityQueue;
import java.util.Set;

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
}
