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
import org.junit.Assert;
import org.junit.Test;

import java.util.PriorityQueue;

public class TabletCollectorTest {

    @Test
    public void testCollectStat() {
        PriorityQueue<CollectStat> queue = new PriorityQueue();
        queue.add(new CollectStat(1L, 1L));
        queue.add(new CollectStat(2L, 2L));
        queue.add(new CollectStat(3L, 3L));
        Assert.assertEquals(1L, queue.poll().lastCollectTime);
        Assert.assertEquals(2L, queue.poll().lastCollectTime);
        Assert.assertEquals(3L, queue.poll().lastCollectTime);
    }
}
