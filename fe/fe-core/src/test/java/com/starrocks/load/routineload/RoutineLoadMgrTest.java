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

package com.starrocks.load.routineload;

import com.starrocks.server.WarehouseManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RoutineLoadMgrTest {

    /**
     * clearBeTaskSlot() must reset every node's task count to 0 and clear the node-to-jobs assignment,
     * so a re-elected leader re-divides routine load jobs from a clean slot count. updateBeTaskSlot()
     * never resets an already-known node's count, so without this reset a slot taken but not released
     * across a demote/re-elect cycle would leak and eventually surface as "no available be slot".
     */
    @Test
    public void testClearBeTaskSlotResetsCountsAndJobs() {
        RoutineLoadMgr mgr = new RoutineLoadMgr();
        // getNodeTasksNum()/getNodeToJobs() expose the live per-warehouse maps; seed stale in-flight
        // accounting on the default warehouse as if the previous leader session had taken slots.
        Map<Long, Integer> nodeTasks = mgr.getNodeTasksNum();
        nodeTasks.put(10001L, 5);
        nodeTasks.put(10002L, 3);
        Map<Long, Set<Long>> nodeToJobs = mgr.getNodeToJobs(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Set<Long> jobs = new HashSet<>();
        jobs.add(1L);
        jobs.add(2L);
        nodeToJobs.put(10001L, jobs);

        mgr.clearBeTaskSlot();

        Assertions.assertEquals(0, nodeTasks.get(10001L).intValue(),
                "BE slot counts must be reset to 0 so the next leader re-divides from a clean count");
        Assertions.assertEquals(0, nodeTasks.get(10002L).intValue());
        Assertions.assertTrue(nodeToJobs.get(10001L).isEmpty(),
                "node-to-jobs assignment must be cleared on demotion");
    }
}
