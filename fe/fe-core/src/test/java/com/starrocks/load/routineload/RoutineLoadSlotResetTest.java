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

public class RoutineLoadSlotResetTest {

    @Test
    public void testOnStoppedReleasesBeTaskSlotsOnDemotion() {
        // Regression: per-backend routine-load task-slot counts are leader-session state. On
        // demotion the tasks they account for are abandoned, so RoutineLoadTaskScheduler#onStopped()
        // must release them. Without this, every leader->follower->leader cycle leaks slots
        // (takeBeTaskSlot only ever increments, demotion never releases, and updateBeTaskSlot does
        // not reset already-present alive backends), eventually exhausting
        // max_routine_load_task_num_per_be so takeBeTaskSlot returns -1 and no routine-load task
        // can be scheduled again.
        RoutineLoadMgr mgr = new RoutineLoadMgr();
        long nodeId = 10001L;
        mgr.getNodeTasksNum().put(nodeId, 0);
        int idleWhenEmpty = mgr.getClusterIdleSlotNum();
        Assertions.assertTrue(idleWhenEmpty > 0, "precondition: backend has idle slots");

        mgr.takeBeTaskSlot(WarehouseManager.DEFAULT_WAREHOUSE_ID, 1L);
        mgr.takeBeTaskSlot(WarehouseManager.DEFAULT_WAREHOUSE_ID, 2L);
        Assertions.assertTrue(mgr.getClusterIdleSlotNum() < idleWhenEmpty,
                "precondition: slots should be taken");

        RoutineLoadTaskScheduler scheduler = new RoutineLoadTaskScheduler(mgr);
        scheduler.onStopped();

        Assertions.assertEquals(idleWhenEmpty, mgr.getClusterIdleSlotNum(),
                "onStopped() must release this leader session's BE task slots");
        Assertions.assertEquals(0, (int) mgr.getNodeTasksNum().get(nodeId));
        Assertions.assertTrue(
                mgr.getNodeToJobs(WarehouseManager.DEFAULT_WAREHOUSE_ID).get(nodeId).isEmpty(),
                "node->jobs mapping must be cleared on demotion");
    }
}
