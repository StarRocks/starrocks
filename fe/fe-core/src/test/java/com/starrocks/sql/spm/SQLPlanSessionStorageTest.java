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

package com.starrocks.sql.spm;

import com.starrocks.catalog.CatalogIdGenerator;
import com.starrocks.persist.EditLogException;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Session baselines are created by a NO_FORWARD statement, so they are stored on whatever FE the client is
 * connected to - a follower most of the time. These tests pin that the session storage stays reachable there,
 * i.e. that it never asks for a catalog id, which only the leader can hand out.
 */
public class SQLPlanSessionStorageTest {
    private SQLPlanSessionStorage storage;

    @BeforeEach
    public void setUp() {
        storage = new SQLPlanSessionStorage();

        // Reproduce a follower FE: it only ever replays OP_SAVE_NEXTID_V2, which leaves nextId == batchEndId,
        // so every single getNextId() takes the journal-writing branch and is refused by the leader WAL gate.
        new MockUp<CatalogIdGenerator>() {
            @Mock
            public long getNextId() {
                throw new EditLogException("leader WAL gate is closed, submit log is not allowed");
            }
        };
    }

    private static BaselinePlan newPlan(String bindSql, long hash) {
        BaselinePlan plan = new BaselinePlan(bindSql, bindSql, hash, bindSql, 1.0);
        // Mirror SPMStmtExecutor.execute(ConnectContext, CreateBaselinePlanStmt).
        plan.setEnable(true);
        plan.setSource(BaselinePlan.SOURCE_USER);
        return plan;
    }

    @Test
    public void testStoreBaselinePlanDoesNotNeedLeaderIdGenerator() {
        // Guard the guard: the storage is supposed to never reach the catalog generator, so nothing below
        // would notice a dead mock - and the test would keep passing with the old dependency back in place.
        Assertions.assertThrows(EditLogException.class, () -> new CatalogIdGenerator(1L).getNextId());

        BaselinePlan plan1 = newPlan("select * from t1 where k1 = 1", 1L);
        BaselinePlan plan2 = newPlan("select * from t1 where k2 = 2", 2L);

        storage.storeBaselinePlan(List.of(plan1, plan2));

        Assertions.assertEquals(2, storage.getBaselineCount());
        Assertions.assertNotEquals(plan1.getId(), plan2.getId());
    }

    @Test
    public void testSessionIdsCannotCollideWithGlobalOnes() {
        // SHOW BASELINE unions the session and global storages, and DROP / ENABLE / DISABLE BASELINE apply an
        // id to both, so the two id spaces have to stay disjoint. Catalog ids count up from NEXT_ID_INIT_VALUE
        // one object at a time; the session range sits far above anything they can reach.
        BaselinePlan plan = newPlan("select * from t1", 3L);
        storage.storeBaselinePlan(List.of(plan));

        Assertions.assertTrue(plan.getId() >= SQLPlanSessionStorage.SESSION_BASELINE_ID_BASE,
                "session baseline id must come from the reserved range, got " + plan.getId());
        Assertions.assertTrue(SQLPlanSessionStorage.SESSION_BASELINE_ID_BASE > GlobalStateMgr.NEXT_ID_INIT_VALUE);
    }

    @Test
    public void testAssignedIdIsUsableByDropAndControl() {
        BaselinePlan plan = newPlan("select * from t1 where k3 = 'a'", 4L);
        storage.storeBaselinePlan(List.of(plan));
        long id = plan.getId();

        Assertions.assertTrue(plan.isEnable());
        storage.controlBaselinePlan(false, List.of(id));
        Assertions.assertFalse(plan.isEnable());

        storage.dropBaselinePlan(List.of(id));
        Assertions.assertEquals(0, storage.getBaselineCount());
    }
}
