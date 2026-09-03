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

package com.starrocks.catalog;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.thrift.TTaskType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The two invalidation contracts of {@link OlapTable}, and why they cannot be one method.
 *
 * <p>A node that is not alive never receives the task {@code AgentBatchTask.run()} would have sent
 * it, so nobody counts its latch mark down and the caller waits out the whole timeout. The drop path
 * must not pay that - it runs once per auto-increment table under the database WRITE lock - while
 * ALTER and RESTORE must, because they use the result as proof that no stale interval survives.
 */
public class DropAutoIncrementMapTaskTest {
    private static final int SECOND_BACKEND_ID = 10002;
    private static final String DB_NAME = "test_drop_auto_increment_map";
    private static final String TABLE_NAME = "t_auto_inc";

    private static final long DEFAULT_TIMEOUT_MS = OlapTable.dropAutoIncrementMapTimeoutMs;

    /**
     * Long enough that a healthy fan-out never trips it, short enough that the one case which has to
     * observe a timeout does not cost a minute.
     */
    private static final long SHORT_TIMEOUT_MS = 2000;

    /**
     * Well below the production latch timeout, well above the milliseconds the call needs when every
     * mark is counted down. Only has to tell "returned" apart from "waited out the timeout".
     */
    private static final long NO_TIMEOUT_WAIT_MS = 30000;

    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(SECOND_BACKEND_ID);
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        starRocksAssert.withTable("CREATE TABLE " + TABLE_NAME + " ("
                + "id BIGINT NOT NULL AUTO_INCREMENT, v BIGINT NOT NULL) "
                + "PRIMARY KEY (id) DISTRIBUTED BY HASH(id) BUCKETS 3 "
                + "PROPERTIES('replication_num' = '1')");
    }

    @AfterEach
    public void resetCluster() {
        clusterInfo().getBackends().forEach(backend -> backend.setAlive(true));
        OlapTable.dropAutoIncrementMapTimeoutMs = DEFAULT_TIMEOUT_MS;
        // A task the strict variant left behind for a dead node stays queued forever here: there is
        // no BE report to make ReportHandler resend it and no finishTask to remove it.
        AgentTaskQueue.clearAllTasks();
    }

    @Test
    public void testBestEffortWithEveryNodeAlive() {
        long elapsedMs = timeBestEffort();
        Assertions.assertTrue(elapsedMs < NO_TIMEOUT_WAIT_MS, "elapsedMs=" + elapsedMs);
    }

    @Test
    public void testBestEffortSkipsDeadNode() {
        setAlive(SECOND_BACKEND_ID, false);

        long elapsedMs = timeBestEffort();

        Assertions.assertTrue(elapsedMs < NO_TIMEOUT_WAIT_MS,
                "a dead node must not be waited for, elapsedMs=" + elapsedMs);
        // Never enqueued, so there was never a mark to wait on. A task queued for a dead node would
        // still be here - nothing acknowledges it - so zero means it was skipped, not completed.
        Assertions.assertEquals(0, queuedTaskNum(SECOND_BACKEND_ID));
    }

    @Test
    public void testBestEffortWithNoNodeAlive() {
        clusterInfo().getBackends().forEach(backend -> backend.setAlive(false));

        // Nothing to tell, so nothing to wait for: no task is built and no mark is ever added.
        long elapsedMs = timeBestEffort();

        Assertions.assertTrue(elapsedMs < NO_TIMEOUT_WAIT_MS,
                "with no live node there is nothing to wait for, elapsedMs=" + elapsedMs);
    }

    /**
     * ALTER TABLE ... AUTO_INCREMENT and RESTORE move the table's counter and only proceed when this
     * returns true, so it must NOT report success while a node that could still hold a reserved
     * interval was never told. Such a node goes {@code isAlive == false} on failed heartbeats and
     * comes back on the next successful one without restarting, so its cache outlives the outage.
     *
     * <p>This is the one case that has to observe a timeout - the refusal only arrives once the
     * latch gives up - so it shortens the wait instead of sitting out the production minute.
     */
    @Test
    public void testStrictRefusesWhenANodeIsDead() {
        setAlive(SECOND_BACKEND_ID, false);
        OlapTable.dropAutoIncrementMapTimeoutMs = SHORT_TIMEOUT_MS;

        Assertions.assertFalse(getTable().sendDropAutoIncrementMapTask(),
                "a node that was never told must not be reported as invalidated");
        // Still queued, which is what lets ReportHandler resend it once the node reports again.
        Assertions.assertEquals(1, queuedTaskNum(SECOND_BACKEND_ID));
    }

    /**
     * Runs the best-effort variant and returns how long it took, asserting it succeeded on the way:
     * every node it targets is alive, so every mark it adds gets counted down.
     */
    private static long timeBestEffort() {
        long start = System.currentTimeMillis();
        boolean ok = getTable().sendDropAutoIncrementMapTaskBestEffort();
        long elapsedMs = System.currentTimeMillis() - start;
        Assertions.assertTrue(ok, "every targeted node is alive, so none of them can time out");
        return elapsedMs;
    }

    private static int queuedTaskNum(long backendId) {
        return AgentTaskQueue.getTaskNum(backendId, TTaskType.DROP_AUTO_INCREMENT_MAP, false);
    }

    private static void setAlive(long backendId, boolean alive) {
        Backend backend = clusterInfo().getBackend(backendId);
        Assertions.assertNotNull(backend);
        backend.setAlive(alive);
    }

    private static OlapTable getTable() {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(DB_NAME).getTable(TABLE_NAME);
    }

    private static SystemInfoService clusterInfo() {
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
    }
}
