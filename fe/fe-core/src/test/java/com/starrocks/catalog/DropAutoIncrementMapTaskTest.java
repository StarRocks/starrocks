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
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A node that is not alive never receives the task {@code AgentBatchTask.run()} would have sent it,
 * so nobody ever counts its latch mark down and the caller waits out the whole latch timeout - once
 * per auto-increment table, all of it under the database WRITE lock held by DROP DATABASE.
 * {@link OlapTable#sendDropAutoIncrementMapTask()} therefore has to leave dead nodes out of the
 * latch entirely.
 */
public class DropAutoIncrementMapTaskTest {
    private static final int SECOND_BACKEND_ID = 10002;
    private static final String DB_NAME = "test_drop_auto_increment_map";
    private static final String TABLE_NAME = "t_auto_inc";

    /**
     * Well below the 60s latch timeout, well above the milliseconds the call needs when every mark
     * is counted down. Only has to tell "returned" apart from "waited out the timeout".
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
    public void reviveBackends() {
        clusterInfo().getBackends().forEach(backend -> backend.setAlive(true));
    }

    @Test
    public void testAllNodesAlive() {
        long elapsedMs = timeSendDropTask();
        Assertions.assertTrue(elapsedMs < NO_TIMEOUT_WAIT_MS, "elapsedMs=" + elapsedMs);
    }

    @Test
    public void testDeadNodeIsSkipped() {
        setAlive(SECOND_BACKEND_ID, false);

        long elapsedMs = timeSendDropTask();

        Assertions.assertTrue(elapsedMs < NO_TIMEOUT_WAIT_MS,
                "a dead node must not be waited for, elapsedMs=" + elapsedMs);
    }

    @Test
    public void testEveryNodeDead() {
        clusterInfo().getBackends().forEach(backend -> backend.setAlive(false));

        // Nothing to tell, so nothing to wait for: no task is built and no mark is ever added.
        long elapsedMs = timeSendDropTask();

        Assertions.assertTrue(elapsedMs < NO_TIMEOUT_WAIT_MS,
                "with no live node there is nothing to wait for, elapsedMs=" + elapsedMs);
    }

    /**
     * Returns how long the call took. Also asserts it succeeded: before dead nodes were filtered out
     * the latch could not reach zero, so {@code latch.await} returned false after the full timeout.
     */
    private static long timeSendDropTask() {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(DB_NAME).getTable(TABLE_NAME);
        long start = System.currentTimeMillis();
        boolean ok = table.sendDropAutoIncrementMapTask();
        long elapsedMs = System.currentTimeMillis() - start;
        Assertions.assertTrue(ok, "sendDropAutoIncrementMapTask() must not report failure");
        return elapsedMs;
    }

    private static void setAlive(long backendId, boolean alive) {
        Backend backend = clusterInfo().getBackend(backendId);
        Assertions.assertNotNull(backend);
        backend.setAlive(alive);
    }

    private static SystemInfoService clusterInfo() {
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
    }
}
