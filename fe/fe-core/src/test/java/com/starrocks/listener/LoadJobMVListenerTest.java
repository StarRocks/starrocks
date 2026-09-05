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

package com.starrocks.listener;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.PartitionRangeDesc;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.util.EitherOr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadJobMVListenerTest extends MVTestBase {

    private static Database getDb() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
    }

    private static Table getBaseTable(String tableName) {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB_NAME, tableName);
    }

    private AtomicInteger mockRefreshCallCount() {
        AtomicInteger callCount = new AtomicInteger(0);
        new MockUp<LocalMetastore>() {
            @Mock
            public String refreshMaterializedView(String dbName, String mvName, boolean force,
                                                   EitherOr<PartitionRangeDesc, Set<PListCell>> partitionDesc,
                                                   int priority, boolean mergeRedundant, boolean isManual) {
                callCount.incrementAndGet();
                return null;
            }
        };
        return callCount;
    }

    @Test
    public void testSkipsAutoTriggerWhenMvSuspendedByConsecutiveFailures() throws Exception {
        starRocksAssert.withTable("CREATE TABLE lt_base_t1 (\n" +
                "   k1 int,\n" +
                "   k2 date,\n" +
                "   k3 string\n" +
                ")\n" +
                "DUPLICATE KEY(k1);");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW lt_mv1\n" +
                "REFRESH ASYNC\n" +
                "AS select sum(k1), k2, k3 from lt_base_t1 group by k2, k3;");
        MaterializedView mv = getMv("lt_mv1");
        mv.setInactiveAndReason(MaterializedViewExceptions.inactiveReasonForConsecutiveFailures(mv.getName()));

        AtomicInteger callCount = mockRefreshCallCount();
        LoadJobMVListener.INSTANCE.onTableDataChange(getDb(), getBaseTable("lt_base_t1"));

        Assertions.assertEquals(0, callCount.get(),
                "auto-trigger must not resubmit a refresh for an MV suspended by consecutive failures");
    }

    @Test
    public void testStillTriggersForTransientlyInactiveMv() throws Exception {
        starRocksAssert.withTable("CREATE TABLE lt_base_t2 (\n" +
                "   k1 int,\n" +
                "   k2 date,\n" +
                "   k3 string\n" +
                ")\n" +
                "DUPLICATE KEY(k1);");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW lt_mv2\n" +
                "REFRESH ASYNC\n" +
                "AS select sum(k1), k2, k3 from lt_base_t2 group by k2, k3;");
        MaterializedView mv = getMv("lt_mv2");
        // A reason that is NOT in MVActiveChecker's non-auto-activatable set -- retry should still
        // be attempted so a transient breakage keeps self-healing on the next write.
        mv.setInactiveAndReason("some transient error unrelated to the blocked reasons");

        AtomicInteger callCount = mockRefreshCallCount();
        LoadJobMVListener.INSTANCE.onTableDataChange(getDb(), getBaseTable("lt_base_t2"));

        Assertions.assertEquals(1, callCount.get(),
                "auto-trigger must still retry an MV that is inactive for a self-healing reason");
    }

    @Test
    public void testStillTriggersForActiveMv() throws Exception {
        starRocksAssert.withTable("CREATE TABLE lt_base_t3 (\n" +
                "   k1 int,\n" +
                "   k2 date,\n" +
                "   k3 string\n" +
                ")\n" +
                "DUPLICATE KEY(k1);");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW lt_mv3\n" +
                "REFRESH ASYNC\n" +
                "AS select sum(k1), k2, k3 from lt_base_t3 group by k2, k3;");

        AtomicInteger callCount = mockRefreshCallCount();
        LoadJobMVListener.INSTANCE.onTableDataChange(getDb(), getBaseTable("lt_base_t3"));

        Assertions.assertEquals(1, callCount.get(), "a healthy, active MV must still auto-refresh on write");
    }
}
