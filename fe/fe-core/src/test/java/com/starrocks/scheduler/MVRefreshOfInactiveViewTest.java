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

package com.starrocks.scheduler;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.scheduler.mv.MVRefreshSchemaChecker;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A refresh that fails because it found the view inactive broke nothing, even when an ALTER ... ACTIVE lands before
 * {@link TaskRun} handles the failure.
 */
public class MVRefreshOfInactiveViewTest extends MVTestBase {
    private static final AtomicInteger VIEWS = new AtomicInteger();

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE test.tbl_inactive_refresh (k1 date, k2 int, v1 int)\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p0 VALUES [('2022-01-01'), ('2022-02-01')),\n" +
                "    PARTITION p1 VALUES [('2022-02-01'), ('2022-03-01'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
    }

    @Test
    public void testAViewActivatedAfterPartitionSyncFoundItInactiveIsNotBroken() throws Exception {
        MaterializedView mv = pctView();
        // a base column change lands after the run's prepare, before its partition sync
        new MockUp<MVRefreshSchemaChecker>() {
            @Mock
            public void checkExternalBaseSchemaCompat(MaterializedView view) {
                view.setInactiveAndReason(MaterializedViewExceptions.inactiveReasonForColumnChanged(Set.of("v1")));
            }
        };
        activateWhenTheFailureIsHandled(mv);

        Exception e = Assertions.assertThrows(Exception.class,
                () -> initAndExecuteTaskRun(buildMVTaskRun(mv, DB_NAME)));
        Assertions.assertTrue(e.getMessage().contains("is not active due to base table schema changed for columns: v1"),
                e.getMessage());
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
    }

    @Test
    public void testAViewActivatedAfterPrepareFoundItInactiveIsNotBroken() throws Exception {
        MaterializedView mv = pctView();
        mv.setInactiveAndReason(MaterializedViewExceptions.inactiveReasonForColumnChanged(Set.of("v1")));
        // the run's own activation fails, as it does while the column is still missing
        new MockUp<MVActiveChecker>() {
            @Mock
            public void tryToActivate(MaterializedView view) {
            }
        };
        activateWhenTheFailureIsHandled(mv);

        Exception e = Assertions.assertThrows(Exception.class,
                () -> initAndExecuteTaskRun(buildMVTaskRun(mv, DB_NAME)));
        Assertions.assertTrue(e.getMessage().contains("is not active due to base table schema changed for columns: v1"),
                e.getMessage());
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
    }

    private MaterializedView pctView() throws Exception {
        String name = "mv_inactive_refresh_" + VIEWS.incrementAndGet();
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test." + name + "\n" +
                "PARTITION BY k1\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES ('replication_num' = '1', 'refresh_mode' = 'pct')\n" +
                "AS SELECT k1, k2, SUM(v1) AS total FROM test.tbl_inactive_refresh GROUP BY k1, k2;");
        return getMv(DB_NAME, name);
    }

    /** An ALTER ... ACTIVE that lands between the run seeing the view inactive and its failure being handled. */
    private static void activateWhenTheFailureIsHandled(MaterializedView mv) {
        new MockUp<Task>() {
            @Mock
            public int incConsecutiveFailCount(Invocation invocation) {
                mv.setActive();
                return invocation.proceed();
            }
        };
    }
}
