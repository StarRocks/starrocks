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

import com.google.common.collect.Lists;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.mv.analyzer.MVPartitionExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The asynchronous rebuild of a replayed ACTIVE analyzes the partition exprs outside every lock, while a replayed
 * inactivation resets what that analysis publishes. Each reader of the ref base tables' partition info re-gets an
 * Iceberg table through the table infos recorded alongside, and checks they are there.
 */
public class MVReplayActivateResetRaceTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    private static boolean isReplayRebuild() {
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
            if (frame.getMethodName().contains("activateOnReplay")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Parks the rebuild in the middle of the partition expr analysis, replays an inactivation, and lets the
     * rebuild finish. It used to publish the table infos first and the partition maps one by one afterwards, so
     * the reset in between left partition maps whose tables had no table info, and every reader then failed
     * with an IllegalStateException until the mv was activated again.
     */
    @Test
    public void testAnInactivationDuringTheRebuildLeavesNoTornPartitionInfo() throws Exception {
        String mvName = "mv_replay_activate_reset_race";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n"
                + "PARTITION BY str2date(`date`, '%Y-%m-%d')\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT id, data, date FROM iceberg0.partitioned_db.t1;");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            MaterializedView mv = getMv(DB_NAME, mvName);
            Assertions.assertFalse(mv.getRefBaseTablePartitionExprs().isEmpty(),
                    "the mv has no ref base table partition exprs, nothing can be torn");
            List<BaseTableInfo> infos = Lists.newArrayList(mv.getBaseTableInfos());
            mv.setInactiveAndReason("test: forced inactive before the replay");

            // Only the rebuild is parked: the mv plan cache executor may analyze the same mv on another thread.
            new MockUp<MvUtils>() {
                @Mock
                public List<MVPartitionExpr> getMvPartitionExpr(Invocation invocation,
                                                                Map<Expr, SlotRef> partitionExprMaps, Table table) {
                    if (isReplayRebuild() && entered.getCount() > 0) {
                        entered.countDown();
                        try {
                            release.await(60, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return invocation.proceed(partitionExprMaps, table);
                }
            };
            CompletableFuture<?> rebuild = mv.activateOnReplay(infos);
            Assertions.assertTrue(entered.await(30, TimeUnit.SECONDS),
                    "the rebuild never analyzed the partition exprs, nothing was in flight");

            mv.setInactiveAndReason("test: a later replayed INACTIVE");
            release.countDown();
            rebuild.get(30, TimeUnit.SECONDS);

            Assertions.assertFalse(mv.isActive(), "the stale verdict re-activated the MV");
            // Each of these re-gets the Iceberg ref table through the published table infos.
            Assertions.assertTrue(mv.getRefBaseTablePartitionExprs().isEmpty(), "the reset was published over");
            Assertions.assertTrue(mv.getRefBaseTablePartitionSlots().isEmpty(), "the reset was published over");
            Assertions.assertTrue(mv.getRefBaseTablePartitionColumns().isEmpty(), "the reset was published over");
        } finally {
            release.countDown();
            starRocksAssert.dropMaterializedView(mvName);
        }
    }
}
