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

import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A partition shape change drops populated partitions before the insert that fills their replacements commits.
 * The recovery state (IVM checkpoint and rewrite freshness) must be invalidated <em>before</em> the first drop, so
 * a failure in the middle of the synchronization cannot leave the mv claiming to be fresh over empty
 * replacements.
 */
public class MVMirrorShapeChangeFailureTest extends MVTestBase {

    private static final String MAY_RANGE = "PARTITION p202105 values [('2021-05-01'),('2021-06-01'))";
    private static final String COARSE_MAY = "p20210501_20210601";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
    }

    @Test
    public void testFailedShapeChangeKeepsRecoveryState() throws Exception {
        boolean original = Config.enable_mv_mirror_base_partition;
        Config.enable_mv_mirror_base_partition = true;
        // Fail only the partition creation of the migration under test: the mock stays pass-through otherwise.
        AtomicBoolean failAdds = new AtomicBoolean(false);
        new MockUp<LocalMetastore>() {
            @Mock
            public void addPartitions(Invocation invocation, ConnectContext ctx, Database db, String tableName,
                                      AddPartitionClause clause) throws DdlException {
                if (failAdds.get()) {
                    throw new DdlException("injected partition creation failure");
                }
                invocation.proceed(ctx, db, tableName, clause);
            }
        };
        try {
            starRocksAssert.withTable("CREATE TABLE test.mirror_shape_fail_base\n" +
                    "(k1 date, k2 int, v1 int)\n" +
                    "PARTITION BY RANGE(k1)\n(" + MAY_RANGE + ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                    "PROPERTIES('replication_num'='1');");
            executeInsertSql(connectContext,
                    "insert into mirror_shape_fail_base partition(p202105) values('2021-05-06', 1, 1);");
            starRocksAssert.withMaterializedView(
                    "create materialized view test.mv_mirror_shape_fail\n" +
                            "partition by date_trunc('day', k1)\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "properties('replication_num'='1', 'partition_refresh_number'='-1')\n" +
                            "refresh deferred manual\n" +
                            "as select k1, k2, sum(v1) as v1 from test.mirror_shape_fail_base group by k1, k2",
                    (mvName) -> {
                        MaterializedView mv = getMv(DB_NAME, (String) mvName);
                        TaskRun initial = buildMVTaskRun(mv, DB_NAME);
                        initial.getProperties().put(TaskRun.FORCE, "true");
                        initial.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, initial.executeTaskRun(),
                                "the initial refresh must succeed");
                        Assertions.assertEquals(Sets.newHashSet(COARSE_MAY),
                                mv.getRangePartitionMap().getPartitionNames());
                        // Arrange the state a completed refresh leaves behind: freshness confirmed by the last
                        // successful run.
                        mv.getRefreshScheme().setLastFreshnessConfirmedAt(System.currentTimeMillis());

                        // Disabling mirror makes this refresh drop the populated coarse month and create day
                        // partitions; failing the creation stops the run between the drop and the insert.
                        Config.enable_mv_mirror_base_partition = false;
                        failAdds.set(true);
                        TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                        taskRun.getProperties().put(TaskRun.FORCE, "true");
                        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        Assertions.assertThrows(DmlException.class, taskRun::executeTaskRun,
                                "the refresh must fail when the replacement partitions cannot be created");

                        Assertions.assertFalse(mv.getRangePartitionMap().getPartitionNames().contains(COARSE_MAY),
                                "the destructive drop must have happened before the failure");
                        Assertions.assertEquals(0L, mv.getRefreshScheme().getLastFreshnessConfirmedAt(),
                                "the freshness must be invalidated before the first drop, otherwise query rewrite "
                                        + "could use the empty replacements");

                        // With the recovery state persisted before the destructive drop, the next refresh
                        // re-materializes the replacements instead of reporting the mv up to date.
                        failAdds.set(false);
                        TaskRun recovery = buildMVTaskRun(mv, DB_NAME);
                        recovery.getProperties().put(TaskRun.FORCE, "true");
                        recovery.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
                        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, recovery.executeTaskRun(),
                                "the recovery refresh did not succeed");
                        Assertions.assertEquals(31, mv.getRangePartitionMap().getPartitionNames().size(),
                                "after: " + mv.getRangePartitionMap().getPartitionNames());
                        // Only a committed refresh records the mv partition's associated base partitions, so this
                        // proves the replacements were materialized instead of being left empty.
                        Assertions.assertTrue(mv.getRefreshScheme().getAsyncRefreshContext()
                                        .getMvPartitionNameRefBaseTablePartitionMap()
                                        .containsKey("p20210506_20210507"),
                                "the replacement partitions must be materialized, not left empty: "
                                        + mv.getRefreshScheme().getAsyncRefreshContext()
                                        .getMvPartitionNameRefBaseTablePartitionMap().keySet());
                        Assertions.assertTrue(mv.isActive(), "mv inactive: " + mv.getInactiveReason());
                    });
        } finally {
            Config.enable_mv_mirror_base_partition = original;
        }
    }
}
