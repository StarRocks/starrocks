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

package com.starrocks.scheduler.mv.ivm;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.tvr.TvrDeltaStats;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.load.loadv2.IVMInsertLoadTxnCallback;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.scheduler.mv.hybrid.MVHybridRefreshProcessor;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Map;
import java.util.Set;

@TestMethodOrder(MethodName.class)
public class IVMBasedMvRefreshProcessorIcebergTest extends MVIVMIcebergTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
        starRocksAssert.useDatabase("test");
        starRocksAssert.withTable(cluster, "depts");
        starRocksAssert.withTable(cluster, "emps");
    }

    @Test
    public void testIVMWithScan() throws Exception {
        doTestWith3Runs("SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;",
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "TABLE: unpartitioned_db.t0\n" +
                                    "     TABLE VERSION: Delta@[MIN,1]");
                },
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "TABLE: unpartitioned_db.t0\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                }
        );
    }

    @Test
    public void testIVMWithScanProjectFilter() throws Exception {
        doTestWith3Runs("SELECT id * 2 + 1, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` where id > 10;",
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "     TABLE: unpartitioned_db.t0\n" +
                                    "     PREDICATES: 1: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 1: id > 10\n" +
                                    "     TABLE VERSION: Delta@[MIN,1]");
                },
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  0:IcebergScanNode\n" +
                                    "     TABLE: unpartitioned_db.t0\n" +
                                    "     PREDICATES: 1: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 1: id > 10\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                }
        );
    }

    @Test
    public void testIVMWithJoin1() throws Exception {
        doTestWith3Runs("SELECT a.id * 2 + 1, b.data FROM `iceberg0`.`unpartitioned_db`.`t0` a inner join " +
                        "`iceberg0`.`partitioned_db`.`t1` b on a.id=b.id where a.id > 10;",
                plan -> {
                    String planStr = plan.getExplainString(TExplainLevel.COSTS);
                    // First run: should have delta scan and snapshot/delta scans via UNION
                    PlanTestBase.assertContains(planStr, "TABLE: unpartitioned_db.t0");
                    PlanTestBase.assertContains(planStr, "TABLE: partitioned_db.t1");
                    PlanTestBase.assertContains(planStr, "TABLE VERSION: Delta@[MIN,1]");
                    PlanTestBase.assertContains(planStr, "UNION");
                },
                plan -> {
                    String planStr = plan.getExplainString(TExplainLevel.COSTS);
                    // Third run: should have delta and snapshot scans
                    PlanTestBase.assertContains(planStr, "TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(planStr, "UNION");
                    PlanTestBase.assertContains(planStr, "TABLE: unpartitioned_db.t0");
                    PlanTestBase.assertContains(planStr, "TABLE: partitioned_db.t1");
                }
        );
    }

    @Test
    public void testIVMWithJoin2() throws Exception {
        doTestWith3Runs("SELECT a.id * 2 + 1, b.data, c.a as ca FROM " +
                        "`iceberg0`.`unpartitioned_db`.`t0` a " +
                        "   join `iceberg0`.`partitioned_db`.`t1` b join `iceberg0`.`partitioned_db`.`part_tbl1` c " +
                        "   on a.id=b.id and a.id=c.c where a.id > 10;",
                plan -> {
                    String planStr = plan.getExplainString(TExplainLevel.COSTS);
                    // First run: 3-table join with delta scans
                    PlanTestBase.assertContains(planStr, "TABLE: unpartitioned_db.t0");
                    PlanTestBase.assertContains(planStr, "TABLE: partitioned_db.t1");
                    PlanTestBase.assertContains(planStr, "TABLE VERSION: Delta@[MIN,1]");
                    PlanTestBase.assertContains(planStr, "UNION");
                },
                plan -> {
                    String planStr = plan.getExplainString(TExplainLevel.COSTS);
                    // Third run: should have delta and snapshot scans
                    PlanTestBase.assertContains(planStr, "TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(planStr, "UNION");
                    PlanTestBase.assertContains(planStr, "TABLE: unpartitioned_db.t0");
                    PlanTestBase.assertContains(planStr, "TABLE: partitioned_db.t1");
                }
        );
    }

    @Test
    public void testPartitionedIVMWithScan() throws Exception {
        doTestWith3Runs("SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1` as a;",
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "  0:IcebergScanNode\n" +
                                    "     TABLE: partitioned_db.t1\n" +
                                    "     TABLE VERSION: Delta@[MIN,1]");
                },
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "  0:IcebergScanNode\n" +
                                    "     TABLE: partitioned_db.t1\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                }
        );
    }

    @Test
    public void testPartitionedIVMWithScanWithDisableIcebergIdentityColumnOptimize() throws Exception {
        connectContext.getSessionVariable().setEnableIcebergIdentityColumnOptimize(false);
        doTestWith3Runs("SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1` as a;",
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "  0:IcebergScanNode\n" +
                                    "     TABLE: partitioned_db.t1\n" +
                                    "     TABLE VERSION: Delta@[MIN,1]");
                },
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "  0:IcebergScanNode\n" +
                                    "     TABLE: partitioned_db.t1\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                }
        );
        connectContext.getSessionVariable().setEnableIcebergIdentityColumnOptimize(true);
    }

    @Test
    public void testUnionAll() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(true);
        doTestWith3Runs("SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1` as a " +
                        "UNION ALL SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` as b;",
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  3:IcebergScanNode\n" +
                                    "     TABLE: unpartitioned_db.t0\n" +
                                    "     TABLE VERSION: Delta@[MIN,1]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  0:IcebergScanNode\n" +
                                    "     TABLE: partitioned_db.t1\n" +
                                    "     TABLE VERSION: Delta@[MIN,1]");
                },
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  3:IcebergScanNode\n" +
                                    "     TABLE: unpartitioned_db.t0\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  0:IcebergScanNode\n" +
                                    "     TABLE: partitioned_db.t1\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                }
        );
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(false);
    }

    @Test
    public void testPartitionedIVMWithAggregate1() throws Exception {
        doTestWith3Runs("SELECT date, sum(id), approx_count_distinct(data) " +
                        "FROM `iceberg0`.`partitioned_db`.`t1` as a group by date;",
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  0:IcebergScanNode\n" +
                                    "     TABLE: partitioned_db.t1\n" +
                                    "     TABLE VERSION: Delta@[MIN,1]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  7:HASH JOIN\n" +
                                    "  |  join op: RIGHT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                                    "  |  colocate: false, reason: \n" +
                                    "  |  equal join conjunct: 9: __ROW_ID__ = 17: from_binary");
                },
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  0:IcebergScanNode\n" +
                                    "     TABLE: partitioned_db.t1\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  6:OlapScanNode\n" +
                                    "     TABLE: test_mv1");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  7:HASH JOIN\n" +
                                    "  |  join op: RIGHT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                                    "  |  colocate: false, reason: \n" +
                                    "  |  equal join conjunct: 9: __ROW_ID__ = 17: from_binary");
                }
        );
    }

    @Test
    public void testJoinAndAggregate1() throws Exception {
        doTestWith3Runs("SELECT b.data, sum(a.id * 2 + 1) FROM `iceberg0`.`unpartitioned_db`.`t0` a " +
                        "inner join `iceberg0`.`partitioned_db`.`t1` b on a.id=b.id where a.id > 10 GROUP BY b.data;",
                plan -> {
                    String planStr = plan.getExplainString(TExplainLevel.NORMAL);
                    // Aggregate MV: LEFT OUTER JOIN with MV state + state_union
                    PlanTestBase.assertContains(planStr, "LEFT OUTER JOIN");
                    PlanTestBase.assertContains(planStr, "__ROW_ID__");
                    PlanTestBase.assertContains(planStr, "TABLE: test_mv1");
                    PlanTestBase.assertContains(planStr, "state_union");
                },
                plan -> {
                    String planStr = plan.getExplainString(TExplainLevel.NORMAL);
                    PlanTestBase.assertContains(planStr, "TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(planStr, "LEFT OUTER JOIN");
                    PlanTestBase.assertContains(planStr, "__ROW_ID__");
                    PlanTestBase.assertContains(planStr, "state_union");
                }
        );
    }

    @Test
    public void testJoinAndAggregate2() throws Exception {
        doTestWith3Runs("SELECT b.data, sum(a.id) as a1, sum(b.id) as b1, avg(a.id) as a2, avg(b.id) as b2, " +
                        "min(a.id) as a3, min(b.id) as b3, max(a.id) as a4, max(b.id) as b4, " +
                        "count(a.id) as a5, count(b.id) as b5, " +
                        "approx_count_distinct(a.id) as a6, approx_count_distinct(b.id) as b6 " +
                        "FROM " +
                        "   `iceberg0`.`unpartitioned_db`.`t0` a inner join `iceberg0`.`partitioned_db`.`t1` b " +
                        "   on a.id=b.id where a.id > 10 GROUP BY b.data;",
                plan -> {
                    String planStr = plan.getExplainString(TExplainLevel.NORMAL);
                    PlanTestBase.assertContains(planStr, "LEFT OUTER JOIN");
                    PlanTestBase.assertContains(planStr, "__ROW_ID__");
                    PlanTestBase.assertContains(planStr, "TABLE: test_mv1");
                    PlanTestBase.assertContains(planStr, "state_union");
                },
                plan -> {
                    String planStr = plan.getExplainString(TExplainLevel.NORMAL);
                    PlanTestBase.assertContains(planStr, "TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(planStr, "LEFT OUTER JOIN");
                    PlanTestBase.assertContains(planStr, "__ROW_ID__");
                    PlanTestBase.assertContains(planStr, "state_union");
                }
        );
    }

    @Test
    public void testJoinAndAggregateMVSchema() throws Exception {
        withMVQuery("SELECT b.data, " +
                        "   sum(a.id) as a1, sum(b.id) as b1, avg(a.id) as a2, avg(b.id) as b2, " +
                        "   min(a.id) as a3, min(b.id) as b3, max(a.id) as a4, max(b.id) as b4, " +
                        "   count(a.id) as a5, count(b.id) as b5, " +
                        "   approx_count_distinct(a.id) as a6, approx_count_distinct(b.id) as b6 " +
                        "FROM " +
                        "   `iceberg0`.`unpartitioned_db`.`t0` a inner join `iceberg0`.`partitioned_db`.`t1` b " +
                        "   on a.id=b.id where a.id > 10 GROUP BY b.data;",
                (mv) -> {
                    String query = String.format("select * from %s", mv.getName());
                    String plan = getFragmentPlan(query);
                    System.out.println(plan);
                    Assertions.assertTrue(plan.contains(" OUTPUT EXPRS:2: data | 3: a1 | 4: b1 | 5: a2 " +
                            "| 6: b2 | 7: a3 | 8: b3 | 9: a4 | 10: b4 | 11: a5 | 12: b5 | 13: a6 | 14: b6\n" +
                            "  PARTITION: UNPARTITIONED"));
                }
        );
    }

    @Test
    public void testUnionAllAndAggregate() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(true);
        doTestWith3Runs("SELECT date, count(data), sum(id) FROM (" +
                        "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1` as a " +
                        "UNION ALL SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` as b) t GROUP BY date;",
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  3:IcebergScanNode\n" +
                                    "     TABLE: unpartitioned_db.t0\n" +
                                    "     TABLE VERSION: Delta@[MIN,1]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  0:IcebergScanNode\n" +
                                    "     TABLE: partitioned_db.t1\n" +
                                    "     TABLE VERSION: Delta@[MIN,1]");
                },
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  3:IcebergScanNode\n" +
                                    "     TABLE: unpartitioned_db.t0\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  0:IcebergScanNode\n" +
                                    "     TABLE: partitioned_db.t1\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                }
        );
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(false);
    }

    @Test
    public void testIncrementalRefreshWithBadJoinOperator() {
        Assertions.assertThrowsExactly(AnalysisException.class, () -> {
            String query = "SELECT a.id * 2 + 1, b.data FROM `iceberg0`.`unpartitioned_db`.`t0` a full join " +
                    "`iceberg0`.`partitioned_db`.`t1` b on a.id=b.id where a.id > 10;";
            MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental");
        });
    }

    @Test
    public void testIncrementalRefreshWithBadJoinAggOperator() {
        Assertions.assertThrowsExactly(AnalysisException.class, () -> {
            String query = "SELECT a.id * 2 + 1, b.data FROM " +
                    " (select id, count(*) from `iceberg0`.`unpartitioned_db`.`t0` group by id) a inner join " +
                    "`iceberg0`.`partitioned_db`.`t1` b on a.id=b.id where a.id > 10;";
            MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental");
        });
    }

    @Test
    public void testIncrementalRefreshWithBadSetOperator() {
        Assertions.assertThrowsExactly(AnalysisException.class, () -> {
            String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1` as a " +
                    " UNION SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` as b;";
            MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental");
        });

        Assertions.assertThrowsExactly(AnalysisException.class, () -> {
            String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1` as a " +
                    " MINUS SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` as b;";
            MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental");
        });

        Assertions.assertThrowsExactly(AnalysisException.class, () -> {
            String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1` as a " +
                    " EXCEPT SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` as b;";
            MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental");
        });
    }

    @Test
    public void testIncrementalRefreshWithWindowOperator() {
        Assertions.assertThrowsExactly(AnalysisException.class, () -> {
            String query = "SELECT id, count(data) over (partition by date)  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
            MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental");
        });
    }

    @Test
    public void testIncrementalRefreshWithOrderOperator() {
        Assertions.assertThrowsExactly(AnalysisException.class, () -> {
            String query = "SELECT id, count(data) FROM `iceberg0`.`unpartitioned_db`.`t0` as a group by id order by id;";
            MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental");
        });
    }

    @Test
    public void testIncrementalRefreshWithTableFunctionOperator() {
        Assertions.assertThrowsExactly(AnalysisException.class, () -> {
            String query = "SELECT id, unnest FROM `iceberg0`.`unpartitioned_db`.`t0` as a, unnest(split(data, ','));";
            createMaterializedViewWithRefreshMode(query, "incremental");
        });
    }

    @Test
    public void testAutoRefreshWithTableFunctionOperator() throws Exception {
        String query = "SELECT id, unnest FROM `iceberg0`.`unpartitioned_db`.`t0` as a, unnest(split(data, ','));";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");
        Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
    }

    @Test
    public void testIncrementalRefreshWithScanOperator() throws Exception {
        String query = "SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental");
        Assertions.assertEquals(MaterializedView.RefreshMode.INCREMENTAL, mv.getCurrentRefreshMode());
    }

    @Test
    public void testFullRefreshWithScanOperator() throws Exception {
        String query = "SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "full");
        Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
    }

    @Test
    public void testFullRefreshWithTableFunctionOperator() throws Exception {
        String query = "SELECT id, unnest FROM `iceberg0`.`unpartitioned_db`.`t0` as a, unnest(split(data, ','));";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "full");
        Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
    }

    @Test
    public void testAutoRefreshWithScanOperator() throws Exception {
        String query = "SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");
        Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());
    }

    @Test
    public void testIncrementalRefreshWithOlapScanOperator() throws Exception {
        Assertions.assertThrowsExactly(AnalysisException.class, () -> {
            String query = "SELECT * from emps;";
            createMaterializedViewWithRefreshMode(query, "incremental");
        });
    }

    @Test
    public void testFullRefreshWithOlapScanOperator() throws Exception {
        String query = "SELECT * from emps;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "full");
        Assertions.assertEquals(MaterializedView.RefreshMode.FULL, mv.getCurrentRefreshMode());
    }

    @Test
    public void testAutoRefreshWithOlapScanOperator() throws Exception {
        String query = "SELECT * from emps;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");
        Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
    }

    @Test
    public void testAutoRefreshWithRetractableChanges1() throws Exception {
        String query = "SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");
        Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());

        // Without a checkpoint, AUTO should bypass IVM planning and fall back to PCT.
        {
            advanceTableVersionTo(2);
            MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(mv);
            Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);
            MVHybridRefreshProcessor hybridBasedRefreshProcessor =
                    (MVHybridRefreshProcessor) mvTaskRunProcessor.getMVRefreshProcessor();
            Assertions.assertTrue(hybridBasedRefreshProcessor.getCurrentProcessor() instanceof MVPCTRefreshProcessor);
        }
        // Once a checkpoint exists, append-only changes can switch AUTO back to IVM.
        {
            MaterializedView refreshedMv = getMv("test_mv1");
            advanceTableVersionTo(3);
            mockListTableDeltaTraits(ImmutableList.of(
                    TvrTableDeltaTrait.ofMonotonic(
                            TvrTableDelta.of(2L, 3L),
                            TvrDeltaStats.EMPTY)
            ));
            MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(refreshedMv);
            Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);
            MVHybridRefreshProcessor hybridBasedRefreshProcessor =
                    (MVHybridRefreshProcessor) mvTaskRunProcessor.getMVRefreshProcessor();
            Assertions.assertTrue(hybridBasedRefreshProcessor.getCurrentProcessor() instanceof MVIVMRefreshProcessor);
        }
    }

    @Test
    public void testAutoRefreshFallbackCanPersistCheckpointForNextIvm() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");
        Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());

        advanceTableVersionTo(2);

        MVTaskRunProcessor run1 = getMVTaskRunProcessor(mv);
        Assertions.assertTrue(run1.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);
        MVHybridRefreshProcessor hybrid1 =
                (MVHybridRefreshProcessor) run1.getMVRefreshProcessor();
        Assertions.assertTrue(hybrid1.getCurrentProcessor() instanceof MVPCTRefreshProcessor);

        MaterializedView refreshedMv = getMv("test_mv1");
        TvrVersionRange checkpoint = refreshedMv.getRefreshScheme()
                .getAsyncRefreshContext()
                .getBaseTableInfoTvrVersionRangeMap()
                .values()
                .iterator()
                .next();
        Assertions.assertTrue(checkpoint instanceof TvrTableSnapshot);
        Assertions.assertEquals(2L, checkpoint.to().getVersion());

        advanceTableVersionTo(3);
        mockListTableDeltaTraits(ImmutableList.of(
                TvrTableDeltaTrait.ofMonotonic(
                        TvrTableDelta.of(2L, 3L),
                        TvrDeltaStats.EMPTY)
        ));

        MVTaskRunProcessor run2 = getMVTaskRunProcessor(refreshedMv);
        Assertions.assertTrue(run2.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);
        MVHybridRefreshProcessor hybrid2 =
                (MVHybridRefreshProcessor) run2.getMVRefreshProcessor();
        Assertions.assertTrue(hybrid2.getCurrentProcessor() instanceof MVIVMRefreshProcessor);
    }

    @Test
    public void testAutoRefreshFallsBackToPctWhenLineageValidationFails() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");
        Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());

        advanceTableVersionTo(2);
        MVTaskRunProcessor run1 = getMVTaskRunProcessor(mv);
        Assertions.assertTrue(run1.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);
        MVHybridRefreshProcessor hybrid1 =
                (MVHybridRefreshProcessor) run1.getMVRefreshProcessor();
        Assertions.assertTrue(hybrid1.getCurrentProcessor() instanceof MVPCTRefreshProcessor);

        MaterializedView refreshedMv = getMv("test_mv1");
        advanceTableVersionTo(3);
        mockListTableDeltaTraitsThrows("Starting snapshot (exclusive) 2 is not a parent ancestor of end snapshot 3");

        MVTaskRunProcessor run2 = getMVTaskRunProcessor(refreshedMv);
        Assertions.assertTrue(run2.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);
        MVHybridRefreshProcessor hybrid2 =
                (MVHybridRefreshProcessor) run2.getMVRefreshProcessor();
        Assertions.assertTrue(hybrid2.getCurrentProcessor() instanceof MVPCTRefreshProcessor);
    }

    @Test
    public void testAutoRefreshWithRetractableChanges2() throws Exception {
        String query = "SELECT id, count(data) over (partition by date)  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");
        Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());

        // if the base table has no retractable changes, the refresh processor should be full refresh
        {
            advanceTableVersionTo(2);
            MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(mv);
            Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVPCTRefreshProcessor);
        }
        // if the base table has retractable changes, the refresh processor should be full refresh
        {
            mockListTableDeltaTraits();
            MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(mv);
            Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVPCTRefreshProcessor);
        }
    }

    /**
     * Verify that IVM→PCT fallback allows multi-batch splitting (respects partition_refresh_number)
     * and correctly persists TVR checkpoint for the next IVM refresh.
     *
     * Before the fix: setCanGenerateNextTaskRun(false) forced all partitions into a single task_run,
     * which could cause OOM on large tables.
     *
     * After the fix: TVR is persisted in tempBaseTableInfoTvrDeltaMap (via editlog) so that
     * subsequent batch task_runs can access it, and partition_refresh_number is respected.
     */
    @Test
    public void testAutoRefreshFallbackAllowsMultiBatchAndPersistsTvr() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");
        Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());

        advanceTableVersionTo(2);
        mockListTableDeltaTraits();

        // Run 1: IVM fails (retractable changes) → fallback to PCT
        MVTaskRunProcessor run1 = getMVTaskRunProcessor(mv);
        Assertions.assertTrue(run1.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);
        MVHybridRefreshProcessor hybrid1 =
                (MVHybridRefreshProcessor) run1.getMVRefreshProcessor();
        // Verify fallback to PCT
        Assertions.assertTrue(hybrid1.getCurrentProcessor() instanceof MVPCTRefreshProcessor);

        // Verify canGenerateNextTaskRun is NOT blocked (the core fix)
        MVPCTRefreshProcessor pctProcessor =
                (MVPCTRefreshProcessor) hybrid1.getCurrentProcessor();
        Assertions.assertTrue(pctProcessor.getMvRefreshParams().isCanGenerateNextTaskRun(),
                "IVM→PCT fallback should allow multi-batch splitting (canGenerateNextTaskRun=true)");

        // Verify TVR checkpoint is persisted to baseTableInfoTvrVersionRangeMap
        MaterializedView refreshedMv = getMv("test_mv1");
        TvrVersionRange checkpoint = refreshedMv.getRefreshScheme()
                .getAsyncRefreshContext()
                .getBaseTableInfoTvrVersionRangeMap()
                .values()
                .iterator()
                .next();
        Assertions.assertTrue(checkpoint instanceof TvrTableSnapshot);
        Assertions.assertEquals(2L, checkpoint.to().getVersion());

        // Run 2: append-only changes → IVM should recover from checkpoint
        advanceTableVersionTo(3);
        mockListTableDeltaTraits(ImmutableList.of(
                TvrTableDeltaTrait.ofMonotonic(
                        TvrTableDelta.of(2L, 3L),
                        TvrDeltaStats.EMPTY)
        ));

        MVTaskRunProcessor run2 = getMVTaskRunProcessor(refreshedMv);
        Assertions.assertTrue(run2.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);
        MVHybridRefreshProcessor hybrid2 =
                (MVHybridRefreshProcessor) run2.getMVRefreshProcessor();
        // Should switch back to IVM since delta[2,3] is append-only
        Assertions.assertTrue(hybrid2.getCurrentProcessor() instanceof MVIVMRefreshProcessor,
                "After PCT fallback persists checkpoint, next IVM refresh should recover");
    }

    /**
     * Verify IVM→PCT fallback with partition_refresh_number=1 produces multi-batch task_runs
     * and only promotes TVR checkpoint on the last batch.
     *
     * Uses partitioned Iceberg table t1 (4 partitions). With partition_refresh_number=1,
     * the PCT fallback should split into multiple batches. Intermediate batches should NOT
     * promote TVR to baseTableInfoTvrVersionRangeMap; only the last batch should.
     */
    @Test
    public void testAutoRefreshFallbackMultiBatchTvrPromoteOnlyOnLastBatch() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto",
                "`date`", Map.of("partition_refresh_number", "1"));
        Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());

        advanceTableVersionTo(2);
        mockListTableDeltaTraits();

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mv.getDbId());

        // First task_run: IVM fails (retractable changes) → fallback to PCT, batch 1
        TaskRun taskRun = withMVRefreshTaskRun(db.getFullName(), mv);
        MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(taskRun);
        Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);
        MVHybridRefreshProcessor hybrid =
                (MVHybridRefreshProcessor) mvTaskRunProcessor.getMVRefreshProcessor();
        // Verify fallback to PCT
        Assertions.assertTrue(hybrid.getCurrentProcessor() instanceof MVPCTRefreshProcessor);

        // Verify canGenerateNextTaskRun is allowed
        MVPCTRefreshProcessor pctProcessor =
                (MVPCTRefreshProcessor) hybrid.getCurrentProcessor();
        Assertions.assertTrue(pctProcessor.getMvRefreshParams().isCanGenerateNextTaskRun(),
                "IVM→PCT fallback should allow multi-batch splitting");

        // Check if there are more batches (partition_refresh_number=1, 4 partitions → should have next)
        TaskRun nextTaskRun = pctProcessor.getNextTaskRun();
        if (nextTaskRun != null) {
            // Intermediate batch: TVR should NOT be promoted to baseTableInfoTvrVersionRangeMap yet
            MaterializedView intermediateMv = getMv("test_mv1");
            Map<BaseTableInfo, TvrVersionRange> intermediateCheckpoint = intermediateMv.getRefreshScheme()
                    .getAsyncRefreshContext()
                    .getBaseTableInfoTvrVersionRangeMap();
            // baseTableInfoTvrVersionRangeMap should be empty (first refresh, no previous IVM success)
            // TVR should only be promoted on the last batch
            Assertions.assertTrue(intermediateCheckpoint.isEmpty(),
                    "TVR should not be promoted to baseTableInfoTvrVersionRangeMap on intermediate batches, " +
                            "but got: " + intermediateCheckpoint);

            // tempBaseTableInfoTvrDeltaMap should have the pending TVR
            Map<BaseTableInfo, TvrVersionRange> tempTvr = intermediateMv.getRefreshScheme()
                    .getAsyncRefreshContext()
                    .getTempBaseTableInfoTvrDeltaMap();
            Assertions.assertFalse(tempTvr.isEmpty(),
                    "tempBaseTableInfoTvrDeltaMap should contain pending TVR for subsequent batches");

            // Verify owner is set during intermediate batches
            String intermediateOwner = intermediateMv.getRefreshScheme()
                    .getAsyncRefreshContext()
                    .getTempTvrOwnerStartTaskRunId();
            Assertions.assertNotNull(intermediateOwner,
                    "Owner should be set during intermediate batches");

            // Execute remaining batches
            while (nextTaskRun != null) {
                initAndExecuteTaskRun(nextTaskRun);
                MVTaskRunProcessor nextProcessor = getMVTaskRunProcessor(nextTaskRun);
                MVRefreshProcessor refreshProcessor = nextProcessor.getMVRefreshProcessor();
                if (refreshProcessor instanceof MVHybridRefreshProcessor) {
                    MVHybridRefreshProcessor nextHybrid =
                            (MVHybridRefreshProcessor) refreshProcessor;
                    nextTaskRun = nextHybrid.getCurrentProcessor().getNextTaskRun();
                } else if (refreshProcessor instanceof MVPCTRefreshProcessor) {
                    nextTaskRun = ((MVPCTRefreshProcessor) refreshProcessor).getNextTaskRun();
                } else {
                    nextTaskRun = null;
                }
            }
        }

        // After all batches complete: TVR checkpoint should be promoted
        MaterializedView finalMv = getMv("test_mv1");
        Map<BaseTableInfo, TvrVersionRange> finalCheckpoint = finalMv.getRefreshScheme()
                .getAsyncRefreshContext()
                .getBaseTableInfoTvrVersionRangeMap();
        Assertions.assertFalse(finalCheckpoint.isEmpty(),
                "TVR checkpoint should be promoted to baseTableInfoTvrVersionRangeMap after all batches complete");
        TvrVersionRange checkpoint = finalCheckpoint.values().iterator().next();
        Assertions.assertTrue(checkpoint instanceof TvrTableSnapshot);
        Assertions.assertEquals(2L, checkpoint.to().getVersion(),
                "TVR checkpoint should point to version 2");

        // Verify owner is cleared after all batches complete
        String finalOwner = finalMv.getRefreshScheme()
                .getAsyncRefreshContext()
                .getTempTvrOwnerStartTaskRunId();
        Assertions.assertNull(finalOwner,
                "Owner should be cleared after all batches complete and TVR is promoted");
    }

    /**
     * Verify that after single-batch IVM→PCT fallback, the frozen TVR in
     * tempBaseTableInfoTvrDeltaMap matches the snapshot that PCT synced to (Bug A fix).
     * Owner should be cleared after TVR promotion on the single batch.
     */
    @Test
    public void testFallbackTvrOwnerLifecycleSingleBatch() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");

        advanceTableVersionTo(2);
        mockListTableDeltaTraits();

        // Run: IVM fails → fallback to PCT (single batch, unpartitioned MV)
        MVTaskRunProcessor run1 = getMVTaskRunProcessor(mv);
        MVHybridRefreshProcessor hybrid =
                (MVHybridRefreshProcessor) run1.getMVRefreshProcessor();
        Assertions.assertTrue(hybrid.getCurrentProcessor() instanceof MVPCTRefreshProcessor);

        // After single-batch completes: TVR promoted, owner cleared
        MaterializedView refreshedMv = getMv("test_mv1");
        MaterializedView.AsyncRefreshContext ctx = refreshedMv.getRefreshScheme().getAsyncRefreshContext();

        // TVR should be promoted
        Map<BaseTableInfo, TvrVersionRange> finalCheckpoint = ctx.getBaseTableInfoTvrVersionRangeMap();
        Assertions.assertFalse(finalCheckpoint.isEmpty(),
                "TVR should be promoted after single-batch fallback");
        Assertions.assertEquals(2L, finalCheckpoint.values().iterator().next().to().getVersion(),
                "TVR should match the PCT-synced snapshot version");

        // Owner should be cleared (single batch = first batch = last batch)
        Assertions.assertNull(ctx.getTempTvrOwnerStartTaskRunId(),
                "Owner should be cleared after single-batch TVR promotion");

        // Temp map should be cleared
        Assertions.assertTrue(ctx.getTempBaseTableInfoTvrDeltaMap().isEmpty(),
                "Temp TVR map should be cleared after promotion");
    }

    /**
     * Test that IVM records complete PCT metadata without truncation by
     * partition_refresh_number.
     */
    @Test
    public void testIVMPCTMetadataNotTruncatedByPartitionRefreshNumber() throws Exception {
        // Create a PARTITIONED incremental MV on partitioned Iceberg table t1
        // (4 partitions: date=2020-01-01..04). partition_refresh_number=1 would
        // truncate to 1 partition in the old code path.
        String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental",
                "`date`", Map.of("partition_refresh_number", "1"));
        Assertions.assertEquals(MaterializedView.RefreshMode.INCREMENTAL, mv.getCurrentRefreshMode());

        advanceTableVersionTo(2);
        MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(mv);
        Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVIVMRefreshProcessor);

        MvTaskRunContext mvContext = mvTaskRunProcessor.getMvTaskRunContext();
        MVTaskRunExtraMessage extraMessage = mvContext.getStatus().getMvTaskRunExtraMessage();
        Set<String> mvPartitionsToRefresh = extraMessage.getMvPartitionsToRefresh();
        Assertions.assertNotNull(mvPartitionsToRefresh, "mvPartitionsToRefresh should not be null");
        Assertions.assertTrue(mvPartitionsToRefresh.size() > 1,
                "IVM should record complete PCT metadata without truncation, but got: " + mvPartitionsToRefresh);
    }

    @Test
    public void testAutoRecoveredIvmRecordsPCTMetadataWithoutBatchTruncation() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto",
                "`date`", Map.of("partition_refresh_number", "1"));
        Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());

        advanceTableVersionTo(2);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mv.getDbId());
        TaskRun taskRun = withMVRefreshTaskRun(db.getFullName(), mv);
        MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(taskRun);
        Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);
        MVHybridRefreshProcessor hybridProcessor =
                (MVHybridRefreshProcessor) mvTaskRunProcessor.getMVRefreshProcessor();
        Assertions.assertTrue(hybridProcessor.getCurrentProcessor() instanceof MVPCTRefreshProcessor);

        TaskRun nextTaskRun = hybridProcessor.getCurrentProcessor().getNextTaskRun();
        while (nextTaskRun != null) {
            initAndExecuteTaskRun(nextTaskRun);
            MVTaskRunProcessor nextProcessor = getMVTaskRunProcessor(nextTaskRun);
            MVRefreshProcessor refreshProcessor = nextProcessor.getMVRefreshProcessor();
            if (refreshProcessor instanceof MVHybridRefreshProcessor) {
                MVHybridRefreshProcessor nextHybrid =
                        (MVHybridRefreshProcessor) refreshProcessor;
                nextTaskRun = nextHybrid.getCurrentProcessor().getNextTaskRun();
            } else if (refreshProcessor instanceof MVPCTRefreshProcessor) {
                nextTaskRun = ((MVPCTRefreshProcessor) refreshProcessor).getNextTaskRun();
            } else {
                nextTaskRun = null;
            }
        }

        MaterializedView refreshedMv = getMv("test_mv1");
        MockIcebergMetadata mockIcebergMetadata =
                (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr()
                        .getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
        mockIcebergMetadata.updatePartitions("partitioned_db", "t1",
                ImmutableList.of("date=2020-01-02", "date=2020-01-03"));
        advanceTableVersionTo(3);
        mockListTableDeltaTraits(ImmutableList.of(
                TvrTableDeltaTrait.ofMonotonic(
                        TvrTableDelta.of(2L, 3L),
                        TvrDeltaStats.EMPTY)
        ));

        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(refreshedMv.getDbId());
        taskRun = withMVRefreshTaskRun(db.getFullName(), refreshedMv);
        mvTaskRunProcessor = getMVTaskRunProcessor(taskRun);
        Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);
        hybridProcessor = (MVHybridRefreshProcessor) mvTaskRunProcessor.getMVRefreshProcessor();
        Assertions.assertTrue(hybridProcessor.getCurrentProcessor() instanceof MVIVMRefreshProcessor);

        MvTaskRunContext mvContext = mvTaskRunProcessor.getMvTaskRunContext();
        MVTaskRunExtraMessage extraMessage = mvContext.getStatus().getMvTaskRunExtraMessage();
        Set<String> mvPartitionsToRefresh = extraMessage.getMvPartitionsToRefresh();
        Assertions.assertEquals(Set.of("p20200102", "p20200103"), mvPartitionsToRefresh,
                "Recovered IVM should record complete changed MV partitions without truncation");
    }

    @Test
    public void testIVMAggregatePlanContainsStateUnionAndMvScan() throws Exception {
        doTestWith3Runs("SELECT date, sum(id), approx_count_distinct(data) " +
                        "FROM `iceberg0`.`partitioned_db`.`t1` as a group by date;",
                plan -> {
                    String planStr = plan.getExplainString(TExplainLevel.NORMAL);
                    // Verify incremental scan
                    PlanTestBase.assertContains(planStr,
                            "TABLE VERSION: Delta@[MIN,1]");
                    // Verify LEFT/RIGHT OUTER JOIN with __ROW_ID__
                    PlanTestBase.assertContains(planStr, "HASH JOIN");
                    PlanTestBase.assertContains(planStr, "equal join conjunct:");
                    PlanTestBase.assertContains(planStr, "from_binary");
                    // Verify MV scan exists (scan existing aggregate state)
                    PlanTestBase.assertContains(planStr, "TABLE: test_mv1");
                    // Verify state_union is in the plan
                    PlanTestBase.assertContains(planStr, "state_union");
                },
                plan -> {
                    String planStr = plan.getExplainString(TExplainLevel.NORMAL);
                    // Verify delta version advanced
                    PlanTestBase.assertContains(planStr,
                            "TABLE VERSION: Delta@[1,2]");
                    // Verify state_union still present on incremental refresh
                    PlanTestBase.assertContains(planStr, "state_union");
                    // Verify MV scan still present
                    PlanTestBase.assertContains(planStr, "TABLE: test_mv1");
                }
        );
    }

    /**
     * End-to-end refresh of a non-aggregate incremental MV.
     *
     * <p>Non-aggregate incremental MVs are PK tables with an AUTO_INCREMENT
     * {@code __ROW_ID__}. Refresh must run successfully through multiple runs
     * (no INSERT positional-column mismatch from the AUTO_INCREMENT column) and
     * the created MV must have {@link KeysType#PRIMARY_KEYS}.
     */
    @Test
    public void testIVMWithScanNonAggregateIsPkTable() throws Exception {
        doTestWith3Runs(
                "SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` as a;",
                plan -> {
                    // First refresh: verify the MV is a PK table with AUTO_INCREMENT __ROW_ID__.
                    MaterializedView mv = getMv("test_mv1");
                    Assertions.assertEquals(KeysType.PRIMARY_KEYS, mv.getKeysType(),
                            "non-aggregate incremental MV must be a PRIMARY_KEYS table");
                    Assertions.assertNotNull(mv.getColumn("__ROW_ID__"),
                            "__ROW_ID__ column must exist on the non-aggregate PK MV");
                    Assertions.assertTrue(mv.getColumn("__ROW_ID__").isAutoIncrement(),
                            "__ROW_ID__ must be AUTO_INCREMENT for a non-aggregate incremental MV");
                },
                plan -> {
                    // Second refresh (delta[1,2]): refresh must still complete successfully.
                    MaterializedView mv = getMv("test_mv1");
                    Assertions.assertEquals(KeysType.PRIMARY_KEYS, mv.getKeysType());
                }
        );
    }

    @Test
    public void testIVMAggregatePlanWithMultiGroupByKeys() throws Exception {
        doTestWith3RunsNoCheckRewrite("SELECT date, id, sum(id), count(data) " +
                        "FROM `iceberg0`.`partitioned_db`.`t1` as a group by date, id;",
                plan -> {
                    String planStr = plan.getExplainString(TExplainLevel.NORMAL);
                    PlanTestBase.assertContains(planStr, "TABLE VERSION: Delta@[MIN,1]");
                    PlanTestBase.assertContains(planStr, "HASH JOIN");
                    PlanTestBase.assertContains(planStr, "state_union");
                    PlanTestBase.assertContains(planStr, "TABLE: test_mv1");
                },
                plan -> {
                    String planStr = plan.getExplainString(TExplainLevel.NORMAL);
                    PlanTestBase.assertContains(planStr, "TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(planStr, "state_union");
                }
        );
    }

    /**
     * Verify that after IVM→PCT fallback's first batch, the pinning owner is installed on
     * AsyncRefreshContext and the runtime pinnedTvrMap is hydrated. This is the precondition
     * for subsequent batches to recognise themselves as pinned and consume pinned state at
     * scan / partition enumeration / partition-info persistence.
     */
    @Test
    public void testPinnedTvrMapHydratedOnFallbackFirstBatch() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto",
                "`date`", Map.of("partition_refresh_number", "1"));

        advanceTableVersionTo(2);
        mockListTableDeltaTraits();

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mv.getDbId());
        TaskRun taskRun = withMVRefreshTaskRun(db.getFullName(), mv);
        MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(taskRun);
        Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);

        // First batch completed — inspect the runtime pinnedTvrMap hydrated by
        // setupPinnedContextIfNeeded after the afterSyncHook installed the owner.
        MvTaskRunContext mvTaskRunContext = mvTaskRunProcessor.getMvTaskRunContext();
        Map<String, TvrVersionRange> pinnedMap = mvTaskRunContext.getRefreshRuntimeState().getPinnedTvrMap();
        Assertions.assertFalse(pinnedMap.isEmpty(),
                "pinnedTvrMap should be populated after fallback first batch hydrates pinned state");
        // Value should be a TvrTableSnapshot pointing at the PCT-synced snapshot (version 2).
        TvrVersionRange pinned = pinnedMap.values().iterator().next();
        Assertions.assertTrue(pinned instanceof TvrTableSnapshot, "pinned range should be a TvrTableSnapshot");
        Assertions.assertEquals(2L, pinned.to().getVersion(),
                "pinned snapshot should match the PCT-synced version (2)");
    }

    /**
     * Verify that when a pinned fallback job generates the next batch task run, the next task
     * run's properties carry PINNED_REFRESH_JOB_ID set to the pinning job's START_TASK_RUN_ID.
     * This is what makes TaskRunManager treat different pinned jobs as non-mergeable and what
     * triggers pinned-mode behaviour on batch 2+ entry.
     */
    @Test
    public void testPinnedRefreshJobIdPropagatedToSubsequentBatch() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto",
                "`date`", Map.of("partition_refresh_number", "1"));

        advanceTableVersionTo(2);
        mockListTableDeltaTraits();

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mv.getDbId());
        TaskRun taskRun = withMVRefreshTaskRun(db.getFullName(), mv);
        MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(taskRun);
        MVHybridRefreshProcessor hybrid =
                (MVHybridRefreshProcessor) mvTaskRunProcessor.getMVRefreshProcessor();
        Assertions.assertTrue(hybrid.getCurrentProcessor() instanceof MVPCTRefreshProcessor);
        MVPCTRefreshProcessor pctProcessor =
                (MVPCTRefreshProcessor) hybrid.getCurrentProcessor();

        // First batch should have generated a next batch (partition_refresh_number=1 on 4 partitions).
        TaskRun nextTaskRun = pctProcessor.getNextTaskRun();
        Assertions.assertNotNull(nextTaskRun, "Fallback first batch should generate a next batch task run");

        // The next batch must carry PINNED_REFRESH_JOB_ID so:
        //  (a) TaskRunManager treats it as non-mergeable with other jobs' batches (MV_COMPARABLE);
        //  (b) on its own getProcessExecPlan, the early-SKIP / isPinnedMode paths resolve correctly.
        String pinnedJobId = nextTaskRun.getProperties().get(TaskRun.PINNED_REFRESH_JOB_ID);
        Assertions.assertNotNull(pinnedJobId,
                "Subsequent pinned batch must carry PINNED_REFRESH_JOB_ID");

        // The value must equal the current job's START_TASK_RUN_ID so the owner-match guard in
        // updateVersionMeta (and the early-SKIP in getProcessExecPlan) will accept the batch.
        MvTaskRunContext mvTaskRunContext = mvTaskRunProcessor.getMvTaskRunContext();
        String expectedJobId = mvTaskRunContext.getStatus().getStartTaskRunId();
        Assertions.assertEquals(expectedJobId, pinnedJobId,
                "PINNED_REFRESH_JOB_ID should equal the pinning job's START_TASK_RUN_ID");

        // Also verify the persisted pinning owner matches — this is what isPinnedMode() compares.
        MaterializedView refreshedMv = getMv("test_mv1");
        String persistedOwner = refreshedMv.getRefreshScheme()
                .getAsyncRefreshContext()
                .getTempTvrOwnerStartTaskRunId();
        Assertions.assertEquals(expectedJobId, persistedOwner,
                "Persisted pinning owner should match the job id carried on the next batch");
    }

    /**
     * Verify that a stale subsequent batch whose pinning owner has been overwritten by a newer
     * refresh job returns SKIPPED early, does not run syncAndCheckPCTPartitions, and does not
     * clobber the newer job's pending state. This exercises the early-SKIP path in
     * MVPCTBasedRefreshProcessor.getProcessExecPlan (Commit 1) together with the owner-match
     * guard on clearTempBaseTableInfoTvrDeltaState (Commit 3).
     */
    @Test
    public void testStalePinnedBatchDoesNotCorruptNewerJobState() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto",
                "`date`", Map.of("partition_refresh_number", "1"));

        advanceTableVersionTo(2);
        mockListTableDeltaTraits();

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mv.getDbId());

        // Job A: first batch installs owner=A_jobId and creates nextTaskRun for batch 2.
        TaskRun jobARun1 = withMVRefreshTaskRun(db.getFullName(), mv);
        MVTaskRunProcessor jobARun1Processor = getMVTaskRunProcessor(jobARun1);
        MVHybridRefreshProcessor jobAHybrid =
                (MVHybridRefreshProcessor) jobARun1Processor.getMVRefreshProcessor();
        MVPCTRefreshProcessor jobAPct =
                (MVPCTRefreshProcessor) jobAHybrid.getCurrentProcessor();
        TaskRun jobABatch2 = jobAPct.getNextTaskRun();
        Assertions.assertNotNull(jobABatch2, "Job A should generate a batch 2");
        String jobAId = jobARun1Processor.getMvTaskRunContext().getStatus().getStartTaskRunId();

        // Simulate job B arriving: overwrite owner to a different value (a different random UUID)
        // directly on AsyncRefreshContext. This mimics what a newer job's fallback first batch would do.
        MaterializedView currentMv = getMv("test_mv1");
        MaterializedView.AsyncRefreshContext asyncCtx = currentMv.getRefreshScheme().getAsyncRefreshContext();
        Map<BaseTableInfo, TvrVersionRange> jobBFrozen = Map.copyOf(asyncCtx.getTempBaseTableInfoTvrDeltaMap());
        String jobBId = com.starrocks.common.util.UUIDUtil.genUUID().toString();
        asyncCtx.replaceTempBaseTableInfoTvrDeltaMap(jobBId, jobBFrozen);
        Assertions.assertEquals(jobBId, asyncCtx.getTempTvrOwnerStartTaskRunId(),
                "Setup: newer job B should have overwritten the pinning owner");

        // Job A's stale batch 2 now runs. It must detect owner mismatch and return SKIPPED,
        // NOT refresh external tables, NOT build a plan, and NOT clear the pending state.
        initAndExecuteTaskRun(jobABatch2);

        // After the stale batch: owner must still be job B (not cleared by stale batch),
        // and the pending map must be intact. Job B's pending state is protected.
        MaterializedView afterStaleMv = getMv("test_mv1");
        MaterializedView.AsyncRefreshContext ctxAfter =
                afterStaleMv.getRefreshScheme().getAsyncRefreshContext();
        Assertions.assertEquals(jobBId, ctxAfter.getTempTvrOwnerStartTaskRunId(),
                "Stale batch must not overwrite or clear the newer job's pinning owner");
        Assertions.assertFalse(ctxAfter.getTempBaseTableInfoTvrDeltaMap().isEmpty(),
                "Stale batch must not clear the newer job's pending TVR delta map");
        // Sanity: the owner is not the stale batch's job id.
        Assertions.assertNotEquals(jobAId, ctxAfter.getTempTvrOwnerStartTaskRunId(),
                "Pinning owner should no longer be job A after job B overwrote it");
    }

    /**
     * Verify Bug B core invariant: a subsequent pinned batch's scan plan pins to the frozen
     * snapshot (S2) and NOT to a live snapshot (S3) that the catalog has advanced to between
     * batches. This exercises the full pipeline from {@code afterSyncHook} (freeze) through
     * {@code setupPinnedContextIfNeeded} (hydrate) through
     * {@code MVPCTRefreshPlanBuilder.injectPinnedTvrForIcebergRelations} (scan-plan injection)
     * down to {@code IcebergScanNode}'s {@code useSnapshot(snapshotId)} call.
     * <p>
     * Without scan-plan injection, batch 2 would pick up the live S3 via
     * {@code TableRelation.getTable().getNativeTable().currentSnapshot()} and scan S3 data while
     * the MV's partition-info / TVR record S2 — the exact divergence Bug B describes. The final
     * TVR checkpoint would still be correct (S2), so existing multi-batch tests do not catch
     * this regression.
     */
    @Test
    public void testSubsequentBatchScanPlanUsesFrozenSnapshotNotLive() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto",
                "`date`", Map.of("partition_refresh_number", "1"));

        advanceTableVersionTo(2);
        mockListTableDeltaTraits();

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mv.getDbId());

        // Batch 1: IVM fails -> Hybrid fallback to PCT -> afterSyncHook freezes at S2.
        TaskRun batch1 = withMVRefreshTaskRun(db.getFullName(), mv);
        MVTaskRunProcessor batch1Proc = getMVTaskRunProcessor(batch1);
        MVHybridRefreshProcessor hybrid =
                (MVHybridRefreshProcessor) batch1Proc.getMVRefreshProcessor();
        MVPCTRefreshProcessor pctProc =
                (MVPCTRefreshProcessor) hybrid.getCurrentProcessor();
        TaskRun batch2 = pctProc.getNextTaskRun();
        Assertions.assertNotNull(batch2, "Multi-batch fallback should generate batch 2");

        // Simulate a background catalog refresh advancing the live Iceberg snapshot to S3 BEFORE
        // batch 2 builds its plan. Batch 2 must still pin scan to S2 via pinnedTvrMap, not read
        // the advanced current snapshot from TableRelation.getTable().
        advanceTableVersionTo(3);

        // Execute batch 2 and capture its exec plan.
        initAndExecuteTaskRun(batch2);
        MVTaskRunProcessor batch2Proc = getMVTaskRunProcessor(batch2);
        MvTaskRunContext batch2Ctx = batch2Proc.getMvTaskRunContext();
        ExecPlan batch2Plan = batch2Ctx.getExecPlan();
        Assertions.assertNotNull(batch2Plan,
                "Batch 2 should have built an exec plan in pinned PCT mode");

        String planStr = batch2Plan.getExplainString(TExplainLevel.NORMAL);

        // The scan operator must pin to version 2 via TvrTableSnapshot (set by
        // injectPinnedTvrForIcebergRelations). TvrTableSnapshot's toString renders as
        // "Snapshot@(<version>)"; MIN -> "MIN", concrete version -> "<n>".
        PlanTestBase.assertContains(planStr, "TABLE VERSION:");
        Assertions.assertTrue(planStr.contains("Snapshot@(2)"),
                "Batch 2's scan must pin to S2 via TvrTableSnapshot(2); plan was:\n" + planStr);
        Assertions.assertFalse(planStr.contains("Snapshot@(3)"),
                "Batch 2 must not scan live S3 after background catalog advance; plan was:\n" + planStr);
    }

    /**
     * Verify the dual of merge isolation: pure PCT multi-batch runs (refresh_mode="pct", not
     * going through Hybrid) must NOT carry PINNED_REFRESH_JOB_ID on the next-batch task run.
     * Only pinned batches should be marked non-mergeable across jobs; pure PCT batches must
     * preserve their original merge semantics so high-frequency PCT MVs do not suffer queue
     * dedup regressions.
     */
    @Test
    public void testPurePCTBatchDoesNotCarryPinnedRefreshJobId() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "pct",
                "`date`", Map.of("partition_refresh_number", "1"));
        Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());

        advanceTableVersionTo(2);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mv.getDbId());
        TaskRun batch1 = withMVRefreshTaskRun(db.getFullName(), mv);
        MVTaskRunProcessor batch1Proc = getMVTaskRunProcessor(batch1);

        // refresh_mode=pct routes through MVPCTBasedRefreshProcessor directly, not Hybrid.
        // Therefore no afterSyncHook is installed, no owner is written, and isPinnedMode()
        // returns false on every batch of this job.
        Assertions.assertTrue(batch1Proc.getMVRefreshProcessor() instanceof MVPCTRefreshProcessor,
                "refresh_mode=pct must use MVPCTBasedRefreshProcessor directly (not Hybrid)");
        MVPCTRefreshProcessor pctProc =
                (MVPCTRefreshProcessor) batch1Proc.getMVRefreshProcessor();

        TaskRun batch2 = pctProc.getNextTaskRun();
        Assertions.assertNotNull(batch2,
                "Pure PCT multi-batch must still generate a next batch task run");

        // The property must be absent; its presence would signal pinned-mode semantics to
        // TaskRunManager (non-mergeable with batches of other jobs on the same range) and to
        // the next batch's getProcessExecPlan (pinned consumer paths). Both would mis-behave
        // for a pure PCT MV.
        Assertions.assertNull(batch2.getProperties().get(TaskRun.PINNED_REFRESH_JOB_ID),
                "Pure PCT batches must not carry PINNED_REFRESH_JOB_ID; " +
                        "setting it would break merge behaviour for all pure-PCT MVs");

        // Also verify nothing installed itself as the pinning owner during pure PCT.
        MaterializedView refreshedMv = getMv("test_mv1");
        MaterializedView.AsyncRefreshContext ctx = refreshedMv.getRefreshScheme().getAsyncRefreshContext();
        Assertions.assertNull(ctx.getTempTvrOwnerStartTaskRunId(),
                "Pure PCT must not install a pinning owner on AsyncRefreshContext");
        Assertions.assertTrue(ctx.getTempBaseTableInfoTvrDeltaMap().isEmpty(),
                "Pure PCT must not populate tempBaseTableInfoTvrDeltaMap");
    }

    /**
     * Regression for isStalePinnedBatch correctness: an unrelated batch whose task run does NOT
     * carry PINNED_REFRESH_JOB_ID must not be early-SKIPped just because some prior pinning
     * owner was left behind (e.g. by a failed/aborted pinning job). Without the
     * PINNED_REFRESH_JOB_ID gate, any partial-range refresh whose START_TASK_RUN_ID differed
     * from the orphaned owner would be falsely SKIPPED and its requested partitions would
     * never refresh.
     */
    @Test
    public void testUnrelatedPCTBatchNotBlockedByOrphanedPinningOwner() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "pct",
                "`date`", Map.of("partition_refresh_number", "1"));
        Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());

        advanceTableVersionTo(2);

        // Simulate leftover state from a hypothetical previous pinning job that aborted before
        // clearing AsyncRefreshContext — owner is set to a UUID that no current or subsequent
        // task run will match.
        MaterializedView.AsyncRefreshContext asyncCtx = mv.getRefreshScheme().getAsyncRefreshContext();
        String orphanedOwner = "orphaned-" + com.starrocks.common.util.UUIDUtil.genUUID();
        asyncCtx.replaceTempBaseTableInfoTvrDeltaMap(orphanedOwner,
                Map.of(mv.getBaseTableInfos().get(0), TvrTableSnapshot.of(99L)));

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mv.getDbId());
        TaskRun batch1 = withMVRefreshTaskRun(db.getFullName(), mv);
        MVTaskRunProcessor batch1Proc = getMVTaskRunProcessor(batch1);
        MVPCTRefreshProcessor pctProc =
                (MVPCTRefreshProcessor) batch1Proc.getMVRefreshProcessor();
        TaskRun batch2 = pctProc.getNextTaskRun();
        Assertions.assertNotNull(batch2, "Pure PCT multi-batch must generate a next batch");
        // Pure PCT never carries PINNED_REFRESH_JOB_ID, which is exactly what signals the
        // isStalePinnedBatch check to skip this batch.
        Assertions.assertNull(batch2.getProperties().get(TaskRun.PINNED_REFRESH_JOB_ID),
                "Pure PCT batch must not carry PINNED_REFRESH_JOB_ID");

        // Batch 2 has isCompleteRefresh=false (PARTITION_START/END set) and an orphaned owner
        // exists on AsyncRefreshContext. Before the fix, isStalePinnedBatch would detect
        // owner != startTaskRunId and return SKIPPED. After the fix, the PINNED_REFRESH_JOB_ID
        // gate makes this a no-op for non-pinned runs.
        initAndExecuteTaskRun(batch2);
        MVTaskRunProcessor batch2Proc = getMVTaskRunProcessor(batch2);
        ExecPlan batch2Plan = batch2Proc.getMvTaskRunContext().getExecPlan();
        Assertions.assertNotNull(batch2Plan,
                "Unrelated pure PCT batch must not be early-SKIPped by orphaned pinning owner " +
                        "state — an exec plan should have been built");
    }

    /**
     * Regression for IVM commit owner-match guard: if the pinning owner is overwritten between
     * {@code beforeCommitted} and {@code afterCommitted} (e.g. leader fail-over + schedule of a
     * different job), {@code afterCommitted} must NOT call {@code clearTempBaseTableInfoTvrDeltaState}.
     * Otherwise the newer job's pending TVR state would be wiped and its subsequent batches
     * would promote incorrect / empty TVR.
     */
    @Test
    public void testIVMCallbackDoesNotClearNewerJobOwner() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0`;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "incremental");

        // Manually stage "IVM has written its delta but transaction commit has not yet fired".
        MaterializedView.AsyncRefreshContext asyncCtx = mv.getRefreshScheme().getAsyncRefreshContext();
        String ownerA = "jobA-" + com.starrocks.common.util.UUIDUtil.genUUID();
        Map<BaseTableInfo, TvrVersionRange> jobAFrozen =
                Map.of(mv.getBaseTableInfos().get(0), TvrTableSnapshot.of(2L));
        asyncCtx.replaceTempBaseTableInfoTvrDeltaMap(ownerA, jobAFrozen);

        // Build a callback + capture owner A via beforeCommitted.
        IVMInsertLoadTxnCallback callback =
                new IVMInsertLoadTxnCallback(mv.getMvId().getDbId(), mv.getId());
        callback.beforeCommitted(null);

        // Simulate a newer job taking over ownership between beforeCommitted and afterCommitted.
        String ownerB = "jobB-" + com.starrocks.common.util.UUIDUtil.genUUID();
        asyncCtx.replaceTempBaseTableInfoTvrDeltaMap(ownerB,
                Map.copyOf(asyncCtx.getTempBaseTableInfoTvrDeltaMap()));

        // afterCommitted must detect the owner change and skip clearTempBaseTableInfoTvrDeltaState
        // so the newer job's pending state survives.
        callback.afterCommitted(null);

        MaterializedView afterMv = getMv("test_mv1");
        MaterializedView.AsyncRefreshContext ctxAfter = afterMv.getRefreshScheme().getAsyncRefreshContext();
        Assertions.assertEquals(ownerB, ctxAfter.getTempTvrOwnerStartTaskRunId(),
                "Newer job's pinning owner must not be cleared by the stale IVM callback");
        Assertions.assertFalse(ctxAfter.getTempBaseTableInfoTvrDeltaMap().isEmpty(),
                "Newer job's pending TVR delta map must not be wiped by the stale IVM callback");
    }

    /**
     * Verify pinned snapshot id per base table is recorded on the task run's extra message so
     * it is visible via information_schema.task_runs.EXTRA_MESSAGE for post-mortem debugging.
     */
    @Test
    public void testPinnedSnapshotIdMapRecordedOnExtraMessage() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto",
                "`date`", Map.of("partition_refresh_number", "1"));

        advanceTableVersionTo(2);
        mockListTableDeltaTraits();

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mv.getDbId());
        TaskRun taskRun = withMVRefreshTaskRun(db.getFullName(), mv);
        MVTaskRunProcessor processor = getMVTaskRunProcessor(taskRun);
        Assertions.assertTrue(processor.getMVRefreshProcessor() instanceof MVHybridRefreshProcessor);

        // After fallback batch 1, setupPinnedRangesIfNeeded should have written the snapshot id
        // into the task run's MVTaskRunExtraMessage. Pure PCT / non-pinned runs leave it empty.
        MVTaskRunExtraMessage extraMessage =
                processor.getMvTaskRunContext().getStatus().getMvTaskRunExtraMessage();
        Map<String, Long> pinnedSnapshotIdMap = extraMessage.getPinnedSnapshotIdMap();
        Assertions.assertFalse(pinnedSnapshotIdMap.isEmpty(),
                "pinnedSnapshotIdMap should be populated on a pinned fallback batch");
        // Value must equal the PCT-synced snapshot (version 2).
        Long snapshotId = pinnedSnapshotIdMap.values().iterator().next();
        Assertions.assertEquals(2L, snapshotId.longValue(),
                "pinnedSnapshotIdMap value should match the PCT-synced snapshot id");
    }
}
