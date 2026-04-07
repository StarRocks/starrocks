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
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.BaseMVRefreshProcessor;
import com.starrocks.scheduler.mv.hybrid.MVHybridBasedRefreshProcessor;
import com.starrocks.scheduler.mv.pct.MVPCTBasedRefreshProcessor;
import com.starrocks.scheduler.persist.MVTaskRunExtraMessage;
import com.starrocks.server.GlobalStateMgr;
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
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "     TABLE: unpartitioned_db.t0\n" +
                                    "     PREDICATES: 14: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 14: id > 10\n" +
                                    "     TABLE VERSION: Delta@[MIN,1]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "     TABLE: partitioned_db.t1\n" +
                                    "     PREDICATES: 17: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 17: id > 10\n" +
                                    "     TABLE VERSION: Snapshot@(1)");
                },
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "     TABLE: unpartitioned_db.t0\n" +
                                    "     PREDICATES: 8: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 8: id > 10\n" +
                                    "     TABLE VERSION: Snapshot@(1)");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "  1:IcebergScanNode\n" +
                                    "     TABLE: partitioned_db.t1\n" +
                                    "     PREDICATES: 11: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 11: id > 10\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "     TABLE: unpartitioned_db.t0\n" +
                                    "     PREDICATES: 14: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 14: id > 10\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "     TABLE: partitioned_db.t1\n" +
                                    "     PREDICATES: 17: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 17: id > 10\n" +
                                    "     TABLE VERSION: Snapshot@(2)");
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
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "     TABLE: unpartitioned_db.t0\n" +
                                    "     PREDICATES: 14: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 14: id > 10\n" +
                                    "     TABLE VERSION: Delta@[MIN,1]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "     TABLE: partitioned_db.t1\n" +
                                    "     PREDICATES: 17: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 17: id > 10\n" +
                                    "     TABLE VERSION: Snapshot@(1)");
                },
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "     TABLE: unpartitioned_db.t0\n" +
                                    "     PREDICATES: 8: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 8: id > 10\n" +
                                    "     TABLE VERSION: Snapshot@(1)");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "  1:IcebergScanNode\n" +
                                    "     TABLE: partitioned_db.t1\n" +
                                    "     PREDICATES: 11: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 11: id > 10\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "     TABLE: unpartitioned_db.t0\n" +
                                    "     PREDICATES: 14: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 14: id > 10\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.COSTS),
                            "     TABLE: partitioned_db.t1\n" +
                                    "     PREDICATES: 17: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 17: id > 10\n" +
                                    "     TABLE VERSION: Snapshot@(2)");
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
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "HASH JOIN\n" +
                                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                                    "  |  colocate: false, reason: \n" +
                                    "  |  equal join conjunct: 28: from_binary = 23: __ROW_ID__");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  16:OlapScanNode\n" +
                                    "     TABLE: test_mv1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/1");
                },
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "     TABLE: unpartitioned_db.t0\n" +
                                    "     PREDICATES: 17: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 17: id > 10\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "     TABLE: unpartitioned_db.t0\n" +
                                    "     PREDICATES: 11: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 11: id > 10\n" +
                                    "     TABLE VERSION: Snapshot@(1)");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "HASH JOIN\n" +
                                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                                    "  |  colocate: false, reason: \n" +
                                    "  |  equal join conjunct: 28: from_binary = 23: __ROW_ID__");
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
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "HASH JOIN\n" +
                                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                                    "  |  colocate: false, reason: \n" +
                                    "  |  equal join conjunct: 82: from_binary = 44: __ROW_ID__");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "  16:OlapScanNode\n" +
                                    "     TABLE: test_mv1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/1");
                },
                plan -> {
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "     TABLE: unpartitioned_db.t0\n" +
                                    "     PREDICATES: 17: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 17: id > 10\n" +
                                    "     TABLE VERSION: Delta@[1,2]");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "     TABLE: unpartitioned_db.t0\n" +
                                    "     PREDICATES: 11: id > 10\n" +
                                    "     MIN/MAX PREDICATES: 11: id > 10\n" +
                                    "     TABLE VERSION: Snapshot@(1)");
                    PlanTestBase.assertContains(plan.getExplainString(TExplainLevel.NORMAL),
                            "HASH JOIN\n" +
                                    "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                                    "  |  colocate: false, reason: \n" +
                                    "  |  equal join conjunct: 82: from_binary = 44: __ROW_ID__");
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

        // if the base table has no retractable changes, the refresh processor should be full refresh
        {
            advanceTableVersionTo(2);
            MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(mv);
            Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVHybridBasedRefreshProcessor);
            MVHybridBasedRefreshProcessor hybridBasedRefreshProcessor =
                    (MVHybridBasedRefreshProcessor) mvTaskRunProcessor.getMVRefreshProcessor();
            Assertions.assertTrue(hybridBasedRefreshProcessor.getCurrentProcessor() instanceof MVIVMBasedRefreshProcessor);
        }
        // if the base table has retractable changes, the refresh processor should be full refresh
        {
            mockListTableDeltaTraits();
            MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(mv);
            Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVHybridBasedRefreshProcessor);
            MVHybridBasedRefreshProcessor hybridBasedRefreshProcessor =
                    (MVHybridBasedRefreshProcessor) mvTaskRunProcessor.getMVRefreshProcessor();
            Assertions.assertTrue(hybridBasedRefreshProcessor.getCurrentProcessor() instanceof MVPCTBasedRefreshProcessor);
        }
    }

    @Test
    public void testAutoRefreshFallbackCanPersistCheckpointForNextIvm() throws Exception {
        String query = "SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0` as a;";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto");
        Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());

        advanceTableVersionTo(2);
        mockListTableDeltaTraits();

        MVTaskRunProcessor run1 = getMVTaskRunProcessor(mv);
        Assertions.assertTrue(run1.getMVRefreshProcessor() instanceof MVHybridBasedRefreshProcessor);
        MVHybridBasedRefreshProcessor hybrid1 =
                (MVHybridBasedRefreshProcessor) run1.getMVRefreshProcessor();
        Assertions.assertTrue(hybrid1.getCurrentProcessor() instanceof MVPCTBasedRefreshProcessor);

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
        Assertions.assertTrue(run2.getMVRefreshProcessor() instanceof MVHybridBasedRefreshProcessor);
        MVHybridBasedRefreshProcessor hybrid2 =
                (MVHybridBasedRefreshProcessor) run2.getMVRefreshProcessor();
        Assertions.assertTrue(hybrid2.getCurrentProcessor() instanceof MVIVMBasedRefreshProcessor);
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
            Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVPCTBasedRefreshProcessor);
        }
        // if the base table has retractable changes, the refresh processor should be full refresh
        {
            mockListTableDeltaTraits();
            MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(mv);
            Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVPCTBasedRefreshProcessor);
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
        Assertions.assertTrue(run1.getMVRefreshProcessor() instanceof MVHybridBasedRefreshProcessor);
        MVHybridBasedRefreshProcessor hybrid1 =
                (MVHybridBasedRefreshProcessor) run1.getMVRefreshProcessor();
        // Verify fallback to PCT
        Assertions.assertTrue(hybrid1.getCurrentProcessor() instanceof MVPCTBasedRefreshProcessor);

        // Verify canGenerateNextTaskRun is NOT blocked (the core fix)
        MVPCTBasedRefreshProcessor pctProcessor =
                (MVPCTBasedRefreshProcessor) hybrid1.getCurrentProcessor();
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
        Assertions.assertTrue(run2.getMVRefreshProcessor() instanceof MVHybridBasedRefreshProcessor);
        MVHybridBasedRefreshProcessor hybrid2 =
                (MVHybridBasedRefreshProcessor) run2.getMVRefreshProcessor();
        // Should switch back to IVM since delta[2,3] is append-only
        Assertions.assertTrue(hybrid2.getCurrentProcessor() instanceof MVIVMBasedRefreshProcessor,
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
        Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVHybridBasedRefreshProcessor);
        MVHybridBasedRefreshProcessor hybrid =
                (MVHybridBasedRefreshProcessor) mvTaskRunProcessor.getMVRefreshProcessor();
        // Verify fallback to PCT
        Assertions.assertTrue(hybrid.getCurrentProcessor() instanceof MVPCTBasedRefreshProcessor);

        // Verify canGenerateNextTaskRun is allowed
        MVPCTBasedRefreshProcessor pctProcessor =
                (MVPCTBasedRefreshProcessor) hybrid.getCurrentProcessor();
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

            // Execute remaining batches
            while (nextTaskRun != null) {
                initAndExecuteTaskRun(nextTaskRun);
                MVTaskRunProcessor nextProcessor = getMVTaskRunProcessor(nextTaskRun);
                BaseMVRefreshProcessor refreshProcessor = nextProcessor.getMVRefreshProcessor();
                if (refreshProcessor instanceof MVHybridBasedRefreshProcessor) {
                    MVHybridBasedRefreshProcessor nextHybrid =
                            (MVHybridBasedRefreshProcessor) refreshProcessor;
                    nextTaskRun = nextHybrid.getCurrentProcessor().getNextTaskRun();
                } else if (refreshProcessor instanceof MVPCTBasedRefreshProcessor) {
                    nextTaskRun = ((MVPCTBasedRefreshProcessor) refreshProcessor).getNextTaskRun();
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
    }

    /**
     * Test that IVM refresh records complete PCT metadata without batch truncation.
     */
    @Test
    public void testIVMPCTMetadataNotTruncatedByPartitionRefreshNumber() throws Exception {
        // Create a PARTITIONED MV on partitioned Iceberg table t1 (4 partitions: date=2020-01-01..04).
        // partition_refresh_number=1 would truncate to 1 partition in the old code path.
        String query = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";
        MaterializedView mv = createMaterializedViewWithRefreshMode(query, "auto",
                "`date`", Map.of("partition_refresh_number", "1"));
        Assertions.assertEquals(MaterializedView.RefreshMode.AUTO, mv.getCurrentRefreshMode());

        // Initial refresh: all 4 partitions detected as new
        {
            advanceTableVersionTo(2);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mv.getDbId());
            TaskRun taskRun = withMVRefreshTaskRun(db.getFullName(), mv);
            MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor(taskRun);

            // Verify IVM path was used (auto mode -> hybrid -> IVM)
            Assertions.assertTrue(mvTaskRunProcessor.getMVRefreshProcessor() instanceof MVHybridBasedRefreshProcessor);
            MVHybridBasedRefreshProcessor hybridProcessor =
                    (MVHybridBasedRefreshProcessor) mvTaskRunProcessor.getMVRefreshProcessor();
            Assertions.assertTrue(hybridProcessor.getCurrentProcessor() instanceof MVIVMBasedRefreshProcessor);

            // Verify PCT metadata records ALL partitions, not truncated to 1.
            // Before the fix, partition_refresh_number=1 would truncate mvPartitionsToRefresh to 1 partition.
            // After the fix (skipBatchFilter=true), all detected partitions should be recorded.
            MvTaskRunContext mvContext = mvTaskRunProcessor.getMvTaskRunContext();
            MVTaskRunExtraMessage extraMessage = mvContext.getStatus().getMvTaskRunExtraMessage();
            Set<String> mvPartitionsToRefresh = extraMessage.getMvPartitionsToRefresh();
            Assertions.assertNotNull(mvPartitionsToRefresh,
                    "mvPartitionsToRefresh should not be null");
            // t1 has 4 partitions -> MV should have 4 corresponding partitions, all recorded
            Assertions.assertTrue(mvPartitionsToRefresh.size() > 1,
                    "IVM should record all detected partitions in PCT metadata, " +
                            "not truncated by partition_refresh_number. " +
                            "Expected >1 but got: " + mvPartitionsToRefresh);
        }
    }
}
