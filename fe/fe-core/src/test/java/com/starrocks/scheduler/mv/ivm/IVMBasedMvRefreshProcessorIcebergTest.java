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

import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodName.class)
public class IVMBasedMvRefreshProcessorIcebergTest extends MVIVMIcebergTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
        starRocksAssert.useDatabase("test");
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
}
