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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.load.loadv2.IVMInsertLoadTxnCallback;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

public abstract class MVIVMTestBase extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        UtFrameUtils.mockTimelinessForAsyncMVTest(connectContext);
    }

    // refresh and get the execute plan for the materialized view
    protected ExecPlan getIVMRefreshedExecPlan(MaterializedView mv) throws Exception {
        TaskRun taskRun = buildMVTaskRun(mv, "test");
        ExecPlan execPlan = getMVRefreshExecPlan(taskRun);
        // update version map
        MvId mvId = mv.getMvId();
        IVMInsertLoadTxnCallback callback =
                new IVMInsertLoadTxnCallback(mvId.getDbId(), mv.getId());
        callback.beforeCommitted(null);
        callback.afterCommitted(null, true);
        return execPlan;
    }

    public abstract void advanceTableVersionTo(long toVersion);

    protected void doTest(String mvQuery) throws Exception {
        String ddl = String.format("CREATE MATERIALIZED VIEW `test`.`test_mv1` " +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"refresh_mode\" = \"incremental\"" +
                ")\n" +
                "AS %s;", mvQuery);
        starRocksAssert.withMaterializedView(ddl);
        MaterializedView mv = getMv("test_mv1");
        ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
        Assertions.assertTrue(execPlan != null);
    }

    protected void doTestWith3RunsNoCheckRewrite(String mvQuery,
                                                 MVIVMIcebergTestBase.ExecPlanChecker run1,
                                                 MVIVMIcebergTestBase.ExecPlanChecker run3) throws Exception {
        doTestWith3Runs(mvQuery, run1, run3, false);
    }
    protected void doTestWith3Runs(String mvQuery,
                                   MVIVMIcebergTestBase.ExecPlanChecker run1,
                                   MVIVMIcebergTestBase.ExecPlanChecker run3) throws Exception {
        doTestWith3Runs(mvQuery, run1, run3, true);
    }
    
    protected void doTestWith3Runs(String mvQuery,
                                   MVIVMIcebergTestBase.ExecPlanChecker run1,
                                   MVIVMIcebergTestBase.ExecPlanChecker run3,
                                   boolean isCheckRewrite) throws Exception {
        String ddl = String.format("CREATE MATERIALIZED VIEW `test`.`test_mv1` " +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"refresh_mode\" = \"incremental\"" +
                ")\n" +
                "AS %s;", mvQuery);
        starRocksAssert.withMaterializedView(ddl);
        MaterializedView mv = getMv("test_mv1");
        // 1th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            run1.check(execPlan);
        }
        // test mv rewrite
        {
            String plan = getFragmentPlan(mvQuery);
            if (isCheckRewrite) {
                Assertions.assertTrue(plan.contains("test_mv1"));
            }
        }
        // 2th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan == null);
        }
        advanceTableVersionTo(2);
        // 3th run
        {
            ExecPlan execPlan = getIVMRefreshedExecPlan(mv);
            Assertions.assertTrue(execPlan != null);
            run3.check(execPlan);
        }
    }
}
