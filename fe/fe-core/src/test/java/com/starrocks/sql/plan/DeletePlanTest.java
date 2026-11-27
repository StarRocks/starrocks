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

package com.starrocks.sql.plan;

<<<<<<< HEAD
=======
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.jmockit.Deencapsulation;
>>>>>>> 086e6d2 ([BugFix] Do not use runtime adaptive DOP together with spill (#84447))
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DeletePlanTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testDelete() throws Exception {
        String explainString = getDeleteExecPlan("delete from tprimary where pk = 1");
        Assertions.assertTrue(explainString.contains("PREDICATES: 1: pk = 1"));

        testExplain("explain delete from tprimary where pk = 1");
        testExplain("explain verbose delete from tprimary where pk = 1");
        testExplain("explain costs delete from tprimary where pk = 1");
    }

    private void testExplain(String explainStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.getState().reset();
        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(explainStmt, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statements.get(0));
        stmtExecutor.execute();
        Assertions.assertEquals(connectContext.getState().getStateType(), QueryState.MysqlStateType.EOF);
    }

<<<<<<< HEAD
    private static String getDeleteExecPlan(String originStmt) throws Exception {
=======
    // A connector sink turns spill on for its statement; it has to happen before planning, so no fragment keeps
    // runtime adaptive DOP (which must not run together with spill), and only on a copy of the session variable.
    @Test
    public void testIcebergDeleteConnectorSinkSpillBeforePlanning() throws Exception {
        String sql = "delete from iceberg0.unpartitioned_db.t0_v2 where id in " +
                "(select id from iceberg0.partitioned_db.t1_v2 group by id)";
        SessionVariable original = connectContext.getSessionVariable();
        SessionVariable sv = (SessionVariable) original.clone();
        connectContext.setSessionVariable(sv);
        try {
            sv.setEnableRuntimeAdaptiveDop(true);
            sv.setEnableSpill(false);

            // without connector sink spill the plan does use runtime adaptive DOP
            Deencapsulation.setField(sv, "enableConnectorSinkSpill", false);
            ExecPlan execPlan = getDeleteExecPlan(sql);
            assertTrue(execPlan.getFragments().stream().anyMatch(PlanFragment::isUseRuntimeAdaptiveDop));
            assertSame(sv, connectContext.getSessionVariable());

            Deencapsulation.setField(sv, "enableConnectorSinkSpill", true);
            execPlan = getDeleteExecPlan(sql);
            assertTrue(execPlan.getFragments().stream().noneMatch(PlanFragment::isUseRuntimeAdaptiveDop));
            assertTrue(connectContext.getSessionVariable().isEnableSpill());
            // the user's session variable is left untouched
            assertFalse(sv.isEnableSpill());
        } finally {
            connectContext.setSessionVariable(original);
        }
    }

    private static ExecPlan getDeleteExecPlan(String originStmt) throws Exception {
>>>>>>> 086e6d2 ([BugFix] Do not use runtime adaptive DOP together with spill (#84447))
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable().getSqlMode())
                        .get(0);
        connectContext.getDumpInfo().setOriginStmt(originStmt);
        ExecPlan execPlan = new StatementPlanner().plan(statementBase, connectContext);

        String ret = execPlan.getExplainString(TExplainLevel.NORMAL);
        return ret;
    }
}