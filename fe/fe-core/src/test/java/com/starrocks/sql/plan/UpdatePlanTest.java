// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.plan;

import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class UpdatePlanTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testUpdate() throws Exception {
        String explainString = getUpdateExecPlan("update tprimary set v1 = 'aaa' where pk = 1");
        Assert.assertTrue(explainString.contains("PREDICATES: 1: pk = 1"));
        Assert.assertTrue(explainString.contains("<slot 4> : 'aaa'"));

        explainString = getUpdateExecPlan("update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        Assert.assertTrue(explainString.contains("v1 = 'aaa'"));
        Assert.assertTrue(explainString.contains("CAST(CAST(3: v2 AS BIGINT) + 1 AS INT)"));

        testExplain("explain update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        testExplain("explain verbose update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        testExplain("explain costs update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
    }

    private void testExplain(String explainStmt) throws Exception {
        connectContext.getState().reset();
        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(explainStmt, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statements.get(0));
        stmtExecutor.execute();
        Assert.assertEquals(connectContext.getState().getStateType(), QueryState.MysqlStateType.EOF);
    }

    private static String getUpdateExecPlan(String originStmt) throws Exception {
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