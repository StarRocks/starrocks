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

import com.starrocks.common.util.UUIDUtil;
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
        testExplain("explain update tprimary set v2 = DEFAULT where v1 = 'aaa'");
        testExplain("explain update tprimary_auto_increment set v2 = DEFAULT where v1 = '123'");
        testExplain("explain verbose update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        testExplain("explain costs update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
    }

    private void testExplain(String explainStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.getState().reset();
        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(explainStmt, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statements.get(0));
        stmtExecutor.execute();
        Assert.assertEquals(connectContext.getState().getStateType(), QueryState.MysqlStateType.EOF);
    }

    private static String getUpdateExecPlan(String originStmt) throws Exception {
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