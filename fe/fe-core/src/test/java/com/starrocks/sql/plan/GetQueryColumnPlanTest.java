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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class GetQueryColumnPlanTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testQueryColumnType() throws Exception {
        String explainString = getColumnTypeExecPlan("select pk,v1,v2 from tprimary");
        Assertions.assertEquals(explainString,
                "[{\"columnName\":\"pk\",\"columnType\":\"BIGINT\"}," +
                        "{\"columnName\":\"v1\",\"columnType\":\"VARCHAR\"}," +
                        "{\"columnName\":\"v2\",\"columnType\":\"INT\"}]");
        testExplain("explain type select pk,v1,v2 from tprimary");
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

    private static String getColumnTypeExecPlan(String originStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable().getSqlMode())
                        .get(0);
        connectContext.getDumpInfo().setOriginStmt(originStmt);
        ExecPlan execPlan = new StatementPlanner().plan(statementBase, connectContext);

        String ret = execPlan.getColumnTypeExplain(connectContext.getQueryId().toString());
        return ret;
    }
}
