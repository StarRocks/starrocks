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

import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.recursivecte.RecursiveCTEAstCheck;
import com.starrocks.qe.recursivecte.RecursiveCTEExecutor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;

import java.util.List;

public class RecursiveCTETest extends PlanTestBase {
    //    @Test
    public void testSimpleRecursiveCTE() throws Exception {
        String sql = "with recursive cte as " +
                "(select v1 from t0 union all select v1 + 1 from cte where v1 < 10) " +
                "select * from cte";
        String plan = testCte(sql);
        assertContains(plan, "MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 11\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 2> : rand()");
    }

    //    @Test
    public void testSimpleRecursiveCTEExplain() throws Exception {
        String sql = "with recursive cte as " +
                "(select v1 from t0 union all select v1 + 1 from cte where v1 < 10) " +
                "select * from cte";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 11\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 2> : rand()");
    }

    public String testCte(String sql) throws Exception {
        List<StatementBase> statements;
        try (Timer ignored = Tracers.watchScope("Parser")) {
            statements = SqlParser.parse(sql, connectContext.getSessionVariable());
        }
        StatementBase statementBase = statements.get(0);

        if (RecursiveCTEAstCheck.hasRecursiveCte(statementBase)) {
            RecursiveCTEExecutor executor = new RecursiveCTEExecutor();
            StatementBase sb = executor.splitOuterStmt(statementBase, connectContext);
            executor.prepareRecursiveCTE();
            System.out.println("asd");
        }
        return null;
    }
}
