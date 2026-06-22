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

package com.starrocks.qe;

import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.ShowWarningStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

// End-to-end coverage for the warning producers that feed SHOW WARNINGS / SHOW ERRORS:
// the failing-statement path in StmtExecutor.execute() must record an Error-level diagnostic.
public class ShowWarningsProducerTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testFailedStatementPopulatesShowErrors() throws Exception {
        ConnectContext ctx = AnalyzeTestUtil.getConnectContext();

        // A statement that fails analysis (unknown table) runs through StmtExecutor.execute()'s
        // error path, which records an Error-level diagnostic into the session buffer.
        new StmtExecutor(ctx, SqlParser.parseSingleStatement(
                "select * from table_that_does_not_exist", ctx.getSessionVariable().getSqlMode())).execute();

        Assertions.assertTrue(ctx.getState().isError());
        List<QueryWarning> warnings = ctx.getWarnings();
        Assertions.assertFalse(warnings.isEmpty());
        Assertions.assertEquals("Error", warnings.get(0).getLevel());

        // SHOW ERRORS surfaces the error; SHOW WARNINGS returns it as well.
        ShowResultSet errors = ShowExecutor.execute(new ShowWarningStmt(null, true, NodePosition.ZERO), ctx);
        Assertions.assertEquals(1, errors.getResultRows().size());
        Assertions.assertEquals("Error", errors.getResultRows().get(0).get(0));

        ShowResultSet allWarnings = ShowExecutor.execute(new ShowWarningStmt(null, false, NodePosition.ZERO), ctx);
        Assertions.assertEquals(1, allWarnings.getResultRows().size());

        // A second failing statement replaces the buffer (cleared at the start of execute()),
        // so SHOW ERRORS still shows exactly one row rather than accumulating.
        new StmtExecutor(ctx, SqlParser.parseSingleStatement(
                "select * from another_missing_table", ctx.getSessionVariable().getSqlMode())).execute();
        ShowResultSet errorsAgain = ShowExecutor.execute(new ShowWarningStmt(null, true, NodePosition.ZERO), ctx);
        Assertions.assertEquals(1, errorsAgain.getResultRows().size());
    }
}
