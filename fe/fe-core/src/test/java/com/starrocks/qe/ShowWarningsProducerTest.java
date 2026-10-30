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
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
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

    @Test
    public void testNoTableStatementsPreserveDiagnostics() throws Exception {
        ConnectContext ctx = AnalyzeTestUtil.getConnectContext();
        ctx.clearWarnings();
        ctx.addWarning(new QueryWarning("Warning", "1265", "seed from the previous load"));

        // SHOW WARNINGS itself, SET, BEGIN and COMMIT use no tables and generate no messages, so
        // the diagnostics area survives all of them (MySQL retention rule) and the load's warning
        // is still readable after the transaction is committed.
        for (String sql : new String[] {"show warnings", "set enable_profile = false", "begin", "commit"}) {
            new StmtExecutor(ctx, SqlParser.parseSingleStatement(
                    sql, ctx.getSessionVariable().getSqlMode())).execute();
            Assertions.assertFalse(ctx.getState().isError(), sql + " unexpectedly failed");
            Assertions.assertEquals(1, ctx.getWarnings().size(), sql + " must preserve the buffer");
            Assertions.assertEquals("seed from the previous load", ctx.getWarnings().get(0).getMessage());
        }

        // A failing statement of a preserving class still replaces the buffer with its own error.
        new StmtExecutor(ctx, SqlParser.parseSingleStatement(
                "set variable_that_does_not_exist = 1", ctx.getSessionVariable().getSqlMode())).execute();
        Assertions.assertTrue(ctx.getState().isError());
        Assertions.assertEquals(1, ctx.getWarnings().size());
        Assertions.assertEquals("Error", ctx.getWarnings().get(0).getLevel());
    }

    // A statement forwarded to the leader is answered with the leader's own ERR packet, relayed by
    // ConnectProcessor.finalizeCommand(). TMasterOpResult brings the message back but no error
    // code, so a diagnostic recorded on this FE would report the 1064 fallback next to the real
    // code the client just read. Nothing is recorded instead. getOutputPacket() returns non-null
    // exactly when a leader result came back, which is what this stubs.
    @Test
    public void testForwardedFailureRecordsNoDiagnostic() throws Exception {
        ConnectContext ctx = AnalyzeTestUtil.getConnectContext();
        ctx.clearWarnings();

        new MockUp<StmtExecutor>() {
            @Mock
            public ByteBuffer getOutputPacket() {
                return ByteBuffer.allocate(1);
            }
        };

        new StmtExecutor(ctx, SqlParser.parseSingleStatement(
                "select * from table_that_does_not_exist", ctx.getSessionVariable().getSqlMode())).execute();

        Assertions.assertTrue(ctx.getState().isError());
        Assertions.assertTrue(ctx.getWarnings().isEmpty());
    }

    // USE belongs to no preserving class, so it drops the previous statement's diagnostics. The
    // MySQL client sends USE as the COM_INIT_DB command instead of a statement, and
    // ConnectProcessor clears the buffer there as well, so the buffer ends up in the same state
    // whichever route the client takes.
    @Test
    public void testUseDatabaseClearsDiagnostics() throws Exception {
        ConnectContext ctx = AnalyzeTestUtil.getConnectContext();
        ctx.clearWarnings();
        ctx.addWarning(new QueryWarning("Warning", "1265", "seed from the previous load"));

        new StmtExecutor(ctx, SqlParser.parseSingleStatement(
                "use test", ctx.getSessionVariable().getSqlMode())).execute();

        Assertions.assertFalse(ctx.getState().isError());
        Assertions.assertTrue(ctx.getWarnings().isEmpty());
    }

    // SHOW WARNINGS / SHOW ERRORS read the buffer back only while they succeed. When one of them
    // fails the client receives an ERR packet, so the buffer must hold that error instead of the
    // previous statement's diagnostics, which the client would otherwise read as the outcome of a
    // statement that never produced them.
    @Test
    public void testFailingShowWarningsReplacesDiagnostics() throws Exception {
        ConnectContext ctx = AnalyzeTestUtil.getConnectContext();
        ctx.clearWarnings();
        ctx.addWarning(new QueryWarning("Warning", "1265", "seed from the previous load"));

        // An unknown column in the WHERE clause fails while ShowStmtAnalyzer resolves the slot
        // against the Level / Code / Message metadata of the statement.
        new StmtExecutor(ctx, SqlParser.parseSingleStatement(
                "show warnings where no_such_column = 'x'", ctx.getSessionVariable().getSqlMode())).execute();

        Assertions.assertTrue(ctx.getState().isError());
        Assertions.assertEquals(1, ctx.getWarnings().size());
        QueryWarning diagnostic = ctx.getWarnings().get(0);
        Assertions.assertEquals("Error", diagnostic.getLevel());
        Assertions.assertEquals(ctx.getState().getErrorMessage(), diagnostic.getMessage());

        ShowResultSet errors = ShowExecutor.execute(new ShowWarningStmt(null, true, NodePosition.ZERO), ctx);
        Assertions.assertEquals(1, errors.getResultRows().size());
        Assertions.assertEquals("Error", errors.getResultRows().get(0).get(0));
    }
}
