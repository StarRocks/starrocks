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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.TableName;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SamplingPredicateGate}.
 *
 * <p>Predicates are obtained by parsing {@code SELECT * FROM s WHERE <pred>} and extracting
 * the WHERE clause — no full analysis needed, just the parse-tree shape.
 */
public class SamplingPredicateGateTest {

    /** Normalized source: catalog=cat, db=db1, table=s (no alias). */
    private static final TableName SOURCE = new TableName("cat", "db1", "s");

    /**
     * Parse {@code SELECT * FROM s WHERE <pred>} and return the raw WHERE Expr.
     *
     * <p>A {@code ?} placeholder makes the parser wrap the SELECT in a {@link PrepareStmt}; unwrap
     * it so parameter predicates can be reached the same way.
     */
    private static Expr whereOf(String pred) {
        String sql = "SELECT * FROM s WHERE " + pred;
        StatementBase parsed = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        if (parsed instanceof PrepareStmt prepareStmt) {
            parsed = prepareStmt.getInnerStmt();
        }
        QueryStatement stmt = (QueryStatement) parsed;
        return ((SelectRelation) stmt.getQueryRelation()).getWhereClause();
    }

    private static boolean safe(Expr e) {
        return SamplingPredicateGate.isDeterministicAndSafe(e, SOURCE, null);
    }

    private static boolean safe(Expr e, String alias) {
        return SamplingPredicateGate.isDeterministicAndSafe(e, SOURCE, alias);
    }

    // --- null predicate ---

    @Test
    public void nullPredicateIsSafe() {
        Assertions.assertTrue(SamplingPredicateGate.isDeterministicAndSafe(null, SOURCE, null));
    }

    // --- safe comparisons ---

    @Test
    public void plainComparisonIsSafe() {
        // a > 10 AND b = 'x'
        Assertions.assertTrue(safe(whereOf("a > 10 AND b = 'x'")));
    }

    // --- non-deterministic functions: still rejected (covered by the broad FunctionCallExpr rule) ---

    @Test
    public void randRejected() {
        Assertions.assertFalse(safe(whereOf("a > rand()")));
    }

    @Test
    public void nowRejected() {
        // now() is a FunctionCallExpr; rejected regardless of determinism.
        Assertions.assertFalse(safe(whereOf("dt > now()")));
    }

    @Test
    public void currentTimestampRejected() {
        Assertions.assertFalse(safe(whereOf("dt > current_timestamp()")));
    }

    @Test
    public void uuidRejected() {
        Assertions.assertFalse(safe(whereOf("a = uuid()")));
    }

    // --- deterministic but session-sensitive functions: also rejected ---

    @Test
    public void deterministicFunctionRejected() {
        // abs() is fully deterministic, but the broad rule rejects every
        // FunctionCallExpr: any function can be session-timezone-sensitive
        // (e.g. from_unixtime, convert_tz), so no per-function exception is made.
        Assertions.assertFalse(safe(whereOf("abs(a) > 10")));
    }

    @Test
    public void fromUnixTimeRejected() {
        // from_unixtime uses the session time zone; the ROOT sampling context
        // may have a different time zone, producing a different row set.
        Assertions.assertFalse(safe(whereOf("from_unixtime(ts) > '2024-01-01'")));
    }

    // --- information functions (InformationFunction subtype, not FunctionCallExpr) ---

    @Test
    public void currentUserRejected() {
        Assertions.assertFalse(safe(whereOf("a = current_user()")));
    }

    @Test
    public void databaseFunctionRejected() {
        Assertions.assertFalse(safe(whereOf("a = database()")));
    }

    // --- session/user variables & prepared-statement parameters ---

    @Test
    public void sessionVariableRejected() {
        // @@session.x parsed as VariableExpr
        Assertions.assertFalse(safe(whereOf("k = @@session.query_timeout")));
    }

    @Test
    public void userVariableRejected() {
        // @x parsed as UserVariableExpr
        Assertions.assertFalse(safe(whereOf("k = @x")));
    }

    @Test
    public void parameterRejected() {
        // ? placeholder parsed as Parameter
        Assertions.assertFalse(safe(whereOf("a = ?")));
    }

    // --- subquery shapes ---

    @Test
    public void scalarSubqueryRejected() {
        Assertions.assertFalse(safe(whereOf("a > (SELECT max(x) FROM t)")));
    }

    @Test
    public void existsSubqueryRejected() {
        Assertions.assertFalse(safe(whereOf("EXISTS (SELECT 1 FROM t)")));
    }

    @Test
    public void inSubqueryRejected() {
        // IN with a subquery — isConstantValues() returns false
        Assertions.assertFalse(safe(whereOf("a IN (SELECT x FROM t)")));
    }

    // --- IN with literal list is safe ---

    @Test
    public void inLiteralListSafe() {
        Assertions.assertTrue(safe(whereOf("a IN (1, 2, 3)")));
    }

    // --- qualifier checks ---

    @Test
    public void foreignTableQualifiedColumnRejected() {
        // other.a > 1: table qualifier "other" != source "s"
        Assertions.assertFalse(safe(whereOf("other.a > 1")));
    }

    @Test
    public void foreignDbQualifiedColumnRejected() {
        // db2.s.a > 1 while source is db1.s
        Assertions.assertFalse(safe(whereOf("db2.s.a > 1")));
    }

    @Test
    public void sourceQualifiedColumnSafe() {
        // s.a > 1 — table name matches source name (no alias)
        Assertions.assertTrue(safe(whereOf("s.a > 1")));
    }

    @Test
    public void sourceQualifiedColumnWithAliasNameSafe() {
        // alias "t" in scope; t.a > 1 should be safe
        Assertions.assertTrue(safe(whereOf("t.a > 1"), "t"));
    }

    @Test
    public void foreignQualifiedColumnWithAliasRejected() {
        // alias "t" in scope; s.a > 1 should be rejected (must use alias)
        Assertions.assertFalse(safe(whereOf("s.a > 1"), "t"));
    }

    // --- toSql round-trip ---

    @Test
    public void toSqlReparseable() {
        Expr expr = whereOf("a > 10");
        String sql = SamplingPredicateGate.toSql(expr);
        Assertions.assertNotNull(sql);
        Assertions.assertFalse(sql.isEmpty());
        // round-trip: re-parse should not throw
        Expr reparsed = whereOf(sql);
        Assertions.assertNotNull(reparsed);
    }
}
