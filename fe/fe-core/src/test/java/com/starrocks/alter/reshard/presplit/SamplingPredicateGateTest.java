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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.GlobalFunctionMgr;
import com.starrocks.catalog.SqlFunction;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    public void deterministicFunctionOutsideAllowlistRejected() {
        // md5() is deterministic, but only the vetted row-level allowlist passes: an
        // unvetted function may read session state the ROOT context does not share.
        Assertions.assertFalse(safe(whereOf("md5(b) = 'x'")));
    }

    @Test
    public void rowLevelAllowlistedFunctionSafe() {
        Assertions.assertTrue(safe(whereOf("date_trunc('day', ts) = '2026-01-01'")));
        Assertions.assertTrue(safe(whereOf("abs(a) > 10 AND upper(b) = 'X'")));
    }

    @Test
    public void dbQualifiedFunctionRejected() {
        // db1.upper(...) names a UDF, not the vetted built-in.
        Assertions.assertFalse(safe(whereOf("db1.upper(b) = 'X'")));
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

    // --- plan-time constant folding in the caller's context ---

    /** A fresh context with a pinned start time and session time zone. */
    private static ConnectContext contextAt(String isoInstant, String timeZone) {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setTimeZone(timeZone);
        ctx.setStartTime(Instant.parse(isoInstant));
        return ctx;
    }

    private static String foldedSql(String pred, ConnectContext ctx) {
        Expr folded = SamplingPredicateGate.foldPlanTimeConstants(whereOf(pred), ctx);
        return folded == null ? null : SamplingPredicateGate.toSql(folded);
    }

    @Test
    public void currentDateArithmeticFoldsToTypedLiteral() {
        ConnectContext ctx = contextAt("2026-09-23T10:00:00Z", "UTC");
        Expr folded = SamplingPredicateGate.foldPlanTimeConstants(
                whereOf("dt >= date_sub(current_date(), 7)"), ctx);
        Assertions.assertNotNull(folded);
        Assertions.assertTrue(safe(folded));
        // date_sub over a DATE yields DATETIME; the cast keeps the type the INSERT itself compares.
        Assertions.assertEquals("`dt` >= (CAST('2026-09-16 00:00:00' AS DATETIME))",
                SamplingPredicateGate.toSql(folded));
        // The sampler re-parses the rendered SQL, and it must still pass the gate there.
        Assertions.assertTrue(safe(whereOf(SamplingPredicateGate.toSql(folded))));
    }

    @Test
    public void foldUsesTheCallersTimeZone() {
        // 2026-09-23T20:00Z is already the 24th in Shanghai: the literal must be the day the
        // INSERT itself sees, not the ROOT sampling context's day.
        Assertions.assertEquals("`dt` = (CAST('2026-09-23' AS DATE))",
                foldedSql("dt = current_date()", contextAt("2026-09-23T20:00:00Z", "UTC")));
        Assertions.assertEquals("`dt` = (CAST('2026-09-24' AS DATE))",
                foldedSql("dt = current_date()", contextAt("2026-09-23T20:00:00Z", "Asia/Shanghai")));
        Assertions.assertEquals("`ts` > (CAST('1970-01-01 08:00:00' AS VARCHAR))",
                foldedSql("ts > from_unixtime(0)", contextAt("2026-09-23T20:00:00Z", "Asia/Shanghai")));
    }

    @Test
    public void nowFoldsToTheQueryStartTime() {
        // now() is fixed at plan time: the INSERT's planner folds it from the same start time.
        Assertions.assertEquals("`ts` < (CAST('2026-09-23 18:30:05' AS DATETIME))",
                foldedSql("ts < now()", contextAt("2026-09-23T10:30:05Z", "Asia/Shanghai")));
    }

    @Test
    public void foldDoesNotMutateTheParsedPredicate() {
        Expr where = whereOf("dt >= date_sub(current_date(), 7)");
        String before = SamplingPredicateGate.toSql(where);
        SamplingPredicateGate.foldPlanTimeConstants(where, contextAt("2026-09-23T10:00:00Z", "UTC"));
        Assertions.assertEquals(before, SamplingPredicateGate.toSql(where));
    }

    @Test
    public void foldKeepsColumnDependentCallsAndFoldsTheirConstantArguments() {
        Assertions.assertEquals("(date_trunc('day', `ts`)) = (CAST('2026-09-22 00:00:00' AS DATETIME))",
                foldedSql("date_trunc('day', ts) = date_sub(current_date(), 1)",
                        contextAt("2026-09-23T10:00:00Z", "UTC")));
    }

    @Test
    public void foldedInListIsConstant() {
        Expr folded = SamplingPredicateGate.foldPlanTimeConstants(
                whereOf("dt IN (current_date(), date_sub(current_date(), 1))"),
                contextAt("2026-09-23T10:00:00Z", "UTC"));
        Assertions.assertNotNull(folded);
        Assertions.assertTrue(safe(folded));
    }

    @Test
    public void perRowNonDeterministicFunctionDoesNotFold() {
        ConnectContext ctx = contextAt("2026-09-23T10:00:00Z", "UTC");
        Assertions.assertNull(SamplingPredicateGate.foldPlanTimeConstants(whereOf("rand() < 0.5"), ctx));
        Assertions.assertNull(SamplingPredicateGate.foldPlanTimeConstants(whereOf("a > rand()"), ctx));
        Assertions.assertNull(SamplingPredicateGate.foldPlanTimeConstants(whereOf("b = uuid()"), ctx));
    }

    @Test
    public void nonBooleanConstantPredicateDoesNotFold() {
        Assertions.assertNull(SamplingPredicateGate.foldPlanTimeConstants(
                whereOf("now()"), contextAt("2026-09-23T10:00:00Z", "UTC")));
    }

    @Test
    public void foldLeavesSessionDependentColumnCallsForTheGate() {
        // from_unixtime(col) is per-row and time-zone dependent: not foldable, and the gate rejects it.
        Expr folded = SamplingPredicateGate.foldPlanTimeConstants(
                whereOf("from_unixtime(ts) > '2024-01-01'"), contextAt("2026-09-23T10:00:00Z", "UTC"));
        Assertions.assertNotNull(folded);
        Assertions.assertFalse(safe(folded));
    }

    // --- computed projections ---

    private static Expr projectionOf(String expression) {
        QueryStatement stmt = (QueryStatement) SqlParser.parseSingleStatement(
                "SELECT " + expression + " FROM s", SqlModeHelper.MODE_DEFAULT);
        return ((SelectRelation) stmt.getQueryRelation()).getSelectList().getItems().get(0).getExpr();
    }

    private static String foldedProjectionSql(String expression, ConnectContext ctx) {
        Expr folded = SamplingPredicateGate.foldProjection(projectionOf(expression), ctx);
        return folded == null ? null : SamplingPredicateGate.toSql(folded);
    }

    @Test
    public void columnFreeProjectionFoldsAsAWholeInTheCallersContext() {
        // Unlike a WHERE clause, a projection need not be boolean: current_date() AS dt is the value
        // the INSERT writes, and it must be the user's day, not the ROOT sampling context's.
        Assertions.assertEquals("CAST('2026-09-24' AS DATE)",
                foldedProjectionSql("current_date()", contextAt("2026-09-23T20:00:00Z", "Asia/Shanghai")));
        Assertions.assertEquals("CAST('2026-09-17' AS DATE)",
                foldedProjectionSql("CAST('2026-09-17' AS DATE)", contextAt("2026-09-23T20:00:00Z", "UTC")));
    }

    @Test
    public void projectionFoldingToNullIsRejected() {
        Assertions.assertNull(foldedProjectionSql("CAST(NULL AS DATE)", contextAt("2026-09-23T10:00:00Z", "UTC")));
    }

    @Test
    public void columnProjectionKeepsItsColumnsAndFoldsItsConstantCalls() {
        ConnectContext ctx = contextAt("2026-09-23T10:00:00Z", "UTC");
        Assertions.assertEquals("date_trunc('day', `ts`)", foldedProjectionSql("date_trunc('day', ts)", ctx));
        String folded = foldedProjectionSql("if(ts >= date_sub(current_date(), 1), 'recent', 'old')", ctx);
        Assertions.assertTrue(folded.contains("CAST('2026-09-22 00:00:00' AS DATETIME)"), folded);
        Assertions.assertTrue(safe(projectionOf(folded)), folded);
    }

    @Test
    public void perRowNonDeterministicProjectionDoesNotFold() {
        ConnectContext ctx = contextAt("2026-09-23T10:00:00Z", "UTC");
        Assertions.assertNull(foldedProjectionSql("rand()", ctx));
        Assertions.assertNull(foldedProjectionSql("uuid()", ctx));
        Assertions.assertNull(foldedProjectionSql("a + rand()", ctx));
    }

    @Test
    public void foldKeepsSubqueryAndForeignQualifierRejections() {
        ConnectContext ctx = contextAt("2026-09-23T10:00:00Z", "UTC");
        Assertions.assertFalse(safe(SamplingPredicateGate.foldPlanTimeConstants(
                whereOf("dt > (SELECT max(x) FROM t WHERE x < current_date())"), ctx)));
        Assertions.assertFalse(safe(SamplingPredicateGate.foldPlanTimeConstants(
                whereOf("other.dt >= date_sub(current_date(), 7)"), ctx)));
        Assertions.assertFalse(safe(SamplingPredicateGate.foldPlanTimeConstants(
                whereOf("dt >= date_sub(current_date(), 7) AND b = current_user()"), ctx)));
    }

    @Test
    public void dictionaryLookupRejected() {
        // dictionary_get parses to its own node, not a FunctionCallExpr, so the function allowlist never sees
        // it; its value depends on the dictionary's state at the moment the sampler runs.
        Assertions.assertFalse(safe(whereOf("dictionary_get('dict', a) IS NOT NULL")));
    }

    @Test
    public void castToAComplexTypeRejected() {
        // Whether a STRUCT converts by field name or by position is the session's SQL mode, and the sampler's
        // session is not the load's.
        Assertions.assertFalse(safe(whereOf("CAST(s AS STRUCT<a INT, b INT>) IS NOT NULL")));
        Assertions.assertTrue(safe(whereOf("CAST(a AS BIGINT) > 10")));
    }

    @Test
    public void implicitCastToAComplexTypeRejected() {
        // An already-analyzed statement (INSERT OVERWRITE) carries the coercions analysis inserted, with no TypeDef.
        SlotRef column = new SlotRef((TableName) null, "a");
        column.setType(IntegerType.INT);
        Assertions.assertFalse(safe(new CastExpr(new ArrayType(IntegerType.INT), column)));
        Assertions.assertTrue(safe(new CastExpr(IntegerType.BIGINT, column)));
    }

    // --- built-in binding ---

    private static final List<Column> STRUCT_SOURCE = List.of(new Column("a", IntegerType.BIGINT),
            new Column("payload", new StructType(List.of(new StructField("name", 0, VarcharType.VARCHAR, null)), true)));

    private static FunctionCallExpr callBoundTo(String name, Function fn, Expr... arguments) {
        FunctionCallExpr call = new FunctionCallExpr(name, List.of(arguments));
        call.setFn(fn);
        return call;
    }

    private static Function functionOfBinaryType(TFunctionBinaryType binaryType) {
        Function fn = mock(Function.class);
        when(fn.getBinaryType()).thenReturn(binaryType);
        return fn;
    }

    /**
     * {@code predicate} as the statement's own analysis leaves it on the INSERT OVERWRITE paths: resolved against
     * {@code columns} of the source, which the statement names {@link #SOURCE}.
     */
    private static Expr analyzedAsInTheStatement(String predicate, List<Column> columns, ConnectContext context) {
        Expr where = whereOf(predicate);
        List<Field> fields = new ArrayList<>();
        for (Column column : columns) {
            fields.add(new Field(column.getName(), column.getType(), SOURCE, null));
        }
        ExpressionAnalyzer.analyzeExpression(where, new AnalyzeState(),
                new Scope(RelationId.anonymous(), new RelationFields(fields)), context);
        return where;
    }

    @Test
    public void onlyACallBoundToABuiltinPasses() {
        // Every call in the tree is checked, not just the outermost one. A SQL function has no binary type, and a call
        // analysis did not bind has no function.
        Function builtin = functionOfBinaryType(TFunctionBinaryType.BUILTIN);
        SlotRef a = new SlotRef((TableName) null, "a");
        SqlFunction sqlFunction = new SqlFunction(new FunctionName("upper"), new Type[] {IntegerType.BIGINT},
                IntegerType.BIGINT, new String[] {"x"}, "x + 1");
        for (Function fn : Arrays.asList(functionOfBinaryType(TFunctionBinaryType.SRJAR),
                functionOfBinaryType(TFunctionBinaryType.PYTHON), sqlFunction, /*unbound*/ null)) {
            Assertions.assertNotNull(SamplingPredicateGate.firstCallNotBoundToABuiltin(callBoundTo("upper", fn, a)));
            Assertions.assertNotNull(SamplingPredicateGate.firstCallNotBoundToABuiltin(callBoundTo("abs", builtin,
                    callBoundTo("upper", fn, a))));
        }
        Assertions.assertNull(SamplingPredicateGate.firstCallNotBoundToABuiltin(callBoundTo("abs", builtin,
                callBoundTo("upper", builtin, a))));
        Assertions.assertNull(SamplingPredicateGate.firstCallNotBoundToABuiltin(whereOf("a > 1 AND b IS NULL")));
    }

    @Test
    public void analysisBindsCallsToBuiltinsWithoutTouchingTheParsedPredicate() {
        Expr where = whereOf("date_trunc('day', s.ts) >= '2026-01-01' AND upper(city) = 'X'");

        Expr analyzed = SamplingPredicateGate.analyzedAgainst(where,
                List.of(new Column("ts", DateType.DATETIME), new Column("city", VarcharType.VARCHAR)), SOURCE, null,
                UtFrameUtils.createDefaultCtx());

        Assertions.assertNotNull(analyzed);
        Assertions.assertNull(SamplingPredicateGate.firstCallNotBoundToABuiltin(analyzed));
        Assertions.assertNotNull(SamplingPredicateGate.firstCallNotBoundToABuiltin(where),
                "the parsed predicate must stay unanalyzed for the INSERT's own planner");
    }

    @Test
    public void qualifiedReferencesResolveAsTheStatementResolvesThem() {
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        List<Column> columns = List.of(new Column("a", IntegerType.BIGINT));
        // No alias in scope: the table name qualifies, alone or under its database and catalog.
        for (String predicate : List.of("abs(a) > 1", "abs(s.a) > 1", "abs(db1.s.a) > 1", "abs(cat.db1.s.a) > 1")) {
            Assertions.assertTrue(safe(whereOf(predicate)), predicate);
            Assertions.assertTrue(SamplingPredicateGate.evaluatesAsTheLoad(whereOf(predicate), columns, SOURCE, null,
                    context), predicate);
        }
        // An alias in scope is the only qualifier: the table name no longer resolves.
        Assertions.assertTrue(safe(whereOf("abs(t.a) > 1"), "t"));
        Assertions.assertTrue(SamplingPredicateGate.evaluatesAsTheLoad(whereOf("abs(t.a) > 1"), columns, SOURCE, "t",
                context));
        Assertions.assertFalse(SamplingPredicateGate.evaluatesAsTheLoad(whereOf("abs(s.a) > 1"), columns, SOURCE, "t",
                context));
    }

    @Test
    public void anAnalyzedStructFieldPredicateStillResolves() {
        // On the INSERT OVERWRITE paths the hook sees the statement analyzed: a STRUCT field read has its column name
        // rewritten to the field path, and only its qualified name still says how it resolves.
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        for (String predicate : List.of("s.payload.name = 'X'", "payload.name = 'X'", "upper(s.payload.name) = 'X'",
                "upper(payload.name) = 'X'")) {
            Expr analyzed = analyzedAsInTheStatement(predicate, STRUCT_SOURCE, context);
            Assertions.assertTrue(safe(analyzed), predicate);
            Assertions.assertTrue(SamplingPredicateGate.evaluatesAsTheLoad(analyzed, STRUCT_SOURCE, SOURCE, null, context),
                    predicate);
        }
        Assertions.assertNotNull(SamplingPredicateGate.analyzedAgainst(
                analyzedAsInTheStatement("upper(s.payload.name) = 'X'", STRUCT_SOURCE, context),
                STRUCT_SOURCE, SOURCE, null, context));
    }

    @Test
    public void aPredicateThatDoesNotAnalyzeAgainstTheSourceHasNoAnalyzedCopy() {
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        List<Column> columns = List.of(new Column("a", IntegerType.BIGINT));

        Assertions.assertNull(SamplingPredicateGate.analyzedAgainst(whereOf("missing > 1"), columns, SOURCE, null,
                context));
        Assertions.assertNull(SamplingPredicateGate.analyzedAgainst(whereOf("other.a > 1"), columns, SOURCE, null,
                context));
        // No built-in upper takes two arguments, and no function of that name is registered.
        Assertions.assertNull(SamplingPredicateGate.analyzedAgainst(whereOf("upper(a, a) = 'x'"), columns, SOURCE,
                null, context));
        Assertions.assertFalse(SamplingPredicateGate.evaluatesAsTheLoad(whereOf("upper(a, a) = 'x'"), columns, SOURCE,
                null, context));
        Assertions.assertTrue(SamplingPredicateGate.evaluatesAsTheLoad(whereOf("abs(a) > 1"), columns, SOURCE, null,
                context));
        // Only a call can bind a user-defined or SQL function, so a call-free predicate is admitted unanalyzed, even
        // one that does not analyze.
        Assertions.assertTrue(SamplingPredicateGate.evaluatesAsTheLoad(whereOf("missing > 1"), columns, SOURCE, null,
                context));
    }

    @Test
    public void anAllowlistedNameBoundToAGlobalSqlFunctionIsNotABuiltin() {
        // upper(BIGINT, BIGINT) matches no built-in and binds the global SQL function of that name. The name allowlist
        // admits the call; only its binding tells the two apart.
        Type[] argumentTypes = {IntegerType.BIGINT, IntegerType.BIGINT};
        SqlFunction sqlFunction = new SqlFunction(new FunctionName("upper"), argumentTypes, IntegerType.BIGINT,
                new String[] {"x", "y"}, "x + y");
        GlobalFunctionMgr globalFunctions = GlobalStateMgr.getCurrentState().getGlobalFunctionMgr();
        boolean savedEnableUdf = Config.enable_udf;
        Config.enable_udf = true;
        globalFunctions.replayAddFunction(sqlFunction);
        try {
            Expr where = whereOf("abs(upper(a, a)) > 0");
            Assertions.assertTrue(safe(where), "the allowlist goes by name");

            Expr analyzed = SamplingPredicateGate.analyzedAgainst(where, List.of(new Column("a", IntegerType.BIGINT)),
                    SOURCE, null, UtFrameUtils.createDefaultCtx());

            Assertions.assertNotNull(analyzed, "the inner call binds the global SQL function");
            Assertions.assertNotNull(SamplingPredicateGate.firstCallNotBoundToABuiltin(analyzed));
        } finally {
            globalFunctions.replayDropFunction(new FunctionSearchDesc(new FunctionName("upper"), argumentTypes, false));
            Config.enable_udf = savedEnableUdf;
        }
    }

    @Test
    public void aLiteralCountsWhenTheLoadsSqlModeReadsItBackAsAnotherType() {
        ConnectContext defaultMode = contextAt("2026-09-23T10:00:00Z", "UTC");
        ConnectContext doubleLiteral = contextAt("2026-09-23T10:00:00Z", "UTC");
        long parsedWithDoubleLiteral = SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_DOUBLE_LITERAL;
        doubleLiteral.getSessionVariable().setSqlMode(parsedWithDoubleLiteral);
        Expr decimal = SqlParser.parseSqlToExpr("a > 1.5", SqlModeHelper.MODE_DEFAULT);
        Expr parsedAsDouble = SqlParser.parseSqlToExpr("a > 1.5", parsedWithDoubleLiteral);

        // With DOUBLE_LITERAL the sampler reads 1.5 as a DOUBLE: a decimal literal counts, the user's DOUBLE does not.
        Assertions.assertTrue(SamplingPredicateGate.literalReadBackDifferently(decimal, doubleLiteral));
        Assertions.assertFalse(SamplingPredicateGate.literalReadBackDifferently(parsedAsDouble, doubleLiteral));
        // Without it the sampler reads 1.5 as a DECIMAL: a DOUBLE the parser kept from a session whose DOUBLE_LITERAL a
        // SET_VAR hint dropped counts, a decimal literal does not.
        Assertions.assertFalse(SamplingPredicateGate.literalReadBackDifferently(decimal, defaultMode));
        Assertions.assertTrue(SamplingPredicateGate.literalReadBackDifferently(parsedAsDouble, defaultMode));
        // An exponent with more than 38 integer digits is a DOUBLE in every sql_mode and renders as one.
        for (long sqlMode : new long[] {SqlModeHelper.MODE_DEFAULT, parsedWithDoubleLiteral}) {
            Expr wideExponent = SqlParser.parseSqlToExpr("a > 1E39", sqlMode);
            Assertions.assertEquals("`a` > 1.0E39", SamplingPredicateGate.toSql(wideExponent));
            Assertions.assertFalse(SamplingPredicateGate.literalReadBackDifferently(wideExponent, defaultMode));
            Assertions.assertFalse(SamplingPredicateGate.literalReadBackDifferently(wideExponent, doubleLiteral));
        }

        // A decimal the FE folds is a decimal literal inside a CAST in every sql_mode.
        Expr folded = SamplingPredicateGate.foldPlanTimeConstants(
                whereOf("a < CAST(concat('2', '.5') AS DECIMAL(10, 1))"), doubleLiteral);
        Assertions.assertEquals("`a` < (CAST(2.5 AS DECIMAL64(10,1)))", SamplingPredicateGate.toSql(folded));
        Assertions.assertTrue(SamplingPredicateGate.literalReadBackDifferently(folded, doubleLiteral));
        Assertions.assertFalse(SamplingPredicateGate.literalReadBackDifferently(folded, defaultMode));
    }

    @Test
    public void aDecimalLiteralCountsWhenItDoesNotReadBackAsTheSameDecimal() {
        ConnectContext defaultMode = contextAt("2026-09-23T10:00:00Z", "UTC");
        // An ordinary decimal literal renders as its digits and reads back as the same decimal; 1.5E3 renders as 1500E0,
        // and 0E0 is DECIMAL32(0,0) both ways.
        for (String predicate : List.of("a > 1.5", "a > 0.25", "a > -3.75", "a > 1234567890123456789012345678901234567.8",
                "a > 1.5E3", "a > 0E0")) {
            Expr parsed = SqlParser.parseSqlToExpr(predicate, SqlModeHelper.MODE_DEFAULT);
            Assertions.assertTrue(parsed.containsSubclass(DecimalLiteral.class), predicate);
            Assertions.assertFalse(SamplingPredicateGate.literalReadBackDifferently(parsed, defaultMode), predicate);
        }
        // An integer wider than LARGEINT renders as <digits>E0, which reads back as a DOUBLE in every sql_mode: under
        // cbo_eq_base_type=varchar a string compared with it would compare other digits.
        Expr wide = SqlParser.parseSqlToExpr("a > 100000000000000000000000000000000000000009", SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("`a` > 100000000000000000000000000000000000000009E0", SamplingPredicateGate.toSql(wide));
        Assertions.assertTrue(SamplingPredicateGate.literalReadBackDifferently(wide, defaultMode));
        // A DECIMALV2 literal renders without its trailing zeros: 1.50 reads back as the DECIMALV2 1.5, 2.0 as an integer.
        boolean savedDecimalV3 = Config.enable_decimal_v3;
        Config.enable_decimal_v3 = false;
        try {
            Assertions.assertFalse(SamplingPredicateGate.literalReadBackDifferently(
                    SqlParser.parseSqlToExpr("a > 1.50", SqlModeHelper.MODE_DEFAULT), defaultMode));
            Assertions.assertTrue(SamplingPredicateGate.literalReadBackDifferently(
                    SqlParser.parseSqlToExpr("a > 2.0", SqlModeHelper.MODE_DEFAULT), defaultMode));
        } finally {
            Config.enable_decimal_v3 = savedDecimalV3;
        }
    }

    @Test
    public void aFoldedFloatingPointConstantCountsOnlyWhenItsCastDoesNotRestoreTheSameValue() {
        // foldToLiteral keeps the folded type with an explicit CAST. Without DOUBLE_LITERAL the sampler reads 2.5 back
        // as a decimal that the cast converts to the same double, and 1.0E300 as a DOUBLE. 1.0E-100 reads back as 0:
        // a decimal literal keeps at most 76 digits, zeros after the point included.
        ConnectContext defaultMode = contextAt("2026-09-23T10:00:00Z", "UTC");
        ConnectContext doubleLiteral = contextAt("2026-09-23T10:00:00Z", "UTC");
        doubleLiteral.getSessionVariable().setSqlMode(SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_DOUBLE_LITERAL);
        Expr exact = SamplingPredicateGate.foldPlanTimeConstants(
                whereOf("a < CAST(concat('2', '.5') AS DOUBLE)"), defaultMode);
        Expr large = SamplingPredicateGate.foldPlanTimeConstants(
                whereOf("a < CAST(concat('1', 'E300') AS DOUBLE)"), defaultMode);
        Expr tiny = SamplingPredicateGate.foldPlanTimeConstants(
                whereOf("a < CAST(concat('1', 'E-100') AS DOUBLE)"), defaultMode);

        Assertions.assertEquals("`a` < (CAST(2.5 AS DOUBLE))", SamplingPredicateGate.toSql(exact));
        Assertions.assertEquals("`a` < (CAST(1.0E300 AS DOUBLE))", SamplingPredicateGate.toSql(large));
        Assertions.assertEquals("`a` < (CAST(1.0E-100 AS DOUBLE))", SamplingPredicateGate.toSql(tiny));
        for (Expr folded : List.of(exact, large, tiny)) {
            Assertions.assertFalse(SamplingPredicateGate.literalReadBackDifferently(folded, doubleLiteral),
                    SamplingPredicateGate.toSql(folded));
        }
        Assertions.assertFalse(SamplingPredicateGate.literalReadBackDifferently(exact, defaultMode));
        Assertions.assertFalse(SamplingPredicateGate.literalReadBackDifferently(large, defaultMode));
        Assertions.assertTrue(SamplingPredicateGate.literalReadBackDifferently(tiny, defaultMode));
    }

    @Test
    public void aDoubleThatAnalysisPutsInATypedConstructorOrMapSubscriptKeepsItsType() {
        // The statement's analysis casts a decimal element, map key or map subscript to the DOUBLE its context needs.
        // toSql renders an analyzed ARRAY and MAP with their types, and analysis casts a map subscript to the key type,
        // so the sampler converts the decimal it reads back to the same DOUBLE.
        ConnectContext context = contextAt("2026-09-23T10:00:00Z", "UTC");
        List<Column> columns = List.of(new Column("a", FloatType.DOUBLE),
                new Column("m", new MapType(FloatType.DOUBLE, IntegerType.INT)));
        for (String predicate : List.of("ARRAY<DOUBLE>[1.5, a] IS NOT NULL", "[1.5, a] IS NOT NULL",
                "MAP<DOUBLE,INT>{1.5:1} IS NOT NULL", "m[1.5] > 0")) {
            Expr analyzed = analyzedAsInTheStatement(predicate, columns, context);
            Assertions.assertTrue(analyzed.containsSubclass(FloatLiteral.class), predicate);
            Assertions.assertFalse(SamplingPredicateGate.literalReadBackDifferently(analyzed, context), predicate);
        }
        // The sampler reads what toSql renders, and that keeps the types analysis gave an ARRAY or MAP.
        Assertions.assertTrue(SamplingPredicateGate.toSql(analyzedAsInTheStatement("[1.5, a] IS NOT NULL", columns,
                context)).contains("ARRAY<DOUBLE>[1.5, "));
        Assertions.assertTrue(SamplingPredicateGate.toSql(analyzedAsInTheStatement("MAP<DOUBLE,INT>{1:1} IS NOT NULL",
                columns, context)).contains("MAP<DOUBLE,INT>{1.0:1}"));
        // A value the decimal read back does not convert to still counts: 1.0E-100 reads back as 0.
        Expr inexact = analyzedAsInTheStatement("ARRAY<DOUBLE>['1E-100', a] IS NOT NULL", columns, context);
        Assertions.assertTrue(SamplingPredicateGate.literalReadBackDifferently(inexact, context));
        // Parsed with DOUBLE_LITERAL and not analyzed, an array written without a type leaves its element type to the
        // sampler, which would infer it from a decimal.
        Expr untyped = SqlParser.parseSqlToExpr("[1.5, a] IS NOT NULL",
                SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_DOUBLE_LITERAL);
        Assertions.assertTrue(SamplingPredicateGate.literalReadBackDifferently(untyped, context));
    }

    @Test
    public void aDecimalThatAnalysisPutsInATypedConstructorOrMapSubscriptKeepsItsTypeOnlyWhenAnalysisCastsItBack() {
        // Analysis casts a decimal element or map subscript to the decimal type its context needs. The sampler reads the
        // rendered digits back as a decimal of their own precision and scale, which analysis casts to the context's type
        // only when the scale differs (an ARRAY element: 1.5 in ARRAY<DECIMAL32(9,2)>) or the decimal width does (a map
        // subscript: 1 for a DECIMAL64 key). Otherwise the literal counts: 1.555 in ARRAY<DECIMAL32(9,2)> renders at the
        // element's scale, and 1 read back for a DECIMAL32 key already has the key's width.
        ConnectContext context = contextAt("2026-09-23T10:00:00Z", "UTC");
        Type decimal32 = TypeFactory.createDecimalV3NarrowestType(9, 2);
        Type decimal64 = TypeFactory.createDecimalV3NarrowestType(18, 2);
        List<Column> columns = List.of(new Column("d", decimal32),
                new Column("m9", new MapType(decimal32, IntegerType.INT)),
                new Column("m18", new MapType(decimal64, IntegerType.INT)));
        for (String predicate : List.of("[1.5, d] IS NOT NULL", "m18[1] > 0")) {
            Expr analyzed = analyzedAsInTheStatement(predicate, columns, context);
            Assertions.assertTrue(analyzed.containsSubclass(DecimalLiteral.class), SamplingPredicateGate.toSql(analyzed));
            Assertions.assertFalse(SamplingPredicateGate.literalReadBackDifferently(analyzed, context),
                    SamplingPredicateGate.toSql(analyzed));
        }
        for (String predicate : List.of("ARRAY<DECIMAL32(9,2)>[1.555] IS NOT NULL", "m9[1] > 0")) {
            Expr analyzed = analyzedAsInTheStatement(predicate, columns, context);
            Assertions.assertTrue(analyzed.containsSubclass(DecimalLiteral.class), SamplingPredicateGate.toSql(analyzed));
            Assertions.assertTrue(SamplingPredicateGate.literalReadBackDifferently(analyzed, context),
                    SamplingPredicateGate.toSql(analyzed));
        }
    }
}
