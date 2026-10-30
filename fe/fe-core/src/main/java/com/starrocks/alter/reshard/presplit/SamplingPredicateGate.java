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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CollectionElementExpr;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.DictionaryGetExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.Parameter;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.ast.expression.UserVariableExpr;
import com.starrocks.sql.ast.expression.VariableExpr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Decides whether a parsed (un-analyzed) WHERE {@code Expr} is safe to copy verbatim into the
 * sampling sub-query, and renders it to SQL.
 *
 * <p>The sampling sub-query runs as ROOT in a statistics {@code ConnectContext}. That session carries
 * the load's semantic session variables ({@link SampleSessionSemantics}: SQL mode, time zone,
 * comparison and decimal settings), but not its user, its current database or its query start time.
 * Expressions that resolve differently in the two sessions must be rejected so the sampled row set
 * does not diverge from the actual INSERT row set.
 *
 * <p>Safe constructs: column references, literals, comparison/logical/arithmetic operators,
 * IN with a constant list, IS NULL, BETWEEN, LIKE, CAST to a scalar type, and calls to the
 * {@link #ROW_LEVEL_FUNCTIONS} allowlist; any other function call is rejected.
 * {@link #foldPlanTimeConstants} runs first and turns every column-free function
 * call (e.g. {@code date_sub(current_date(), 7)}) into a literal evaluated in the user's own
 * context, so such calls never reach the sampler at all.
 *
 * <p>The allowlist goes by name. A UDF or SQL function of an allowlisted name is reached only when no
 * built-in signature accepts the arguments, and is looked up in the session's current database, which
 * the sampler's session does not share, so {@link #evaluatesAsTheLoad} also checks that every call
 * binds to a built-in function.
 */
final class SamplingPredicateGate {

    private static final Logger LOG = LogManager.getLogger(SamplingPredicateGate.class);

    /**
     * Built-in functions whose result depends only on their arguments and on session variables the
     * sample session inherits ({@code lower} and {@code upper} read {@code lower_upper_support_utf8})
     * -- not on the session time zone, the current user / database, or the clock -- so a
     * column-dependent call evaluates identically in the ROOT sampling context and in the user's
     * INSERT context. The session-time-zone-sensitive ones ({@code from_unixtime}, {@code convert_tz},
     * {@code unix_timestamp}) stay out although the sample session inherits the time zone.
     */
    private static final Set<String> ROW_LEVEL_FUNCTIONS = ImmutableSet.of(
            "date_trunc", "date", "to_date", "year", "month", "day", "dayofmonth", "hour", "minute", "second",
            "substr", "substring", "left", "right", "lower", "upper", "trim", "ltrim", "rtrim", "length",
            "concat", "abs", "coalesce", "ifnull", "nullif", "if");

    private SamplingPredicateGate() {
    }

    /**
     * Replaces every column-free sub-expression that contains a function call with the literal it
     * folds to in the user's {@code ConnectContext}, so plan-time constants such as
     * {@code date_sub(current_date(), 7)} reach the ROOT sampling context as a typed literal and
     * leave no session state (time zone, query start time) to disagree about. The INSERT's own
     * planner folds the same calls with the same context, so both filter the same row set.
     *
     * <p>The input is never mutated: the caller's parsed statement is analyzed later by the real
     * planner. A sub-expression that the FE cannot fold to a constant -- a per-row
     * non-deterministic function such as {@code rand()}, or one the FE has no constant evaluator
     * for -- makes the whole predicate unusable.
     *
     * @return {@code wherePredicate} itself when it has nothing to fold (including {@code null}), a
     *         folded copy otherwise, or {@code null} when some function-bearing column-free
     *         sub-expression does not fold to a constant
     */
    static Expr foldPlanTimeConstants(Expr wherePredicate, ConnectContext context) {
        if (wherePredicate == null || !wherePredicate.containsSubclass(FunctionCallExpr.class)) {
            return wherePredicate;
        }
        Expr folded = foldInPlace(wherePredicate.clone(), context);
        // A predicate that folds as a whole must still be a boolean; the INSERT rejects anything
        // else at analysis, so do not hand the sampler a WHERE it cannot mean.
        if (folded != null && !wherePredicate.contains((Predicate<Expr>) SamplingPredicateGate::blocksFolding)
                && !folded.getType().isBoolean() && !folded.getType().isNull()) {
            return null;
        }
        return folded;
    }

    /**
     * {@link #foldPlanTimeConstants} for a computed INSERT projection rather than a WHERE clause. A
     * projection with no column folds as a whole, so a cast literal or {@code current_date()} reaches
     * the sampler as the constant the INSERT writes; one with a column keeps its column-bearing nodes
     * and folds only its column-free calls, exactly as a predicate does.
     *
     * @return a folded copy, or {@code null} when a column-free part does not fold to a constant or
     *         the whole projection folds to NULL
     */
    static Expr foldProjection(Expr projection, ConnectContext context) {
        if (projection.contains((Predicate<Expr>) SamplingPredicateGate::blocksFolding)) {
            return foldInPlace(projection.clone(), context);
        }
        Expr folded = foldToLiteral(projection.clone(), context);
        return folded == null || folded.getChild(0) instanceof NullLiteral ? null : folded;
    }

    private static Expr foldInPlace(Expr expr, ConnectContext context) {
        if (!expr.containsSubclass(FunctionCallExpr.class)) {
            return expr;
        }
        if (!expr.contains((Predicate<Expr>) SamplingPredicateGate::blocksFolding)) {
            return foldToLiteral(expr, context);
        }
        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr child = foldInPlace(expr.getChild(i), context);
            if (child == null) {
                return null;
            }
            expr.setChild(i, child);
        }
        return expr;
    }

    /**
     * A node that must not be evaluated in the user's context ahead of the INSERT: a column
     * (per-row, not a constant), a subquery (reads a relation the hook never resolved or
     * authorized), or a variable / parameter / information function (left to the gate's
     * rejection, which predates folding).
     */
    private static boolean blocksFolding(Expr expr) {
        return expr instanceof SlotRef
                || expr instanceof Subquery
                || expr instanceof InformationFunction
                || expr instanceof VariableExpr
                || expr instanceof UserVariableExpr
                || expr instanceof Parameter;
    }

    private static Expr foldToLiteral(Expr expr, ConnectContext context) {
        ScalarOperator scalar;
        try {
            ExpressionAnalyzer.analyzeExpressionIgnoreSlot(expr, context);
            scalar = new ScalarOperatorRewriter().rewrite(SqlToScalarOperatorTranslator.translate(expr),
                    ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        } catch (Exception e) {
            return null;
        }
        if (!scalar.isConstantRef() || !scalar.getType().isScalarType()) {
            return null;
        }
        Expr literal = ScalarOperatorToExpr.buildExprIgnoreSlot(scalar,
                new ScalarOperatorToExpr.FormatterContext(Maps.newHashMap()));
        // Keep the folded type: '2026-01-01' re-parses as VARCHAR, and comparing a VARCHAR column
        // with a VARCHAR literal is not the comparison the INSERT makes against a DATE.
        return new CastExpr(new TypeDef(scalar.getType()), literal);
    }

    /**
     * Returns {@code true} when {@code wherePredicate} is safe to copy verbatim into the ROOT
     * sampling sub-query; {@code false} when the predicate contains any construct that may
     * resolve differently in the ROOT statistics context than in the user's INSERT context.
     *
     * <p>The WHERE clause may contain only: columns, literals, comparison/logical/arithmetic
     * operators, IN with a constant list, IS NULL, BETWEEN, LIKE, CAST to a scalar type, and
     * {@link #ROW_LEVEL_FUNCTIONS}; any other {@link FunctionCallExpr} is rejected. Run
     * {@link #foldPlanTimeConstants} first to admit column-free calls.
     *
     * @param wherePredicate      the raw WHERE expression from the parsed INSERT statement; {@code null}
     *                            means no WHERE clause, which is always safe
     * @param normalizedSourceName fully-qualified source name (catalog/db/tbl) for qualifier matching
     * @param sourceAlias         the FROM-clause alias for the source relation, or {@code null}
     */
    static boolean isDeterministicAndSafe(Expr wherePredicate, TableName normalizedSourceName, String sourceAlias) {
        if (wherePredicate == null) {
            return true;
        }

        // One walk collects every node that is unsafe to copy into the ROOT sampling
        // sub-query. Rejected in a single pass:
        //   - any FunctionCallExpr outside ROW_LEVEL_FUNCTIONS (see its javadoc);
        //   - dictionary lookups: DictionaryGetExpr is its own node, outside the function allowlist,
        //     and reads the dictionary's state when the sampler runs;
        //   - casts to a complex type, written or inserted by analysis: a STRUCT converts by field
        //     name or by position, and the rule deliberately keeps such casts out rather than reason
        //     about them;
        //   - InformationFunction nodes (a distinct Expr subtype that some information
        //     functions like current_user() parse into);
        //   - session / user variables and prepared-statement parameters, which resolve
        //     from the caller's ConnectContext;
        //   - any subquery shape (scalar, IN-subquery, EXISTS holds its Subquery as a child);
        //   - IN predicates not backed by a constant value list;
        //   - column references qualified with a table other than the source relation.
        List<Expr> rejected = new ArrayList<>();
        wherePredicate.collectAll(
                (Predicate<Expr>) e -> isUnsafe(e, normalizedSourceName, sourceAlias), rejected);
        return rejected.isEmpty();
    }

    private static boolean isUnsafe(Expr expr, TableName normalizedSourceName, String sourceAlias) {
        // Function calls outside ROW_LEVEL_FUNCTIONS are rejected; evaluatesAsTheLoad later checks that each
        // admitted call binds to a built-in. A subclass (dict_mapping, grouping) or a db-qualified name (a UDF)
        // never matches.
        if (expr instanceof FunctionCallExpr fn) {
            return fn.getClass() != FunctionCallExpr.class
                    || fn.getDbName() != null
                    || fn.isDistinct()
                    || fn.getParams().isStar()
                    || !ROW_LEVEL_FUNCTIONS.contains(fn.getFunctionName().toLowerCase(Locale.ROOT));
        }
        if (expr instanceof CastExpr cast) {
            // A cast analysis inserted (an implicit coercion) carries no TypeDef, only its result type.
            return (cast.getTargetTypeDef() != null && cast.getTargetTypeDef().getType().isComplexType())
                    || (cast.getType() != null && cast.getType().isComplexType());
        }
        // InformationFunction is a separate Expr subtype (e.g. current_user(), database())
        // that does not extend FunctionCallExpr. DictionaryGetExpr is likewise its own node.
        if (expr instanceof DictionaryGetExpr
                || expr instanceof InformationFunction
                || expr instanceof VariableExpr
                || expr instanceof UserVariableExpr
                || expr instanceof Parameter
                || expr instanceof Subquery) {
            return true;
        }
        if (expr instanceof InPredicate in) {
            return !in.isConstantValues();
        }
        if (expr instanceof SlotRef slot) {
            return slot.getTblName() != null
                    && !InsertSelectSourceColumns.matchesSource(slot.getTblName(), normalizedSourceName, sourceAlias);
        }
        return false;
    }

    /**
     * A copy of {@code expr} analyzed in {@code context} against the source relation, or {@code null} when it does
     * not analyze. The relation is made of {@code columns} and named as the statement names it
     * ({@code TableRelation#getResolveTableName}): the alias, under the source's catalog and database, when there is
     * one, otherwise the normalized table name. A reference therefore keeps whatever qualifier or STRUCT field path
     * it carries and resolves as it does in the statement -- also one the statement's own analysis already resolved,
     * as on the INSERT OVERWRITE paths, where a STRUCT field read keeps its path only in its qualified name. With
     * neither a name nor an alias the relation is unnamed, so only unqualified references resolve. {@code expr} itself
     * is left untouched: the INSERT's own planner analyzes it.
     */
    static Expr analyzedAgainst(Expr expr, List<Column> columns, TableName normalizedSourceName, String sourceAlias,
                                ConnectContext context) {
        TableName relationName = sourceAlias == null ? normalizedSourceName
                : normalizedSourceName == null ? new TableName(null, null, sourceAlias)
                : new TableName(normalizedSourceName.getCatalog(), normalizedSourceName.getDb(), sourceAlias);
        List<Field> fields = new ArrayList<>();
        for (Column column : columns) {
            fields.add(new Field(column.getName(), column.getType(), relationName, null));
        }
        Expr analyzed = expr.clone();
        try {
            ExpressionAnalyzer.analyzeExpression(analyzed, new AnalyzeState(),
                    new Scope(RelationId.anonymous(), new RelationFields(fields)), context);
        } catch (RuntimeException e) {
            return null;
        }
        return analyzed;
    }

    /**
     * The first function call in {@code analyzed}, an analyzed expression, that is not bound to a built-in function --
     * a Java or Python UDF, a SQL function (it has no binary type) or a call analysis did not bind -- walking a call
     * before its arguments, or {@code null} when there is none. A built-in is looked up before any user-defined
     * function and independently of the session, so a call the load binds to one binds to the same one in the sampler
     * for the same argument types.
     */
    static FunctionCallExpr firstCallNotBoundToABuiltin(Expr analyzed) {
        List<FunctionCallExpr> calls = new ArrayList<>();
        analyzed.collectAll((Predicate<Expr>) node -> node instanceof FunctionCallExpr call
                && (call.getFn() == null || call.getFn().getBinaryType() != TFunctionBinaryType.BUILTIN), calls);
        return calls.isEmpty() ? null : calls.get(0);
    }

    /**
     * Whether the sample session evaluates {@code folded}, an expression admitted after folding, as the load does, as
     * far as the hook can tell before the load runs: no literal reads back as another type
     * ({@link #literalReadBackDifferently}), and every call binds to a built-in function when {@code folded} is
     * analyzed against the source relation ({@link #analyzedAgainst}); {@code false} when it does not analyze. An
     * expression without a call needs no analysis: an operator, a cast, a predicate or a CASE never binds a
     * user-defined or SQL function.
     */
    static boolean evaluatesAsTheLoad(Expr folded, List<Column> columns, TableName normalizedSourceName,
                                      String sourceAlias, ConnectContext context) {
        if (literalReadBackDifferently(folded, context)) {
            return false;
        }
        if (!folded.containsSubclass(FunctionCallExpr.class)) {
            return true;
        }
        Expr analyzed = analyzedAgainst(folded, columns, normalizedSourceName, sourceAlias, context);
        return analyzed != null && firstCallNotBoundToABuiltin(analyzed) == null;
    }

    /** {@link #evaluatesAsTheLoad} for a load's folded WHERE clause; a decline is logged against {@code targetName}. */
    static boolean whereEvaluatesAsTheLoad(Expr where, List<Column> columns, TableName normalizedSourceName,
                                           String sourceAlias, ConnectContext context, String targetName) {
        if (evaluatesAsTheLoad(where, columns, normalizedSourceName, sourceAlias, context)) {
            return true;
        }
        LOG.info("Sample-Based Tablet Pre-Split: the WHERE clause of the load into table {} calls a function that "
                + "binds to no built-in, or has a literal the sample session would read as another type; "
                + "skipping pre-split", targetName);
        return false;
    }

    /**
     * Whether the sampler would read a literal of {@code expr} back as another literal. The sampler parses the SQL
     * rendered from {@code expr} in the load's sql_mode, which {@code context} carries and the sample session
     * inherits. The literals of {@code expr} were parsed otherwise: a stored definition's in the default sql_mode;
     * the statement's in the session's sql_mode together with a SET_VAR hint's, so a hint that drops DOUBLE_LITERAL
     * leaves the statement's numbers DOUBLEs; analysis and folding built the rest from values.
     *
     * <p>A decimal or floating-point literal counts when its rendered text, parsed in that sql_mode, is not a literal
     * of the same class, type and value: under DOUBLE_LITERAL every decimal literal, which comes back as a DOUBLE;
     * without it a DOUBLE, which comes back as a decimal unless it is an exponent with more than 38 integer digits,
     * and a decimal of scale 0 with more than 38 digits, which renders as {@code <digits>E0} and comes back as a
     * DOUBLE. It does not count where its context converts what the sampler reads back to its own type and value
     * ({@link #restoredBy}).
     */
    static boolean literalReadBackDifferently(Expr expr, ConnectContext context) {
        return readBackDifferently(expr, /*parent*/ null, /*childIndex*/ -1, context.getSessionVariable().getSqlMode());
    }

    private static boolean readBackDifferently(Expr expr, Expr parent, int childIndex, long sqlMode) {
        if (expr instanceof DecimalLiteral || expr instanceof FloatLiteral) {
            Expr readBack = SqlParser.parseSqlToExpr(toSql(expr), sqlMode);
            return !sameLiteral(expr, readBack) && !restoredBy(parent, childIndex, expr, readBack);
        }
        for (int i = 0; i < expr.getChildren().size(); i++) {
            if (readBackDifferently(expr.getChild(i), expr, i, sqlMode)) {
                return true;
            }
        }
        return false;
    }

    /** Whether {@code readBack} is a literal of the class, type and value of {@code literal}. */
    private static boolean sameLiteral(Expr literal, Expr readBack) {
        if (readBack.getClass() != literal.getClass() || !readBack.getType().equals(literal.getType())) {
            return false;
        }
        return literal instanceof DecimalLiteral decimal
                ? decimal.getValue().compareTo(((DecimalLiteral) readBack).getValue()) == 0
                : Double.compare(((FloatLiteral) literal).getValue(), ((FloatLiteral) readBack).getValue()) == 0;
    }

    /**
     * Whether the SQL rendered for {@code parent} makes the sampler convert {@code readBack}, which it reads in place
     * of {@code literal}, the child number {@code childIndex}, to the type and value {@code literal} has there. A
     * floating-point literal is restored when the context converts to a floating-point type and the value read back
     * is the same double: a decimal converts through {@code BigDecimal#doubleValue}, in a CAST the FE folds as in an
     * ARRAY, a MAP or a map subscript that analysis casts. A decimal literal is restored only when it reads back as a
     * decimal of the same value and the context converts that to the literal's type: a CAST to a decimal type, which
     * converts by value; an ARRAY element or MAP key or value of the literal's type, which analysis casts to it when
     * the decimal read back does not {@code matchesType} it; or a map subscript of the literal's key type, which
     * analysis casts to it when the primitive types differ.
     */
    private static boolean restoredBy(Expr parent, int childIndex, Expr literal, Expr readBack) {
        Type target = convertedTypeOf(parent, childIndex);
        if (target == null) {
            return false;
        }
        if (literal instanceof FloatLiteral floatLiteral) {
            Double value = readBack instanceof DecimalLiteral decimal ? Double.valueOf(decimal.getValue().doubleValue())
                    : readBack instanceof FloatLiteral read ? Double.valueOf(read.getValue()) : null;
            return target.isFloatingPointType() && value != null && Double.compare(value, floatLiteral.getValue()) == 0;
        }
        if (!(readBack instanceof DecimalLiteral read) || !target.isDecimalOfAnyVersion()
                || read.getValue().compareTo(((DecimalLiteral) literal).getValue()) != 0) {
            return false;
        }
        if (parent instanceof CastExpr) {
            return true;
        }
        if (!target.equals(literal.getType())) {
            return false;
        }
        return parent instanceof CollectionElementExpr
                ? read.getType().getPrimitiveType() != target.getPrimitiveType()
                : !read.getType().matchesType(target);
    }

    /**
     * The type the SQL rendered for {@code parent} converts its child number {@code childIndex} to, in one of the
     * contexts {@link #restoredBy} lists, or {@code null}. toSql renders an ARRAY or MAP with its type.
     */
    private static Type convertedTypeOf(Expr parent, int childIndex) {
        if (parent instanceof CastExpr cast) {
            return !cast.isImplicit() && cast.getTargetTypeDef() != null ? cast.getTargetTypeDef().getType() : null;
        }
        if (parent instanceof ArrayExpr array) {
            return array.getType() instanceof ArrayType arrayType ? arrayType.getItemType() : null;
        }
        if (parent instanceof MapExpr map) {
            return map.getType() instanceof MapType mapType
                    ? (childIndex % 2 == 0 ? mapType.getKeyType() : mapType.getValueType()) : null;
        }
        return parent instanceof CollectionElementExpr element && childIndex == 1
                && element.getChild(0).getType() instanceof MapType mapType ? mapType.getKeyType() : null;
    }

    /**
     * Renders the predicate to re-parseable SQL.
     *
     * <p>Callers should only invoke this after {@link #isDeterministicAndSafe} has returned
     * {@code true}.
     */
    static String toSql(Expr wherePredicate) {
        return AstToSQLBuilder.toSQL(wherePredicate);
    }
}
