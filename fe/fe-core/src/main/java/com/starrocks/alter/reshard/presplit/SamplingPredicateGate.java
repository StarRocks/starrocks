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
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.Parameter;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.ast.expression.UserVariableExpr;
import com.starrocks.sql.ast.expression.VariableExpr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.ScalarOperatorToExpr;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Decides whether a parsed (un-analyzed) WHERE {@code Expr} is safe to copy verbatim into the
 * sampling sub-query, and renders it to SQL.
 *
 * <p>The sampling sub-query runs as ROOT in a statistics {@code ConnectContext}. Expressions that
 * resolve differently in that context than in the user's INSERT context must be rejected so the
 * sampled row set does not diverge from the actual INSERT row set.
 *
 * <p>Safe constructs: column references, literals, comparison/logical/arithmetic operators,
 * IN with a constant list, IS NULL, BETWEEN, LIKE, CAST, and calls to the
 * {@link #ROW_LEVEL_FUNCTIONS} allowlist. Any other function call is rejected: even deterministic
 * functions that are session-timezone-sensitive (e.g. {@code from_unixtime}, {@code convert_tz},
 * {@code unix_timestamp}) produce a different row set when the ROOT context uses a different time
 * zone than the user's session. {@link #foldPlanTimeConstants} runs first and turns every
 * column-free function call (e.g. {@code date_sub(current_date(), 7)}) into a literal evaluated in
 * the user's own context, so such calls never reach the ROOT context at all.
 */
final class SamplingPredicateGate {

    /**
     * Built-in functions whose result depends only on their arguments -- not on the session time
     * zone, SQL mode, current user / database, or the clock -- so a column-dependent call evaluates
     * identically in the ROOT sampling context and in the user's INSERT context.
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
     * operators, IN with a constant list, IS NULL, BETWEEN, LIKE, CAST, and
     * {@link #ROW_LEVEL_FUNCTIONS}. Any other {@link FunctionCallExpr} is rejected because even
     * deterministic functions (e.g. {@code from_unixtime}, {@code convert_tz}) can be
     * session-timezone-sensitive and produce a different filter result when ROOT's context uses a
     * different time zone. Run {@link #foldPlanTimeConstants} first to admit column-free calls.
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
        //   - any FunctionCallExpr outside ROW_LEVEL_FUNCTIONS: even deterministic functions
        //     such as from_unixtime / convert_tz use the session time zone, which differs between
        //     the user's context and the ROOT statistics context, so they can filter a different
        //     row set;
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
        // Function calls outside the allowlist are rejected: even deterministic functions may
        // depend on session state (time zone, locale) that differs in the ROOT sampling context.
        // A subclass (dict_mapping, grouping) or a db-qualified name (a UDF) never matches.
        if (expr instanceof FunctionCallExpr fn) {
            return fn.getClass() != FunctionCallExpr.class
                    || fn.getDbName() != null
                    || fn.isDistinct()
                    || fn.getParams().isStar()
                    || !ROW_LEVEL_FUNCTIONS.contains(fn.getFunctionName().toLowerCase(Locale.ROOT));
        }
        // InformationFunction is a separate Expr subtype (e.g. current_user(), database())
        // that does not extend FunctionCallExpr.
        if (expr instanceof InformationFunction
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
     * Renders the predicate to re-parseable SQL.
     *
     * <p>Callers should only invoke this after {@link #isDeterministicAndSafe} has returned
     * {@code true}.
     */
    static String toSql(Expr wherePredicate) {
        return AstToSQLBuilder.toSQL(wherePredicate);
    }
}
