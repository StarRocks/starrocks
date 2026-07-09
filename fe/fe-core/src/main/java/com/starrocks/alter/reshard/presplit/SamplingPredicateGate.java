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
import com.starrocks.catalog.TableName;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.Parameter;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.UserVariableExpr;
import com.starrocks.sql.ast.expression.VariableExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * Decides whether a parsed (un-analyzed) WHERE {@code Expr} is safe to copy verbatim into the
 * sampling sub-query, and renders it to SQL.
 *
 * <p>The sampling sub-query runs as ROOT in a statistics {@code ConnectContext}. Expressions that
 * resolve differently in that context than in the user's INSERT context must be rejected so the
 * sampled row set does not diverge from the actual INSERT row set.
 *
 * <p>Safe constructs: column references, literals, comparison/logical/arithmetic operators,
 * IN with a constant list, IS NULL, BETWEEN, LIKE, and CAST. Any function call is rejected:
 * even deterministic functions that are session-timezone-sensitive (e.g. {@code from_unixtime},
 * {@code convert_tz}, {@code unix_timestamp}) produce a different row set when the ROOT context
 * uses a different time zone than the user's session.
 */
final class SamplingPredicateGate {

    private SamplingPredicateGate() {
    }

    /**
     * Returns {@code true} when {@code wherePredicate} is safe to copy verbatim into the ROOT
     * sampling sub-query; {@code false} when the predicate contains any construct that may
     * resolve differently in the ROOT statistics context than in the user's INSERT context.
     *
     * <p>The WHERE clause may contain only: columns, literals, comparison/logical/arithmetic
     * operators, IN with a constant list, IS NULL, BETWEEN, LIKE, and CAST. Any
     * {@link FunctionCallExpr} is rejected because even deterministic functions (e.g.
     * {@code from_unixtime}, {@code convert_tz}) can be session-timezone-sensitive and produce a
     * different filter result when ROOT's context uses a different time zone.
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
        //   - any FunctionCallExpr: even deterministic functions such as from_unixtime /
        //     convert_tz use the session time zone, which differs between the user's context
        //     and the ROOT statistics context, so they can filter a different row set;
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
        // All function calls are rejected: even deterministic functions may depend on
        // session state (time zone, locale) that differs in the ROOT sampling context.
        if (expr instanceof FunctionCallExpr) {
            return true;
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
