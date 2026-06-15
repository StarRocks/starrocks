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
import com.starrocks.catalog.FunctionSet;
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
 */
final class SamplingPredicateGate {

    private SamplingPredicateGate() {
    }

    /**
     * Returns {@code true} when {@code wherePredicate} is safe to copy verbatim.
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

        // Reject non-deterministic and information functions.
        // Use allNonDeterministicFunctions (covers time functions like now(), current_timestamp())
        // plus INFORMATION_FUNCTIONS (database(), current_user(), etc. resolve from context).
        // Note: some information functions (current_user, database) are parsed as InformationFunction
        // nodes rather than FunctionCallExpr — both must be rejected.
        List<FunctionCallExpr> functions = new ArrayList<>();
        wherePredicate.collectAll((Predicate<Expr>) e -> e instanceof FunctionCallExpr, functions);
        for (FunctionCallExpr function : functions) {
            String name = function.getFunctionName();
            if (FunctionSet.allNonDeterministicFunctions.contains(name)
                    || FunctionSet.INFORMATION_FUNCTIONS.contains(name)) {
                return false;
            }
        }
        List<InformationFunction> infoFunctions = new ArrayList<>();
        wherePredicate.collectAll((Predicate<Expr>) e -> e instanceof InformationFunction, infoFunctions);
        if (!infoFunctions.isEmpty()) {
            return false;
        }

        // Session variables, user variables, and prepared-statement parameters resolve from the
        // caller's ConnectContext; the ROOT sampling context evaluates them differently.
        List<Expr> contextVariables = new ArrayList<>();
        wherePredicate.collectAll(
                (Predicate<Expr>) e -> e instanceof VariableExpr || e instanceof UserVariableExpr
                        || e instanceof Parameter,
                contextVariables);
        if (!contextVariables.isEmpty()) {
            return false;
        }

        // Reject all subquery shapes (scalar, IN-subquery, EXISTS). EXISTS holds its Subquery as a
        // child, so this single pass covers it too.
        List<Subquery> subqueries = new ArrayList<>();
        wherePredicate.collectAll((Predicate<Expr>) e -> e instanceof Subquery, subqueries);
        if (!subqueries.isEmpty()) {
            return false;
        }

        // Reject IN predicates backed by a subquery; allow literal-list IN.
        List<InPredicate> ins = new ArrayList<>();
        wherePredicate.collectAll((Predicate<Expr>) e -> e instanceof InPredicate, ins);
        for (InPredicate in : ins) {
            if (!in.isConstantValues()) {
                return false;
            }
        }

        // Reject column references qualified with a table other than the source relation.
        List<SlotRef> slots = new ArrayList<>();
        wherePredicate.collectAll((Predicate<Expr>) e -> e instanceof SlotRef, slots);
        for (SlotRef slot : slots) {
            if (slot.getTblName() != null
                    && !InsertSelectSourceColumns.matchesSource(slot.getTblName(), normalizedSourceName, sourceAlias)) {
                return false;
            }
        }

        return true;
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
