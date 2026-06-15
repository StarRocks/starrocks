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

        // One walk collects every node that resolves differently in the ROOT
        // sampling context than in the user's INSERT context, or that the
        // sampler cannot copy verbatim. The predicate rejects, in one pass:
        //   - non-deterministic / information FunctionCallExpr (e.g. now(), database());
        //     some information functions parse as InformationFunction nodes instead, so
        //     both shapes are matched;
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
        if (expr instanceof FunctionCallExpr function) {
            String name = function.getFunctionName();
            return FunctionSet.allNonDeterministicFunctions.contains(name)
                    || FunctionSet.INFORMATION_FUNCTIONS.contains(name);
        }
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
