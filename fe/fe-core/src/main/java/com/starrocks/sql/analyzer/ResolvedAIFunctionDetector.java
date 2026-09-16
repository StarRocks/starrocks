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

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.PivotRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.Subquery;

import java.util.Optional;

/** Detects AI functions from resolved function metadata rather than unresolved SQL names. */
public final class ResolvedAIFunctionDetector {
    private ResolvedAIFunctionDetector() {
    }

    public static boolean contains(ParseNode node) {
        return findFirst(node).isPresent();
    }

    static Optional<FunctionCallExpr> findFirst(ParseNode node) {
        return findFirst(node, true);
    }

    static Optional<FunctionCallExpr> findFirstInCurrentQueryBlock(ParseNode node) {
        return findFirst(node, false);
    }

    private static Optional<FunctionCallExpr> findFirst(ParseNode node, boolean descendIntoSubqueries) {
        Visitor visitor = new Visitor(descendIntoSubqueries);
        visitor.visit(node);
        return Optional.ofNullable(visitor.firstAIFunction);
    }

    private static class Visitor extends AstTraverser<Void, Void> {
        private final boolean descendIntoSubqueries;
        private FunctionCallExpr firstAIFunction;

        private Visitor(boolean descendIntoSubqueries) {
            this.descendIntoSubqueries = descendIntoSubqueries;
        }

        @Override
        public Void visitFunctionCall(FunctionCallExpr expr, Void context) {
            if (firstAIFunction == null && expr.getFn() != null && expr.getFn().isAi()) {
                firstAIFunction = expr;
            }
            return super.visitExpression(expr, context);
        }

        @Override
        public Void visitSubqueryExpr(Subquery node, Void context) {
            return descendIntoSubqueries ? super.visitSubqueryExpr(node, context) : null;
        }

        @Override
        public Void visitSetOp(SetOperationRelation node, Void context) {
            if (node.hasWithClause()) {
                node.getCteRelations().forEach(cte -> visit(cte, context));
            }
            node.getRelations().forEach(relation -> visit(relation, context));
            if (node.getOrderBy() != null) {
                node.getOrderBy().forEach(orderBy -> visit(orderBy.getExpr(), context));
            }
            return null;
        }

        @Override
        public Void visitValues(ValuesRelation node, Void context) {
            if (node.hasWithClause()) {
                node.getCteRelations().forEach(cte -> visit(cte, context));
            }
            if (node.getOrderBy() != null) {
                node.getOrderBy().forEach(orderBy -> visit(orderBy.getExpr(), context));
            }
            node.getRows().forEach(row -> row.forEach(expr -> visit(expr, context)));
            return null;
        }

        @Override
        public Void visitTableFunction(TableFunctionRelation node, Void context) {
            if (node.getChildExpressions() != null) {
                node.getChildExpressions().forEach(expression -> visit(expression, context));
            }
            return null;
        }

        @Override
        public Void visitNormalizedTableFunction(NormalizedTableFunctionRelation node, Void context) {
            return super.visitJoin(node, context);
        }

        @Override
        public Void visitPivotRelation(PivotRelation node, Void context) {
            if (node.getQuery() != null) {
                visit(node.getQuery(), context);
            }
            node.getAggregateFunctions().forEach(
                    aggregation -> visit(aggregation.getFunctionCallExpr(), context));
            node.getRewrittenAggFunctions().forEach(expression -> visit(expression, context));
            return null;
        }
    }
}
