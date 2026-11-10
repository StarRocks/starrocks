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
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.PivotRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.ast.expression.Expr;

public final class SavedSqlGeoValidator extends AstTraverser<Void, Void> {
    private SavedSqlGeoValidator() {
    }

    // Validate at persistence boundaries because SQL rendering is also used for EXPLAIN, errors and dumps.
    public static void validate(ParseNode definition) {
        new SavedSqlGeoValidator().visit(definition, null);
    }

    @Override
    public Void visit(ParseNode node, Void context) {
        if (node instanceof Expr expr && expr.getType() != null && expr.getType().hasNestedGeoType()) {
            throw new SemanticException(
                    "Nested GEOGRAPHY/GEOMETRY types are not supported in saved SQL definitions", expr.getPos());
        }
        return super.visit(node, context);
    }

    @Override
    public Void visitCTE(CTERelation node, Void context) {
        return node.isAnchor() ? visit(node.getCteQueryStatement(), context) : null;
    }

    @Override
    public Void visitView(ViewRelation node, Void context) {
        return null;
    }

    @Override
    public Void visitSetOp(SetOperationRelation node, Void context) {
        visitOrderBy(node, context);
        return super.visitSetOp(node, context);
    }

    @Override
    public Void visitValues(ValuesRelation node, Void context) {
        node.getCteRelations().forEach(cte -> visit(cte, context));
        visitOrderBy(node, context);
        node.getRows().forEach(row -> row.forEach(expr -> visit(expr, context)));
        return null;
    }

    private void visitOrderBy(QueryRelation node, Void context) {
        if (node.hasOrderByClause()) {
            node.getOrderBy().forEach(orderBy -> visit(orderBy.getExpr(), context));
        }
    }

    @Override
    public Void visitTableFunction(TableFunctionRelation node, Void context) {
        node.getChildExpressions().forEach(expr -> visit(expr, context));
        return null;
    }

    @Override
    public Void visitNormalizedTableFunction(NormalizedTableFunctionRelation node, Void context) {
        return visitJoin(node, context);
    }

    @Override
    public Void visitJoin(JoinRelation node, Void context) {
        if (node.getSkewColumn() != null) {
            visit(node.getSkewColumn(), context);
            if (node.getSkewValues() != null) {
                node.getSkewValues().forEach(expr -> visit(expr, context));
            }
        }
        return super.visitJoin(node, context);
    }

    @Override
    public Void visitPivotRelation(PivotRelation node, Void context) {
        visit(node.getQuery(), context);
        node.getRewrittenAggFunctions().forEach(expr -> visit(expr, context));
        return null;
    }
}
