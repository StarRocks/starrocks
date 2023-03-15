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

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.Subquery;

public class AstTraverser<R, C> extends AstVisitor<R, C> {

    // ---------------------------------------- Query Statement --------------------------------------------------------------

    @Override
    public R visitQueryStatement(QueryStatement statement, C context) {
        visitQueryRelation(statement.getQueryRelation(), context);
        return null;
    }

    // ------------------------------------------- DML Statement -------------------------------------------------------

    @Override
    public R visitInsertStatement(InsertStmt statement, C context) {
        return visit(statement.getQueryStatement(), context);
    }

    @Override
    public R visitUpdateStatement(UpdateStmt statement, C context) {
        if (statement.getFromRelations() != null) {
            statement.getFromRelations().forEach(r -> visit(r, context));
        }
        if (statement.getWherePredicate() != null) {
            visit(statement.getWherePredicate(), context);
        }
        if (statement.getCommonTableExpressions() != null) {
            statement.getCommonTableExpressions().forEach(e -> visit(e, context));
        }
        if (statement.getQueryStatement() != null) {
            visit(statement.getQueryStatement(), context);
        }
        return null;
    }

    @Override
    public R visitDeleteStatement(DeleteStmt statement, C context) {
        if (statement.getUsingRelations() != null) {
            statement.getUsingRelations().forEach(r -> visit(r, context));
        }

        if (statement.getWherePredicate() != null) {
            visit(statement.getWherePredicate(), context);
        }
        if (statement.getCommonTableExpressions() != null) {
            statement.getCommonTableExpressions().forEach(e -> visit(e, context));
        }
        if (statement.getQueryStatement() != null) {
            visit(statement.getQueryStatement(), context);
        }
        return null;
    }

    // ------------------------------------------- Relation ----------------------------------==------------------------

    @Override
    public R visitSelect(SelectRelation node, C context) {
        if (node.hasWithClause()) {
            node.getCteRelations().forEach(r -> visit(r, context));
        }

        for (OrderByElement orderByElement : node.getOrderBy()) {
            visit(orderByElement.getExpr(), context);
        }

        node.getOutputExpression().forEach(e -> visit(e, context));

        if (node.getPredicate() != null) {
            visit(node.getPredicate(), context);
        }

        node.getGroupBy().forEach(c -> visit(c, context));
        node.getAggregate().forEach(c -> visit(c, context));

        if (node.getHaving() != null) {
            visit(node.getHaving(), context);
        }

        return visit(node.getRelation(), context);
    }

    @Override
    public R visitJoin(JoinRelation node, C context) {
        if (node.getOnPredicate() != null) {
            visit(node.getOnPredicate(), context);
        }

        visit(node.getLeft(), context);
        visit(node.getRight(), context);
        return null;
    }

    @Override
    public R visitSubquery(SubqueryRelation node, C context) {
        return visit(node.getQueryStatement(), context);
    }

    @Override
    public R visitSetOp(SetOperationRelation node, C context) {
        if (node.hasWithClause()) {
            node.getRelations().forEach(r -> visit(r, context));
        }
        node.getRelations().forEach(r -> visit(r, context));
        return null;
    }

    @Override
    public R visitCTE(CTERelation node, C context) {
        return visit(node.getCteQueryStatement(), context);
    }

    @Override
    public R visitView(ViewRelation node, C context) {
        return visit(node.getQueryStatement(), context);
    }

    // ------------------------------------------- Expression --------------------------------==------------------------

    @Override
    public R visitExpression(Expr node, C context) {
        node.getChildren().forEach(c -> visit(c, context));
        return null;
    }

    @Override
    public R visitSubquery(Subquery node, C context) {
        return visit(node.getQueryStatement(), context);
    }
}
