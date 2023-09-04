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
        visit(statement.getQueryRelation(), context);
        return null;
    }

    // ------------------------------------------- DML Statement -------------------------------------------------------

    @Override
    public R visitInsertStatement(InsertStmt statement, C context) {
        return visit(statement.getQueryStatement());
    }

    @Override
    public R visitUpdateStatement(UpdateStmt statement, C context) {
        //Update Statement after analyze, all information will be used to build QueryStatement, so it is enough to traverse Query
        if (statement.getQueryStatement() != null) {
            visit(statement.getQueryStatement());
        }
        return null;
    }

    @Override
    public R visitDeleteStatement(DeleteStmt statement, C context) {
        //Delete Statement after analyze, all information will be used to build QueryStatement, so it is enough to traverse Query
        if (statement.getQueryStatement() != null) {
            visit(statement.getQueryStatement());
        }
        return null;
    }

    // ------------------------------------------- Relation ----------------------------------==------------------------

    @Override
    public R visitSelect(SelectRelation node, C context) {
        if (node.hasWithClause()) {
            node.getCteRelations().forEach(this::visit);
        }

        if (node.getOrderBy() != null) {
            for (OrderByElement orderByElement : node.getOrderBy()) {
                visit(orderByElement.getExpr());
            }
        }

        if (node.getOutputExpression() != null) {
            node.getOutputExpression().forEach(this::visit);
        }

        if (node.getPredicate() != null) {
            visit(node.getPredicate());
        }

        if (node.getGroupBy() != null) {
            node.getGroupBy().forEach(this::visit);
        }

        if (node.getAggregate() != null) {
            node.getAggregate().forEach(this::visit);
        }

        if (node.getHaving() != null) {
            visit(node.getHaving());
        }

        return visit(node.getRelation());
    }

    @Override
    public R visitJoin(JoinRelation node, C context) {
        if (node.getOnPredicate() != null) {
            visit(node.getOnPredicate());
        }

        visit(node.getLeft());
        visit(node.getRight());
        return null;
    }

    @Override
    public R visitSubquery(SubqueryRelation node, C context) {
        return visit(node.getQueryStatement());
    }

    @Override
    public R visitSetOp(SetOperationRelation node, C context) {
        if (node.hasWithClause()) {
            node.getRelations().forEach(this::visit);
        }
        node.getRelations().forEach(this::visit);
        return null;
    }

    @Override
    public R visitCTE(CTERelation node, C context) {
        return visit(node.getCteQueryStatement());
    }

    @Override
    public R visitView(ViewRelation node, C context) {
        return visit(node.getQueryStatement(), context);
    }

    // ------------------------------------------- Expression --------------------------------==------------------------

    @Override
    public R visitExpression(Expr node, C context) {
        node.getChildren().forEach(this::visit);
        return null;
    }

    @Override
    public R visitSubquery(Subquery node, C context) {
        return visit(node.getQueryStatement());
    }
}
