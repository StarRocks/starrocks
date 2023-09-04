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
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.Subquery;

public class AstRewriter<C> extends AstVisitor<ParseNode, C> {

    // ---------------------------------------- Query Statement --------------------------------------------------------------

    @Override
    public ParseNode visitQueryStatement(QueryStatement statement, C context) {
        visit(statement.getQueryRelation(), context);
        return statement;
    }

    // ------------------------------------------- DML Statement -------------------------------------------------------

    @Override
    public ParseNode visitInsertStatement(InsertStmt statement, C context) {
        visit(statement.getQueryStatement());
        return statement;
    }

    @Override
    public ParseNode visitUpdateStatement(UpdateStmt statement, C context) {
        //Update Statement after analyze, all information will be used to build QueryStatement, so it is enough to traverse Query
        if (statement.getQueryStatement() != null) {
            visit(statement.getQueryStatement());
        }
        return statement;
    }

    @Override
    public ParseNode visitDeleteStatement(DeleteStmt statement, C context) {
        //Delete Statement after analyze, all information will be used to build QueryStatement, so it is enough to traverse Query
        if (statement.getQueryStatement() != null) {
            visit(statement.getQueryStatement());
        }
        return statement;
    }

    // ------------------------------------------- Relation ----------------------------------==------------------------

    @Override
    public ParseNode visitSelect(SelectRelation node, C context) {
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

        visit(node.getRelation());
        return node;
    }

    @Override
    public ParseNode visitJoin(JoinRelation node, C context) {
        if (node.getOnPredicate() != null) {
            visit(node.getOnPredicate());
        }

        visit(node.getLeft());
        visit(node.getRight());
        return node;
    }

    @Override
    public ParseNode visitSubquery(SubqueryRelation node, C context) {
        return visit(node.getQueryStatement());
    }

    @Override
    public ParseNode visitSetOp(SetOperationRelation node, C context) {
        node.getRelations().forEach(this::visit);
        return node;
    }

    @Override
    public ParseNode visitCTE(CTERelation node, C context) {
        visit(node.getCteQueryStatement());
        return node;
    }

    @Override
    public ParseNode visitView(ViewRelation node, C context) {
        visit(node.getQueryStatement(), context);
        return node;
    }

    // ------------------------------------------- Expression --------------------------------==------------------------

    @Override
    public ParseNode visitExpression(Expr node, C context) {
        node.getChildren().forEach(this::visit);
        return node;
    }

    @Override
    public ParseNode visitSubquery(Subquery node, C context) {
        visit(node.getQueryStatement());
        return node;
    }
}
