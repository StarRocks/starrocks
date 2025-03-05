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
import com.starrocks.sql.ast.pipe.CreatePipeStmt;

public class AstTraverser<R, C> implements AstVisitor<R, C> {

    // ---------------------------------------- Query Statement --------------------------------------------------------------

    @Override
    public R visitQueryStatement(QueryStatement statement, C context) {
        visit(statement.getQueryRelation(), context);
        return null;
    }

    // ------------------------------------------- DML Statement -------------------------------------------------------

    @Override
    public R visitInsertStatement(InsertStmt statement, C context) {
        if (statement.getQueryStatement() != null) {
            visit(statement.getQueryStatement(), context);
        }
        return null;
    }

    @Override
    public R visitUpdateStatement(UpdateStmt statement, C context) {
        //Update Statement after analyze, all information will be used to build QueryStatement, so it is enough to traverse Query
        if (statement.getQueryStatement() != null) {
            visit(statement.getQueryStatement(), context);
        }
        return null;
    }

    @Override
    public R visitDeleteStatement(DeleteStmt statement, C context) {
        //Delete Statement after analyze, all information will be used to build QueryStatement, so it is enough to traverse Query
        if (statement.getQueryStatement() != null) {
            visit(statement.getQueryStatement(), context);
        }
        return null;
    }

    @Override
    public R visitSubmitTaskStatement(SubmitTaskStmt statement, C context) {
        if (statement.getInsertStmt() != null) {
            visit(statement.getInsertStmt(), context);
        }
        if (statement.getCreateTableAsSelectStmt() != null) {
            visit(statement.getCreateTableAsSelectStmt(), context);
        }
        return null;
    }

    @Override
    public R visitCreatePipeStatement(CreatePipeStmt statement, C context) {
        if (statement.getInsertStmt() != null) {
            visit(statement.getInsertStmt(), context);
        }
        return null;
    }

    @Override
    public R visitCreateTableAsSelectStatement(CreateTableAsSelectStmt statement, C context) {
        if (statement.getQueryStatement() != null) {
            visit(statement.getQueryStatement(), context);
        }
        if (statement.getInsertStmt() != null) {
            visit(statement.getInsertStmt(), context);
        }
        return null;
    }

    // ------------------------------------------- DDL Statement -------------------------------------------------------

    @Override
    public R visitAlterTableStatement(AlterTableStmt statement, C context) {
        statement.getAlterClauseList().forEach(x -> visit(x, context));
        return null;
    }

    @Override
    public R visitAlterViewStatement(AlterViewStmt statement, C context) {
        if (statement.getAlterClause() != null) {
            visit(statement.getAlterClause(), context);
        }
        return null;
    }

    @Override
    public R visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement, C context) {
        if (statement.getAlterTableClause() != null) {
            visit(statement.getAlterTableClause(), context);
        }
        return null;
    }

    // ------------------------------------------- Relation ------------------------------------------------------------

    @Override
    public R visitSelect(SelectRelation node, C context) {
        if (node.hasWithClause()) {
            node.getCteRelations().forEach(x -> visit(x, context));
        }

        if (node.getOrderBy() != null) {
            for (OrderByElement orderByElement : node.getOrderBy()) {
                visit(orderByElement.getExpr(), context);
            }
        }

        if (node.getOutputExpression() != null) {
            node.getOutputExpression().forEach(x -> visit(x, context));
        }

        if (node.getPredicate() != null) {
            visit(node.getPredicate(), context);
        }

        if (node.getGroupBy() != null) {
            node.getGroupBy().forEach(x -> visit(x, context));
        }

        if (node.getAggregate() != null) {
            node.getAggregate().forEach(x -> visit(x, context));
        }

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
    public R visitSubqueryRelation(SubqueryRelation node, C context) {
        return visit(node.getQueryStatement(), context);
    }

    @Override
    public R visitSetOp(SetOperationRelation node, C context) {
        if (node.hasWithClause()) {
            node.getCteRelations().forEach(x -> visit(x, context));
        }
        node.getRelations().forEach(x -> visit(x, context));
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
        node.getChildren().forEach(x -> visit(x, context));
        return null;
    }

    @Override
    public R visitSubqueryExpr(Subquery node, C context) {
        return visit(node.getQueryStatement(), context);
    }
}
