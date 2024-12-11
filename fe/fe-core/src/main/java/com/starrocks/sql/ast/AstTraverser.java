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
<<<<<<< HEAD

public class AstTraverser<R, C> extends AstVisitor<R, C> {
=======
import com.starrocks.sql.ast.pipe.CreatePipeStmt;

public class AstTraverser<R, C> implements AstVisitor<R, C> {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

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
<<<<<<< HEAD
            visit(statement.getQueryStatement());
=======
            visit(statement.getQueryStatement(), context);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        return null;
    }

    @Override
    public R visitUpdateStatement(UpdateStmt statement, C context) {
        //Update Statement after analyze, all information will be used to build QueryStatement, so it is enough to traverse Query
        if (statement.getQueryStatement() != null) {
<<<<<<< HEAD
            visit(statement.getQueryStatement());
=======
            visit(statement.getQueryStatement(), context);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        return null;
    }

    @Override
    public R visitDeleteStatement(DeleteStmt statement, C context) {
        //Delete Statement after analyze, all information will be used to build QueryStatement, so it is enough to traverse Query
        if (statement.getQueryStatement() != null) {
<<<<<<< HEAD
            visit(statement.getQueryStatement());
=======
            visit(statement.getQueryStatement(), context);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        return null;
    }

    @Override
    public R visitSubmitTaskStatement(SubmitTaskStmt statement, C context) {
        if (statement.getInsertStmt() != null) {
<<<<<<< HEAD
            visit(statement.getInsertStmt());
        }
        if (statement.getCreateTableAsSelectStmt() != null) {
            visit(statement.getCreateTableAsSelectStmt());
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        return null;
    }

    @Override
    public R visitCreateTableAsSelectStatement(CreateTableAsSelectStmt statement, C context) {
        if (statement.getQueryStatement() != null) {
<<<<<<< HEAD
            visit(statement.getQueryStatement());
        }
        if (statement.getInsertStmt() != null) {
            visit(statement.getInsertStmt());
=======
            visit(statement.getQueryStatement(), context);
        }
        if (statement.getInsertStmt() != null) {
            visit(statement.getInsertStmt(), context);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        return null;
    }

    // ------------------------------------------- Relation ----------------------------------==------------------------

    @Override
    public R visitSelect(SelectRelation node, C context) {
        if (node.hasWithClause()) {
<<<<<<< HEAD
            node.getCteRelations().forEach(this::visit);
=======
            node.getCteRelations().forEach(x -> visit(x, context));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        if (node.getOrderBy() != null) {
            for (OrderByElement orderByElement : node.getOrderBy()) {
<<<<<<< HEAD
                visit(orderByElement.getExpr());
=======
                visit(orderByElement.getExpr(), context);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        }

        if (node.getOutputExpression() != null) {
<<<<<<< HEAD
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
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public R visitJoin(JoinRelation node, C context) {
        if (node.getOnPredicate() != null) {
<<<<<<< HEAD
            visit(node.getOnPredicate());
        }

        visit(node.getLeft());
        visit(node.getRight());
=======
            visit(node.getOnPredicate(), context);
        }

        visit(node.getLeft(), context);
        visit(node.getRight(), context);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return null;
    }

    @Override
    public R visitSubquery(SubqueryRelation node, C context) {
<<<<<<< HEAD
        return visit(node.getQueryStatement());
=======
        return visit(node.getQueryStatement(), context);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public R visitSetOp(SetOperationRelation node, C context) {
        if (node.hasWithClause()) {
<<<<<<< HEAD
            node.getCteRelations().forEach(this::visit);
        }
        node.getRelations().forEach(this::visit);
=======
            node.getCteRelations().forEach(x -> visit(x, context));
        }
        node.getRelations().forEach(x -> visit(x, context));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return null;
    }

    @Override
    public R visitCTE(CTERelation node, C context) {
<<<<<<< HEAD
        return visit(node.getCteQueryStatement());
=======
        return visit(node.getCteQueryStatement(), context);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public R visitView(ViewRelation node, C context) {
        return visit(node.getQueryStatement(), context);
    }

    // ------------------------------------------- Expression --------------------------------==------------------------

    @Override
    public R visitExpression(Expr node, C context) {
<<<<<<< HEAD
        node.getChildren().forEach(this::visit);
=======
        node.getChildren().forEach(x -> visit(x, context));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return null;
    }

    @Override
    public R visitSubquery(Subquery node, C context) {
<<<<<<< HEAD
        return visit(node.getQueryStatement());
=======
        return visit(node.getQueryStatement(), context);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }
}
