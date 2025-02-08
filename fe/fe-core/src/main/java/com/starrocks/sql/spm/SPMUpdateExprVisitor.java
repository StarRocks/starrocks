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

package com.starrocks.sql.spm;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.ValuesRelation;

import java.util.List;
import java.util.stream.Collectors;

public class SPMUpdateExprVisitor<C> implements AstVisitor<ParseNode, C> {
    private Expr visitExpr(Expr node) {
        if (node != null) {
            return (Expr) node.accept(this, null);
        }
        return null;
    }

    private List<Expr> visitExprList(List<Expr> nodes) {
        if (nodes != null && !nodes.isEmpty()) {
            return nodes.stream().map(this::visitExpr).collect(Collectors.toList());
        }
        return nodes;
    }

    @Override
    public ParseNode visitQueryStatement(QueryStatement statement, C context) {
        statement.getQueryRelation().accept(this, context);
        return statement;
    }

    @Override
    public ParseNode visitSelect(SelectRelation stmt, C context) {
        stmt.getCteRelations().forEach(this::visit);
        visit(stmt.getRelation());
        stmt.getSelectList().getItems().forEach(item -> item.setExpr(visitExpr(item.getExpr())));
        stmt.setOutputExpr(visitExprList(stmt.getOutputExpression()));
        stmt.setWhereClause(visitExpr(stmt.getWhereClause()));
        stmt.setGroupBy(visitExprList(stmt.getGroupBy()));
        stmt.setHaving(visitExpr(stmt.getHaving()));
        return stmt;
    }

    public ParseNode visitJoin(JoinRelation stmt, C context) {
        visit(stmt.getLeft());
        visit(stmt.getRight());
        visitExpr(stmt.getOnPredicate());
        return stmt;
    }

    @Override
    public ParseNode visitSubquery(SubqueryRelation stmt, C context) {
        return visit(stmt.getQueryStatement());
    }

    @Override
    public ParseNode visitSetOp(SetOperationRelation stmt, C context) {
        for (QueryRelation relation : stmt.getRelations()) {
            visit(relation);
        }
        return stmt;
    }

    @Override
    public ParseNode visitValues(ValuesRelation stmt, C context) {
        for (int i = 0; i < stmt.getRows().size(); i++) {
            List<Expr> row = stmt.getRow(i);
            stmt.getRows().set(i, visitExprList(row));
        }
        return stmt;
    }

    @Override
    public ParseNode visitCTE(CTERelation stmt, C context) {
        visit(stmt.getCteQueryStatement());
        return stmt;
    }

    @Override
    public ParseNode visitExpression(Expr node, C context) {
        if (node.getChildren() != null && !node.getChildren().isEmpty()) {
            for (int i = 0; i < node.getChildren().size(); i++) {
                node.setChild(i, visitExpr(node.getChild(i)));
            }
        }
        return node;
    }
}
