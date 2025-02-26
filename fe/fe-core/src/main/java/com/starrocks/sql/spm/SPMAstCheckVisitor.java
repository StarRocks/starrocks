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
import com.starrocks.analysis.Subquery;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;

import java.util.List;

/*
 * This class is used to compare two ASTs
 */
public class SPMAstCheckVisitor implements AstVisitor<Boolean, ParseNode> {
    private static <T> T cast(ParseNode node) {
        return (T) node;
    }

    public boolean check(List<? extends ParseNode> list1, List<? extends ParseNode> list2) {
        if (list1 == list2) {
            return true;
        }
        if (list1 == null || list2 == null) {
            return false;
        }
        if (list1.size() != list2.size()) {
            return false;
        }
        for (int i = 0; i < list1.size(); i++) {
            if (!check(list1.get(i), list2.get(i))) {
                return false;
            }
        }
        return true;
    }

    public boolean check(ParseNode node1, ParseNode node2) {
        if (node1 == node2) {
            return true;
        }
        if (node1 == null || node2 == null) {
            return false;
        }
        return visit(node1, node2);
    }

    @Override
    public Boolean visitQueryStatement(QueryStatement statement, ParseNode context) {
        QueryStatement other = cast(context);
        return check(statement.getQueryRelation(), other.getQueryRelation());
    }

    @Override
    public Boolean visitSubquery(SubqueryRelation node, ParseNode context) {
        SubqueryRelation other = cast(context);
        return check(node.getQueryStatement(), other.getQueryStatement());
    }

    @Override
    public Boolean visitSelect(SelectRelation node, ParseNode node2) {
        SelectRelation other = cast(node2);
        if (!check(node.getCteRelations(), other.getCteRelations())) {
            return false;
        }

        boolean check = check(node.getRelation(), other.getRelation());
        check = check && check(node.getOutputExpression(), other.getOutputExpression());
        check = check && check(node.getWhereClause(), other.getWhereClause());
        check = check && check(node.getGroupBy(), other.getGroupBy());
        check = check && check(node.getHaving(), other.getHaving());
        return check;
    }

    @Override
    public Boolean visitJoin(JoinRelation node, ParseNode context) {
        JoinRelation other = cast(context);
        if (!node.getJoinOp().equals(other.getJoinOp())) {
            return false;
        }
        boolean check = check(node.getLeft(), other.getLeft());
        check = check && check(node.getRight(), other.getRight());
        check = check && check(node.getOnPredicate(), other.getOnPredicate());
        return check;
    }

    @Override
    public Boolean visitTable(TableRelation node, ParseNode node2) {
        TableRelation other = cast(node2);
        return node.getTable().getId() == other.getTable().getId();
    }

    @Override
    public Boolean visitSubqueryExpr(Subquery node, ParseNode context) {
        Subquery other = cast(context);
        return check(node.getQueryStatement(), other.getQueryStatement());
    }

    @Override
    public Boolean visitExpression(Expr node, ParseNode node2) {
        if (!node.equalsWithoutChild(node2)) {
            return false;
        }
        return check(node.getChildren(), ((Expr) node2).getChildren());
    }
}