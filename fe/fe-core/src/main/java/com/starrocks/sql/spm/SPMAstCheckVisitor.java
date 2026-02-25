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

import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FieldReference;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.Subquery;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

/*
 * This class is used to compare two ASTs
 */
public class SPMAstCheckVisitor implements AstVisitorExtendInterface<Boolean, ParseNode> {
    protected static <T> T cast(ParseNode node) {
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
    public Boolean visitSubqueryRelation(SubqueryRelation node, ParseNode context) {
        SubqueryRelation other = cast(context);
        return check(node.getQueryStatement(), other.getQueryStatement());
    }

    @Override
    public Boolean visitOrderByElement(OrderByElement node, ParseNode context) {
        OrderByElement other = cast(context);
        boolean check = Objects.equals(node.getNullsFirstParam(), other.getNullsFirstParam());
        check = check && Objects.equals(node.getIsAsc(), other.getIsAsc());
        return check && check(node.getExpr(), other.getExpr());
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
        check = check && check(node.getOrderBy(), other.getOrderBy());
        return check;
    }

    @Override
    public Boolean visitSlot(SlotRef node, ParseNode node2) {
        if (node2 instanceof SlotRef other) {
            return node.equalsWithoutChild(other);
        } else if (node2 instanceof FieldReference other) {
            // field reference has none index, we just compare type here
            // and depend on the field reference to check the index
            return node.getType().equals(other.getType());
        }
        return false;
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
        if (node.getTable() != null && other.getTable() != null) {
            if (node.getTable().isNativeTable() && other.getTable().isNativeTable()) {
                return node.getTable().getId() == other.getTable().getId();
            } else {
                return StringUtils.equals(node.getTable().getTableIdentifier(), other.getTable().getTableIdentifier());
            }
        } else if (node.getTable() == null && other.getTable() == null) {
            return node.getName().equals(other.getName());
        }
        return false;
    }

    @Override
    public Boolean visitCTE(CTERelation stmt, ParseNode context) {
        CTERelation other = cast(context);
        return check(stmt.getCteQueryStatement(), other.getCteQueryStatement());
    }

    @Override
    public Boolean visitSetOp(SetOperationRelation node, ParseNode context) {
        SetOperationRelation other = cast(context);
        if (node.getQualifier() != other.getQualifier()) {
            return false;
        }
        return check(node.getRelations(), other.getRelations());
    }

    @Override
    public Boolean visitSubqueryExpr(Subquery node, ParseNode context) {
        Subquery other = cast(context);

        QueryStatement qs1 = skipEmptyRelation(node.getQueryStatement());
        QueryStatement qs2 = skipEmptyRelation(other.getQueryStatement());
        return check(qs1, qs2);
    }

    private QueryStatement skipEmptyRelation(QueryStatement query) {
        while ((query.getQueryRelation() instanceof SubqueryRelation qs)) {
            if (qs.hasOffset() || qs.hasLimit() || qs.hasWithClause()) {
                break;
            }
            query = qs.getQueryStatement();
        }
        return query;
    }

    @Override
    public Boolean visitAnalyticExpr(AnalyticExpr node, ParseNode node2) {
        AnalyticExpr other = cast(node2);
        boolean check = Objects.equals(node.getWindow(), other.getWindow());
        check = check && Objects.equals(node.getPartitionHint(), other.getPartitionHint());
        check = check && Objects.equals(node.getSkewHint(), other.getSkewHint());
        check = check && Objects.equals(node.isUseHashBasedPartition(), other.isUseHashBasedPartition());
        check = check && Objects.equals(node.isSkewed(), other.isSkewed());

        check = check && check(node.getFnCall(), other.getFnCall());
        check = check && check(node.getPartitionExprs(), other.getPartitionExprs());
        check = check && check(node.getOrderByElements(), other.getOrderByElements());
        check = check && check(node.getChildren(), ((Expr) node2).getChildren());
        return check;
    }

    @Override
    public Boolean visitExpression(Expr node, ParseNode node2) {
        if (!node.equalsWithoutChild(node2)) {
            return false;
        }
        return check(node.getChildren(), ((Expr) node2).getChildren());
    }
}