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

package com.starrocks.sql.automv.qe;

import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.GroupByClause;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.ast.expression.Subquery;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class CollectAstVisitor implements AstVisitorExtendInterface<Void, Void> {
    private final AopAstHandler handler;

    private CollectAstVisitor(AopAstHandler handler) {
        this.handler = Objects.requireNonNull(handler);
    }

    public static CollectAstVisitor createCollectAstVisitor(AopAstHandler handler) {
        return new CollectAstVisitor(handler);
    }

    public static QueryStatementPlus collectFQTables(QueryStatement queryStatement,
                                                     ConnectContext context) {
        FQTableCollector collector = new FQTableCollector();
        CollectAstVisitor collectAstVisitor =
                CollectAstVisitor.createCollectAstVisitor(collector);
        collectAstVisitor.visit(queryStatement, null);
        return QueryStatementPlus.of(queryStatement, collector.getFQTableMap());
    }

    public static List<TableName> collectTableNames(QueryStatement queryStatement, ConnectContext context) {
        TableNameCollector collector = new TableNameCollector();
        CollectAstVisitor collectAstVisitor = CollectAstVisitor.createCollectAstVisitor(collector);
        collectAstVisitor.visit(queryStatement, null);
        return collector.getTableNames();
    }

    private Void visit(Collection<? extends ParseNode> nodes, Void context) {
        Optional.ofNullable(nodes)
                .ifPresent(nodeList -> nodeList.forEach(node -> visit(node, context)));
        return null;
    }

    @Override
    public Void visit(ParseNode node, Void context) {
        if (node == null) {
            return null;
        }
        try {
            handler.preProcess(node);
            return node.accept(this, context);
        } finally {
            handler.postProcess(node);
        }
    }

    @Override
    public Void visitNode(ParseNode node, Void context) {
        return visit(node, context);
    }

    @Override
    public Void visitQueryStatement(QueryStatement statement, Void context) {
        return statement.getQueryRelation().accept(this, context);
    }

    @Override
    public Void visitSelect(SelectRelation node, Void context) {
        visit(node.getCteRelations(), context);
        List<Expr> exprList = node.getSelectList().getItems().stream()
                .map(SelectListItem::getExpr)
                .collect(Collectors.toList());
        visit(exprList, context);
        visit(node.getRelation(), context);
        visit(node.getWhereClause(), context);
        visit(node.getGroupByClause(), context);
        visit(node.getHaving(), context);
        visit(node.getOrderBy(), context);
        visit(node.getLimit(), context);
        return null;
    }

    @Override
    public Void visitRelation(Relation node, Void context) {
        return null;
    }

    @Override
    public Void visitSubqueryRelation(SubqueryRelation node, Void context) {
        return visit(node.getQueryStatement(), context);
    }

    @Override
    public Void visitJoin(JoinRelation node, Void context) {
        visit(node.getLeft(), context);
        visit(node.getRight(), context);
        visit(node.getOnPredicate(), context);
        return null;
    }

    private Void visitSetOperationRelation(SetOperationRelation setRelation, Void context) {
        visit(setRelation.getCteRelations(), context);
        visit(setRelation.getOutputExpression(), context);
        visit(setRelation.getRelations(), context);
        visit(setRelation.getOrderBy(), context);
        visit(setRelation.getLimit(), context);
        return null;
    }

    @Override
    public Void visitUnion(UnionRelation node, Void context) {
        return visitSetOperationRelation(node, context);
    }

    @Override
    public Void visitExcept(ExceptRelation node, Void context) {
        return visitSetOperationRelation(node, context);
    }

    @Override
    public Void visitIntersect(IntersectRelation node, Void context) {
        return visitSetOperationRelation(node, context);
    }

    @Override
    public Void visitCTE(CTERelation node, Void context) {
        return visit(node.getCteQueryStatement(), context);
    }

    @Override
    public Void visitView(ViewRelation node, Void context) {
        return visit(node.getQueryStatement());
    }

    @Override
    public Void visitGroupByClause(GroupByClause node, Void context) {
        visit(node.getGroupingExprs(), context);
        Optional.ofNullable(node.getGroupingSetList())
                .ifPresent(groupSetList -> groupSetList.forEach(groupSet -> visit(groupSet, context)));
        return null;
    }

    @Override
    public Void visitOrderByElement(OrderByElement node, Void context) {
        visit(node.getExpr(), context);
        return null;
    }

    @Override
    public Void visitLimitElement(LimitElement node, Void context) {
        return null;
    }

    @Override
    public Void visitExpression(Expr node, Void context) {
        return visit(node.getChildren(), context);
    }

    @Override
    public Void visitSubqueryExpr(Subquery node, Void context) {
        return visit(node.getQueryStatement(), context);
    }
}
