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
package com.starrocks.sql.optimizer.dump.generator;

import com.clearspring.analytics.util.Lists;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArraySliceExpr;
import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CloneExpr;
import com.starrocks.analysis.CollectionElementExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MultiInPredicate;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.connector.parser.trino.PlaceholderExpr;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DefaultValueExpr;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.LambdaArgument;
import com.starrocks.sql.ast.LambdaFunctionExpr;
import com.starrocks.sql.ast.MapExpr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

public class CollectLiteralVisitor extends AstVisitor<Void, CollectLiteralVisitor.Context> {
    public static final class Context {
        private final List<LiteralExpr> literalExprs = Lists.newArrayList();
    }

    private CollectLiteralVisitor() {
    }

    public static List<LiteralExpr> collect(ParseNode node) {
        Context context = new Context();
        new CollectLiteralVisitor().visit(node, context);
        return context.literalExprs;
    }

    @Override
    public Void visit(ParseNode node) {
        throw new UnsupportedOperationException("missing context");
    }

    @Override
    public Void visit(ParseNode node, Context context) {
        if (node != null) {
            return super.visit(node, context);
        }
        return null;
    }

    @Override
    public Void visitNode(ParseNode node, Context context) {
        throw new UnsupportedOperationException(String.format("Not yet support %s",
                node != null ? node.getClass().getSimpleName() : null));
    }

    @Override
    public Void visitQueryStatement(QueryStatement statement, Context context) {
        return visit(statement.getQueryRelation(), context);
    }

    @Override
    public Void visitCreateViewStatement(CreateViewStmt statement, Context context) {
        return visit(statement.getQueryStatement(), context);
    }

    @Override
    public Void visitInsertStatement(InsertStmt statement, Context context) {
        return visit(statement.getQueryStatement(), context);
    }

    @Override
    public Void visitSelect(SelectRelation node, Context context) {
        visit(node.getRelation(), context);
        visit(node.getGroupByClause(), context);
        visit(node.getHavingClause(), context);
        visit(node.getWhereClause(), context);
        if (CollectionUtils.isNotEmpty(node.getOrderBy())) {
            node.getOrderBy().forEach(sortNode -> visit(sortNode, context));
        }
        return null;
    }

    @Override
    public Void visitTable(TableRelation node, Context context) {
        return null;
    }

    @Override
    public Void visitJoin(JoinRelation node, Context context) {
        visit(node.getOnPredicate(), context);
        visit(node.getLeft(), context);
        visit(node.getRight(), context);
        return null;
    }

    @Override
    public Void visitSubquery(SubqueryRelation node, Context context) {
        visit(node.getQueryStatement().getQueryRelation(), context);
        if (CollectionUtils.isNotEmpty(node.getCteRelations())) {
            node.getCteRelations().forEach(cte -> visit(cte, context));
        }
        if (CollectionUtils.isNotEmpty(node.getOrderBy())) {
            node.getOrderBy().forEach(orderBy -> visit(orderBy, context));
        }
        return null;
    }

    @Override
    public Void visitSetOp(SetOperationRelation node, Context context) {
        if (CollectionUtils.isNotEmpty(node.getRelations())) {
            node.getRelations().forEach(relation -> visit(relation, context));
        }
        if (CollectionUtils.isNotEmpty(node.getCteRelations())) {
            node.getCteRelations().forEach(relation -> visit(relation, context));
        }
        return null;
    }

    @Override
    public Void visitUnion(UnionRelation node, Context context) {
        return visitSetOp(node, context);
    }

    @Override
    public Void visitExcept(ExceptRelation node, Context context) {
        return visitSetOp(node, context);
    }

    @Override
    public Void visitIntersect(IntersectRelation node, Context context) {
        return visitSetOp(node, context);
    }

    @Override
    public Void visitValues(ValuesRelation node, Context context) {
        return null;
    }

    @Override
    public Void visitTableFunction(TableFunctionRelation node, Context context) {
        return null;
    }

    @Override
    public Void visitCTE(CTERelation node, Context context) {
        return visit(node.getCteQueryStatement().getQueryRelation(), context);
    }

    @Override
    public Void visitView(ViewRelation node, Context context) {
        return visit(node.getQueryStatement().getQueryRelation(), context);
    }

    @Override
    public Void visitExpression(Expr node, Context context) {
        node.getChildren().forEach(child -> visit(child, context));
        return null;
    }

    @Override
    public Void visitArithmeticExpr(ArithmeticExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitAnalyticExpr(AnalyticExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitArrayExpr(ArrayExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitMapExpr(MapExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitCollectionElementExpr(CollectionElementExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitArraySliceExpr(ArraySliceExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitArrowExpr(ArrowExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitBetweenPredicate(BetweenPredicate node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitBinaryPredicate(BinaryPredicate node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitCaseWhenExpr(CaseExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitCastExpr(CastExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitCompoundPredicate(CompoundPredicate node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitDefaultValueExpr(DefaultValueExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitExistsPredicate(ExistsPredicate node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitFieldReference(FieldReference node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitFunctionCall(FunctionCallExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitGroupingFunctionCall(GroupingFunctionCallExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitInformationFunction(InformationFunction node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitInPredicate(InPredicate node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitMultiInPredicate(MultiInPredicate node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitIsNullPredicate(IsNullPredicate node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitLikePredicate(LikePredicate node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitLambdaFunctionExpr(LambdaFunctionExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitLambdaArguments(LambdaArgument node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitLiteral(LiteralExpr node, Context context) {
        context.literalExprs.add(node);
        return null;
    }

    @Override
    public Void visitSlot(SlotRef node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitSubfieldExpr(SubfieldExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitSubquery(Subquery node, Context context) {
        return visit(node.getQueryStatement().getQueryRelation(), context);
    }

    @Override
    public Void visitVariableExpr(VariableExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitTimestampArithmeticExpr(TimestampArithmeticExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitCloneExpr(CloneExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitPlaceholderExpr(PlaceholderExpr node, Context context) {
        return visitExpression(node, context);
    }

    @Override
    public Void visitLimitElement(LimitElement node, Context context) {
        return null;
    }

    @Override
    public Void visitOrderByElement(OrderByElement node, Context context) {
        return visitExpression(node.getExpr(), context);
    }

    @Override
    public Void visitGroupByClause(GroupByClause node, Context context) {
        if (CollectionUtils.isNotEmpty(node.getGroupingExprs())) {
            node.getGroupingExprs().forEach(expr -> visit(expr, context));
        }
        return null;
    }
}
