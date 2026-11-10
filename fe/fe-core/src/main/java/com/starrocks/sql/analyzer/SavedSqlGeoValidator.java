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

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.GroupByClause;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.PivotRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.sql.ast.expression.NamedArgument;
import com.starrocks.sql.ast.expression.Parameter;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

public final class SavedSqlGeoValidator extends AstTraverser<Void, Void> {
    private final Set<ParseNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());

    private SavedSqlGeoValidator() {
    }

    // Validate at persistence boundaries because SQL rendering is also used for EXPLAIN, errors and dumps.
    public static void validate(ParseNode definition) {
        new SavedSqlGeoValidator().visit(definition, null);
    }

    @Override
    public Void visit(ParseNode node, Void context) {
        if (node == null || !visited.add(node)) {
            return null;
        }
        if (node instanceof Expr expr && expr.getType() != null && expr.getType().hasNestedGeoType()) {
            throw new SemanticException(
                    "Nested GEOGRAPHY/GEOMETRY types are not supported in saved SQL definitions", expr.getPos());
        }
        if (node instanceof QueryRelation relation && !(relation instanceof SelectRelation)) {
            relation.getCteRelations().forEach(cte -> visit(cte, context));
            if (relation.hasOrderByClause()) {
                relation.getOrderBy().forEach(orderBy -> visit(orderBy, context));
            }
        }
        return super.visit(node, context);
    }

    @Override
    public Void visitSelect(SelectRelation node, Void context) {
        if (node.getSelectList() != null) {
            node.getSelectList().getItems().forEach(item -> visit(item.getExpr(), context));
        }
        visit(node.getGroupByClause(), context);
        return super.visitSelect(node, context);
    }

    @Override
    public Void visitGroupByClause(GroupByClause node, Void context) {
        if (node.getGroupingSetList() != null) {
            node.getGroupingSetList().forEach(group -> group.forEach(expr -> visit(expr, context)));
        } else if (node.getOriGroupingExprs() != null) {
            node.getOriGroupingExprs().forEach(expr -> visit(expr, context));
        }
        return null;
    }

    @Override
    public Void visitOrderByElement(OrderByElement node, Void context) {
        return visit(node.getExpr(), context);
    }

    @Override
    public Void visitCTE(CTERelation node, Void context) {
        return node.isAnchor() ? visit(node.getCteQueryStatement(), context) : null;
    }

    @Override
    public Void visitView(ViewRelation node, Void context) {
        return null;
    }

    @Override
    public Void visitTable(TableRelation node, Void context) {
        if (node.getQueryPeriod() != null) {
            node.getQueryPeriod().getEnd().ifPresent(expr -> visit(expr, context));
        }
        return null;
    }

    @Override
    public Void visitSetOp(SetOperationRelation node, Void context) {
        node.getRelations().forEach(relation -> visit(relation, context));
        return null;
    }

    @Override
    public Void visitValues(ValuesRelation node, Void context) {
        node.getRows().forEach(row -> row.forEach(expr -> visit(expr, context)));
        return null;
    }

    @Override
    public Void visitTableFunction(TableFunctionRelation node, Void context) {
        List<Expr> args = node.getChildExpressions() != null ? node.getChildExpressions() : node.getFunctionParams().exprs();
        args.forEach(expr -> visit(expr, context));
        return null;
    }

    @Override
    public Void visitNormalizedTableFunction(NormalizedTableFunctionRelation node, Void context) {
        return visitJoin(node, context);
    }

    @Override
    public Void visitJoin(JoinRelation node, Void context) {
        if (node.getSkewColumn() != null) {
            visit(node.getSkewColumn(), context);
            if (node.getSkewValues() != null) {
                node.getSkewValues().forEach(expr -> visit(expr, context));
            }
        }
        return super.visitJoin(node, context);
    }

    @Override
    public Void visitPivotRelation(PivotRelation node, Void context) {
        visit(node.getQuery(), context);
        node.getAggregateFunctions().forEach(agg -> visit(agg.getFunctionCallExpr(), context));
        node.getPivotValues().forEach(value -> value.getExprs().forEach(expr -> visit(expr, context)));
        node.getRewrittenAggFunctions().forEach(expr -> visit(expr, context));
        return null;
    }

    @Override
    public Void visitFunctionCall(FunctionCallExpr node, Void context) {
        List<OrderByElement> orderBy = node.getParams().getOrderByElements();
        if (orderBy != null) {
            orderBy.forEach(element -> visit(element, context));
        }
        return super.visitFunctionCall(node, context);
    }

    @Override
    public Void visitAnalyticExpr(AnalyticExpr node, Void context) {
        visit(node.getFnCall(), context);
        node.getPartitionExprs().forEach(expr -> visit(expr, context));
        node.getOrderByElements().forEach(element -> visit(element, context));
        if (node.getWindow() != null) {
            visit(node.getWindow().getLeftBoundary().getExpr(), context);
            if (node.getWindow().getRightBoundary() != null) {
                visit(node.getWindow().getRightBoundary().getExpr(), context);
            }
        }
        return super.visitAnalyticExpr(node, context);
    }

    @Override
    public Void visitParameterExpr(Parameter node, Void context) {
        return visit(node.getExpr(), context);
    }

    @Override
    public Void visitNamedArgument(NamedArgument node, Void context) {
        return visit(node.getExpr(), context);
    }

    @Override
    public Void visitIntervalLiteral(IntervalLiteral node, Void context) {
        return visit(node.getValue(), context);
    }
}
