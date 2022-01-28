// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.sql.analyzer.FieldId;
import com.starrocks.sql.analyzer.Scope;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SelectRelation extends QueryRelation {
    private final Expr predicate;

    private final List<Expr> groupBy;
    private final List<FunctionCallExpr> aggregate;
    private final List<List<Expr>> groupingSetsList;
    private final Expr having;
    private final List<Expr> groupingFunctionCallExprs;

    private final List<OrderByElement> sortClause;
    private LimitElement limit;

    private final boolean isDistinct;

    private final List<AnalyticExpr> outputAnalytic;
    private final List<AnalyticExpr> orderByAnalytic;

    private final Scope orderScope;

    /**
     * out fields is different with output expr
     * output fields is represent externally visible resolved names
     * such as "k+1 as alias_name", k+1 is ArithmericExpr, alias_name is the output field
     */
    private List<Expr> outputExpr;

    /**
     * order by expression resolve source expression,
     * column ref map will build in project operator when aggregation present
     */
    private final List<Expr> orderSourceExpressions;

    /**
     * Relations referenced in From clause. The Relation can be a CTE/table
     * reference a subquery or two relation joined together.
     */
    private final Relation relation;

    private Map<Expr, FieldId> columnReferences;

    public SelectRelation(List<Expr> outputExpr, List<String> columnOutputNames, boolean isDistinct,
                          Scope orderScope, List<Expr> orderSourceExpressions,
                          Relation relation, Expr predicate, LimitElement limit,
                          List<Expr> groupBy, List<FunctionCallExpr> aggregate, List<List<Expr>> groupingSetsList,
                          List<Expr> groupingFunctionCallExprs,
                          List<OrderByElement> orderBy, Expr having,
                          List<AnalyticExpr> outputAnalytic, List<AnalyticExpr> orderByAnalytic,
                          Map<Expr, FieldId> columnReferences) {
        super(columnOutputNames);

        this.outputExpr = outputExpr;
        this.isDistinct = isDistinct;
        this.orderScope = orderScope;
        this.relation = relation;
        this.predicate = predicate;
        this.limit = limit;

        this.groupBy = groupBy;
        this.aggregate = aggregate;
        this.groupingSetsList = groupingSetsList;
        this.having = having;
        this.groupingFunctionCallExprs = groupingFunctionCallExprs;

        this.sortClause = orderBy;
        this.orderSourceExpressions = orderSourceExpressions;

        this.outputAnalytic = outputAnalytic;
        this.orderByAnalytic = orderByAnalytic;

        this.columnReferences = columnReferences;
    }

    public List<Expr> getOutputExpr() {
        return outputExpr;
    }

    public Expr getPredicate() {
        return predicate;
    }

    public Expr getHaving() {
        return having;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public LimitElement getLimit() {
        return limit;
    }

    public void setLimit(LimitElement limit) {
        this.limit = limit;
    }

    public boolean hasLimit() {
        return limit != null;
    }

    public List<FunctionCallExpr> getAggregate() {
        return aggregate;
    }

    public List<List<Expr>> getGroupingSetsList() {
        return groupingSetsList;
    }

    public List<Expr> getGroupingFunctionCallExprs() {
        return groupingFunctionCallExprs;
    }

    public List<Expr> getGroupBy() {
        return groupBy;
    }

    public List<OrderByElement> getOrderBy() {
        return sortClause;
    }

    public List<Expr> getOrderByExpressions() {
        return sortClause.stream().map(OrderByElement::getExpr).collect(Collectors.toList());
    }

    public Relation getRelation() {
        return relation;
    }

    public Scope getOrderScope() {
        return orderScope;
    }

    public List<Expr> getOrderSourceExpressions() {
        return orderSourceExpressions;
    }

    public boolean hasAggregation() {
        return !(groupBy.isEmpty() && aggregate.isEmpty());
    }

    public boolean hasOrderBy() {
        return !sortClause.isEmpty();
    }

    public void clearOrder() {
        sortClause.clear();
    }

    public List<AnalyticExpr> getOutputAnalytic() {
        return outputAnalytic;
    }

    public List<AnalyticExpr> getOrderByAnalytic() {
        return orderByAnalytic;
    }

    public Map<Expr, FieldId> getColumnReferences() {
        return columnReferences;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSelect(this, context);
    }
}

