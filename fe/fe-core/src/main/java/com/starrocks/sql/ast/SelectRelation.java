// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SelectList;
import com.starrocks.sql.analyzer.FieldId;
import com.starrocks.sql.analyzer.Scope;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SelectRelation extends QueryRelation {
    /**
     * selectList is created by parser
     * and will be converted to outputExpr in Analyzer
     */
    private SelectList selectList;
    /**
     * out fields is different with output expr
     * output fields is represent externally visible resolved names
     * such as "k+1 as alias_name", k+1 is ArithmericExpr, alias_name is the output field
     */
    private List<Expr> outputExpr;

    private final Expr predicate;

    /**
     * groupByClause is created by parser
     * and will be converted to groupBy and groupingSetsList in Analyzer
     */
    private GroupByClause groupByClause;
    private List<Expr> groupBy;
    private List<FunctionCallExpr> aggregate;
    private List<List<Expr>> groupingSetsList;
    private Expr having;
    private List<Expr> groupingFunctionCallExprs;

    private boolean isDistinct;

    private List<AnalyticExpr> outputAnalytic;
    private List<AnalyticExpr> orderByAnalytic;

    private Scope orderScope;

    /**
     * order by expression resolve source expression,
     * column ref map will build in project operator when aggregation present
     */
    private List<Expr> orderSourceExpressions;

    /**
     * Relations referenced in From clause. The Relation can be a CTE/table
     * reference a subquery or two relation joined together.
     */
    private final Relation relation;

    private Map<Expr, FieldId> columnReferences;

    public SelectRelation(
            SelectList selectList,
            Relation fromRelation,
            Expr predicate,
            GroupByClause groupByClause,
            Expr having) {
        super(null);
        this.selectList = selectList;
        this.relation = fromRelation;
        this.predicate = predicate;
        this.groupByClause = groupByClause;
        this.having = having;
    }


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

