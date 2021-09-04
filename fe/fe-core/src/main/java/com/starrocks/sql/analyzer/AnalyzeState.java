// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.sql.analyzer.relation.QuerySpecification;
import com.starrocks.sql.analyzer.relation.Relation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AnalyzeState is used to record some temporary variables that may be used during translate.
 * temporary variables for namespace resolution,
 * hierarchical relationship of namespace and with clause
 */
public class AnalyzeState {
    /**
     * Structure used to build QueryBlock
     */
    private List<Expr> outputExpressions;
    private Scope outputScope;
    private boolean isDistinct = false;
    private Scope orderScope;
    private Expr predicate;
    private Relation relation;
    private LimitElement limit;

    private List<Expr> groupBy;
    private List<List<Expr>> groupingSetsList;
    private List<FunctionCallExpr> aggregate;
    private Expr having;
    private List<Expr> groupingFunctionCallExprs;

    private List<OrderByElement> orderBy;
    private List<String> columnOutputNames;

    private List<Expr> orderSourceExpressions;

    private List<AnalyticExpr> outputAnalytic;
    private List<AnalyticExpr> orderByAnalytic;

    /**
     * Mapping of slotref to fieldid, used to determine
     * whether two expressions come from the same column
     */
    private final Map<Expr, FieldId> columnReferences = new HashMap<>();

    public AnalyzeState() {
    }

    public Scope getOutputScope() {
        return outputScope;
    }

    public void addColumnReference(Expr e, FieldId fieldId) {
        this.columnReferences.put(e, fieldId);
    }

    public Map<Expr, FieldId> getColumnReferences() {
        return columnReferences;
    }

    public QuerySpecification build() {
        return new QuerySpecification(
                outputExpressions, columnOutputNames, isDistinct,
                outputScope, orderScope, orderSourceExpressions,
                relation, predicate, limit,
                groupBy, aggregate, groupingSetsList, groupingFunctionCallExprs,
                orderBy, having,
                outputAnalytic, orderByAnalytic,
                columnReferences);
    }

    public void setOrderScope(Scope orderScope) {
        this.orderScope = orderScope;
    }

    public void setOrderSourceExpressions(List<Expr> orderSourceExpressions) {
        this.orderSourceExpressions = orderSourceExpressions;
    }

    public void setOutputExpression(List<Expr> outputExpressions) {
        this.outputExpressions = outputExpressions;
    }

    public void setOutputScope(Scope outputScope) {
        this.outputScope = outputScope;
    }

    public void setIsDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public void setRelation(Relation relation) {
        this.relation = relation;
    }

    public Relation getRelation() {
        return relation;
    }

    public void setPredicate(Expr predicate) {
        this.predicate = predicate;
    }

    public void setLimit(LimitElement limit) {
        this.limit = limit;
    }

    public void setGroupBy(List<Expr> groupBy) {
        this.groupBy = groupBy;
    }

    public void setGroupingSetsList(List<List<Expr>> groupingSetsList) {
        this.groupingSetsList = groupingSetsList;
    }

    public void setGroupingFunctionCallExprs(List<Expr> groupingFunctionCallExprs) {
        this.groupingFunctionCallExprs = groupingFunctionCallExprs;
    }

    public void setAggregate(List<FunctionCallExpr> aggregate) {
        this.aggregate = aggregate;
    }

    public void setHaving(Expr having) {
        this.having = having;
    }

    public Expr getHaving() {
        return having;
    }

    public void setOrderBy(List<OrderByElement> orderBy) {
        this.orderBy = orderBy;
    }

    public void setColumnOutputNames(List<String> columnOutputNames) {
        this.columnOutputNames = columnOutputNames;
    }

    public void setOutputAnalytic(List<AnalyticExpr> outputAnalytic) {
        this.outputAnalytic = outputAnalytic;
    }

    public void setOrderByAnalytic(List<AnalyticExpr> orderByAnalytic) {
        this.orderByAnalytic = orderByAnalytic;
    }
}
