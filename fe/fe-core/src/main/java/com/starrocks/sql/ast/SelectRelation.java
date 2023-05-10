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

import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.FieldId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.parser.NodePosition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private Expr predicate;

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

    private List<Integer> outputExprInOrderByScope;

    /**
     * Relations referenced in From clause. The Relation can be a CTE/table
     * reference a subquery or two relation joined together.
     */
    private Relation relation;

    private Map<Expr, FieldId> columnReferences;

    /**
     *  materializeExpressionToColumnRef stores the mapping relationship
     *  between materialized expressions and materialized columns
     */
    private Map<Expr, SlotRef> generatedExprToColumnRef = new HashMap<>();

    public SelectRelation(
            SelectList selectList,
            Relation fromRelation,
            Expr predicate,
            GroupByClause groupByClause,
            Expr having) {
        this(selectList, fromRelation, predicate, groupByClause, having, NodePosition.ZERO);
    }

    public SelectRelation(
            SelectList selectList,
            Relation fromRelation,
            Expr predicate,
            GroupByClause groupByClause,
            Expr having, NodePosition pos) {
        super(pos);
        this.selectList = selectList;
        this.relation = fromRelation;
        this.predicate = predicate;
        this.groupByClause = groupByClause;
        this.having = having;
    }

    public SelectRelation(List<Expr> outputExpr, boolean isDistinct,
                          Scope orderScope, List<Expr> orderSourceExpressions,
                          Relation relation, Expr predicate, LimitElement limit,
                          List<Expr> groupBy, List<FunctionCallExpr> aggregate, List<List<Expr>> groupingSetsList,
                          List<Expr> groupingFunctionCallExprs,
                          List<OrderByElement> orderBy, Expr having,
                          List<AnalyticExpr> outputAnalytic, List<AnalyticExpr> orderByAnalytic,
                          Map<Expr, FieldId> columnReferences) {
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

    public void fillResolvedAST(AnalyzeState analyzeState) {
        this.outputExpr = analyzeState.getOutputExpressions();
        this.isDistinct = analyzeState.isDistinct();
        this.orderScope = analyzeState.getOrderScope();
        this.predicate = analyzeState.getPredicate();
        this.limit = analyzeState.getLimit();

        this.groupBy = analyzeState.getGroupBy();
        this.aggregate = analyzeState.getAggregate();
        this.groupingSetsList = analyzeState.getGroupingSetsList();
        this.having = analyzeState.getHaving();
        this.groupingFunctionCallExprs = analyzeState.getGroupingFunctionCallExprs();

        this.sortClause = analyzeState.getOrderBy();
        this.orderSourceExpressions = analyzeState.getOrderSourceExpressions();
        this.outputExprInOrderByScope = analyzeState.getOutputExprInOrderByScope();

        this.outputAnalytic = analyzeState.getOutputAnalytic();
        this.orderByAnalytic = analyzeState.getOrderByAnalytic();

        this.columnReferences = analyzeState.getColumnReferences();

        this.generatedExprToColumnRef = analyzeState.getGeneratedExprToColumnRef();

        this.setScope(analyzeState.getOutputScope());
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

    public void setAggregate(List<FunctionCallExpr> aggregate) {
        this.aggregate = aggregate;
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

    public void setGroupBy(List<Expr> groupBy) {
        this.groupBy = groupBy;
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

    public List<Integer> getOutputExprInOrderByScope() {
        return outputExprInOrderByScope;
    }

    public boolean hasAggregation() {
        return !(groupBy.isEmpty() && aggregate.isEmpty()) || isDistinct;
    }

    public List<AnalyticExpr> getOutputAnalytic() {
        return outputAnalytic;
    }

    public void setOutputAnalytic(List<AnalyticExpr> outputAnalytic) {
        this.outputAnalytic = outputAnalytic;
    }

    public List<AnalyticExpr> getOrderByAnalytic() {
        return orderByAnalytic;
    }

    public Map<Expr, FieldId> getColumnReferences() {
        return columnReferences;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSelect(this, context);
    }

    public void setOutputExpr(List<Expr> outputExpr) {
        this.outputExpr = outputExpr;
    }

    public SelectList getSelectList() {
        return selectList;
    }

    public boolean hasGroupByClause() {
        return groupByClause != null;
    }

    public void setIsDistinct(boolean distinct) {
        isDistinct = distinct;
    }

    public boolean hasWhereClause() {
        return predicate != null;
    }

    public void setWhereClause(Expr predicate) {
        this.predicate = predicate;
    }

    public Expr getWhereClause() {
        return predicate;
    }

    public GroupByClause getGroupByClause() {
        return groupByClause;
    }

    public boolean hasHavingClause() {
        return having != null;
    }

    public Expr getHavingClause() {
        return having;
    }

    public void setHaving(Expr having) {
        this.having = having;
    }

    public void setRelation(Relation relation) {
        this.relation = relation;
    }

    public void setColumnReferences(Map<Expr, FieldId> columnReferences) {
        this.columnReferences = columnReferences;
    }

    public void setOrderScope(Scope orderScope) {
        this.orderScope = orderScope;
    }

    public boolean hasAnalyticInfo() {
        return (outputAnalytic != null && outputAnalytic.size() > 0)
                || (orderByAnalytic != null && orderByAnalytic.size() > 0);
    }

    @Override
    public List<Expr> getOutputExpression() {
        return outputExpr;
    }

    public Map<Expr, SlotRef> getGeneratedExprToColumnRef() {
        return generatedExprToColumnRef;
    }
}
