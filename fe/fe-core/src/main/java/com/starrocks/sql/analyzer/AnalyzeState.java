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

import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprId;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.common.IdGenerator;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;

import java.util.ArrayList;
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
    private Expr predicate;
    private Relation relation;
    private LimitElement limit;

    private List<Expr> groupBy;
    private List<List<Expr>> groupingSetsList;
    private List<FunctionCallExpr> aggregate;
    private Expr having;
    private List<Expr> groupingFunctionCallExprs;

    private List<OrderByElement> orderBy;
    private Scope orderScope;
    private List<Expr> orderSourceExpressions;

    /**
     * outputExprInOrderByScope is used to record which expressions in outputExpression are to be
     * recorded in the first level of OrderByScope (order by expressions can refer to columns in output)
     * Which columns satisfy the condition?
     * 1. An expression of type SlotRef.
     * When both tables t0 and t1 contain the v1 column, select t0.v1 from t0, t1 order by v1 will
     * refer to v1 in outputExpr instead of reporting an error.
     * 2. There is an aliased output expression
     */
    private List<Integer> outputExprInOrderByScope;

    private List<AnalyticExpr> outputAnalytic;
    private List<AnalyticExpr> orderByAnalytic;

    /**
     * Mapping of slotref to fieldid, used to determine
     * whether two expressions come from the same column
     */
    private final Map<Expr, FieldId> columnReferences = new HashMap<>();

    /**
     * Non-deterministic functions should be mapped multiple times in the project,
     * which requires different hashes for each non-deterministic function,
     * so in Expression Analyzer, each non-deterministic function will be numbered to achieve different hash values.
     */
    private final IdGenerator<ExprId> nondeterministicIdGenerator = ExprId.createGenerator();

    /**
     * Columns that do not appear in GroupBy for use with MODE_ONLY_FULL_GROUP_BY.
     */
    private final List<Expr> columnNotInGroupBy = new ArrayList<>();

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

    public SelectRelation build() {
        SelectRelation selectRelation = new SelectRelation(
                outputExpressions, isDistinct,
                orderScope, orderSourceExpressions,
                relation, predicate, limit,
                groupBy, aggregate, groupingSetsList, groupingFunctionCallExprs,
                orderBy, having,
                outputAnalytic, orderByAnalytic,
                columnReferences);
        selectRelation.setScope(new Scope(RelationId.of(selectRelation), outputScope.getRelationFields()));
        return selectRelation;
    }

    public Scope getOrderScope() {
        return orderScope;
    }

    public void setOrderScope(Scope orderScope) {
        this.orderScope = orderScope;
    }

    public List<Expr> getOrderSourceExpressions() {
        return orderSourceExpressions;
    }

    public void setOrderSourceExpressions(List<Expr> orderSourceExpressions) {
        this.orderSourceExpressions = orderSourceExpressions;
    }

    public List<Integer> getOutputExprInOrderByScope() {
        return outputExprInOrderByScope;
    }

    public void setOutputExprInOrderByScope(List<Integer> outputExprInOrderByScope) {
        this.outputExprInOrderByScope = outputExprInOrderByScope;
    }

    public List<Expr> getOutputExpressions() {
        return outputExpressions;
    }

    public void setOutputExpression(List<Expr> outputExpressions) {
        this.outputExpressions = outputExpressions;
    }

    public void setOutputScope(Scope outputScope) {
        this.outputScope = outputScope;
    }

    public boolean isDistinct() {
        return isDistinct;
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

    public Expr getPredicate() {
        return predicate;
    }

    public void setPredicate(Expr predicate) {
        this.predicate = predicate;
    }

    public LimitElement getLimit() {
        return limit;
    }

    public void setLimit(LimitElement limit) {
        this.limit = limit;
    }

    public List<Expr> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<Expr> groupBy) {
        this.groupBy = groupBy;
    }

    public List<List<Expr>> getGroupingSetsList() {
        return groupingSetsList;
    }

    public void setGroupingSetsList(List<List<Expr>> groupingSetsList) {
        this.groupingSetsList = groupingSetsList;
    }

    public List<Expr> getGroupingFunctionCallExprs() {
        return groupingFunctionCallExprs;
    }

    public void setGroupingFunctionCallExprs(List<Expr> groupingFunctionCallExprs) {
        this.groupingFunctionCallExprs = groupingFunctionCallExprs;
    }

    public List<FunctionCallExpr> getAggregate() {
        return aggregate;
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

    public List<OrderByElement> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(List<OrderByElement> orderBy) {
        this.orderBy = orderBy;
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

    public void setOrderByAnalytic(List<AnalyticExpr> orderByAnalytic) {
        this.orderByAnalytic = orderByAnalytic;
    }

    public ExprId getNextNondeterministicId() {
        return nondeterministicIdGenerator.getNextId();
    }

    public List<Expr> getColumnNotInGroupBy() {
        return columnNotInGroupBy;
    }
}
