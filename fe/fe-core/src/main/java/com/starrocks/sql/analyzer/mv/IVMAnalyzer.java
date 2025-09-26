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

package com.starrocks.sql.analyzer.mv;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.combinator.AggStateUtils;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CaseWhenClause;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOpUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This class is responsible for analyzing and rewriting the query statement for IVM (Incremental View Maintenance) refresh.
 */
public class IVMAnalyzer {
    public record IVMAggFunctionInfo(FunctionCallExpr aggFunc,
                                     String aggFuncName,
                                     FunctionCallExpr newAggFunc,
                                     String newAggFuncName) {
    }

    public record IVMAnalyzeResult(QueryStatement queryStatement,
                                   boolean needRetractableSink) {
        public static IVMAnalyzeResult of(QueryStatement queryStatement, boolean needRetractableSink) {
            return new IVMAnalyzeResult(queryStatement, needRetractableSink);
        }
    }

    private final ConnectContext connectContext;
    private final CreateMaterializedViewStatement statement;

    private boolean isNeedRetractableSink = false;

    public IVMAnalyzer(ConnectContext connectContext,
                       CreateMaterializedViewStatement statement) {
        this.connectContext = connectContext;
        this.statement = statement;
    }

    public static final Set<JoinOperator> IVM_SUPPORTED_JOIN_OPS = Set.of(
            JoinOperator.INNER_JOIN,
            JoinOperator.CROSS_JOIN
    );

    /**
     * Rewrite the mv defined query to incremental refresh query.
     * - Optional.empty: cannot be applied to incremental refresh.
     * - If incremental refresh is supported, the result must not be none.
     */
    public Optional<IVMAnalyzeResult> rewrite() {
        try {
            MaterializedView.RefreshMode refreshMode = getRefreshMode(statement);
            if (!refreshMode.isIncremental()) {
                return Optional.empty();
            }
            QueryStatement queryStatement = statement.getQueryStatement();
            QueryRelation queryRelation = queryStatement.getQueryRelation();
            rewriteImpl(queryRelation);
            IVMAnalyzeResult result = IVMAnalyzeResult.of(queryStatement, isNeedRetractableSink);
            return Optional.of(result);
        } catch (Exception e) {
            throw new SemanticException("Failed to rewrite the query for IVM: %s", e.getMessage());
        }
    }

    /**
     * Rewrite the query relation for incremental view maintenance.
     * NOTE: Only return non-empty Optional if the query relation has been rewritten, otherwise return empty Optional.
     */
    private boolean rewriteImpl(QueryRelation queryRelation) throws AnalysisException {
        if (queryRelation instanceof SelectRelation) {
            SelectRelation selectRelation = (SelectRelation) queryRelation;
            // For SelectRelation, we rewrite it to support incremental view maintenance.
            return checkSelectRelation(selectRelation);
        } else if (queryRelation instanceof SetOperationRelation) {
            return checkSetRelation((SetOperationRelation) queryRelation);
        } else if (queryRelation instanceof SubqueryRelation) {
            return checkSubqueryRelation((SubqueryRelation) queryRelation);
        } else {
            throw new SemanticException("IVMAnalyzer can only handle SelectRelation/UnionRelation, but got: %s",
                    queryRelation.getClass().getSimpleName());
        }
    }

    private boolean checkSubqueryRelation(SubqueryRelation subqueryRelation) throws AnalysisException {
        QueryStatement subQueryStatement = subqueryRelation.getQueryStatement();
        boolean isChildRetractable = rewriteImpl(subQueryStatement.getQueryRelation());
        if (isChildRetractable) {
            throw new SemanticException("IVMAnalyzer does not support subquery relation, " +
                    "but got: %s", subqueryRelation.getClass().getSimpleName());
        }
        return false;
    }

    private boolean checkSetRelation(SetOperationRelation setOperationRelation) throws AnalysisException {
        if (!(setOperationRelation instanceof UnionRelation)) {
            throw new SemanticException("IVMAnalyzer can only handle UnionRelation, " +
                    "but got: %s", setOperationRelation.getClass().getSimpleName());
        }
        UnionRelation unionRelation = (UnionRelation) setOperationRelation;
        // For UnionRelation, we only handle the case where all children are SelectRelation.
        List<QueryRelation> children = unionRelation.getRelations();
        for (QueryRelation child : children) {
            if (!(child instanceof SelectRelation)) {
                throw new SemanticException("IVMAnalyzer can only handle SelectRelation/UnionRelation, but got: %s",
                        child.getClass().getSimpleName());
            }
            SelectRelation selectChild = (SelectRelation) child;
            List<FunctionCallExpr> aggregateExprs = selectChild.getAggregate();
            if (CollectionUtils.isNotEmpty(aggregateExprs)) {
                throw new SemanticException("UnionRelation in IVMAnalyzer should not have aggregate functions, " +
                        "but got: %s", aggregateExprs);
            }
            boolean isChildRetractable = checkRelation(selectChild);
            if (isChildRetractable) {
                throw new SemanticException("IVMAnalyzer does not support UnionRelation with retractable sink, " +
                        "but got: %s", unionRelation.getClass().getSimpleName());
            }
        }
        return false;
    }

    private boolean checkSelectRelation(SelectRelation selectRelation) throws AnalysisException {
        boolean isRetractable = checkAggregate(selectRelation);
        Relation innerRelation = selectRelation.getRelation();
        isRetractable |= checkRelation(innerRelation);
        return isRetractable;
    }

    private void markRetractableSink() {
        if (hasMarkedRetractableSink()) {
            throw new SemanticException("IVMAnalyzer has already marked retractable sink, " +
                    "but got another mark request.");
        }
        this.isNeedRetractableSink = true;
    }

    private boolean hasMarkedRetractableSink() {
        return this.isNeedRetractableSink;
    }

    private boolean isRetractableJoin(JoinOperator joinType) {
        return !(joinType.isInnerJoin() || joinType.isCrossJoin() || joinType.isLeftSemiJoin());
    }

    private boolean checkRelation(Relation relation) throws AnalysisException {
        if (relation == null) {
            return false;
        }

        if (relation instanceof JoinRelation) {
            JoinRelation joinRelation = (JoinRelation) relation;
            JoinOperator joinType = joinRelation.getJoinOp();
            if (!IVM_SUPPORTED_JOIN_OPS.contains(joinType)) {
                throw new SemanticException("IVMAnalyzer does not support join type: %s", joinType);
            }
            if (isRetractableJoin(joinType)) {
                // only can support two tables with outer join, cannot support multi tables with outer join,
                if (hasMarkedRetractableSink()) {
                    throw new SemanticException("IVMAnalyzer does not support outer join with multiple tables, " +
                            "but got: %s", joinType);
                }
                markRetractableSink();
                return true;
            }
            return false;
        } else if (relation instanceof QueryRelation) {
            // If the inner relation is a QueryRelation, we need to rewrite it recursively.
            QueryRelation innerQueryRelation = (QueryRelation) relation;
            return rewriteImpl(innerQueryRelation);
        } else {
            if (!(relation instanceof TableRelation)) {
                // If the inner relation is not a JoinRelation or QueryRelation, we cannot handle it.
                throw new SemanticException("IVMAnalyzer does not support inner relation type: %s",
                        relation.getClass().getSimpleName());
            }
            return false;
        }
    }

    private boolean checkAggregate(SelectRelation selectRelation) throws AnalysisException {
        List<FunctionCallExpr> aggregateExprs = selectRelation.getAggregate();
        if (CollectionUtils.isEmpty(aggregateExprs)) {
            return false;
        }
        if (hasMarkedRetractableSink()) {
            throw new RuntimeException("IVMAnalyzer does not support aggregate functions with " +
                    "retractable input yet");
        }

        List<Expr> groupByExprs = selectRelation.getGroupBy();
        if (CollectionUtils.isEmpty(groupByExprs)) {
            // If there are no group by expressions, we cannot apply IVM optimizations.
            throw new SemanticException("IVMAnalyzer requires group by expressions for incremental view maintenance.");
        }
        // new aggregate functions
        List<IVMAggFunctionInfo> newAggFuncInfos = Lists.newArrayList();
        ExprSubstitutionMap substitutionMap = new ExprSubstitutionMap(false);
        for (FunctionCallExpr aggFuncExpr : aggregateExprs) {
            String aggFuncName = aggFuncExpr.getFnName().getFunction();
            // build intermediate aggregate function
            FunctionCallExpr intermediateAggFuncExpr = buildIntermediateAggregateFunc(aggFuncExpr);
            String newAggFuncName = TvrOpUtils.getTvrAggStateColumnName(aggFuncExpr);

            IVMAggFunctionInfo aggFunctionInfo = new IVMAggFunctionInfo(aggFuncExpr, aggFuncName,
                    intermediateAggFuncExpr, newAggFuncName);
            Expr stateMergeFuncExpr = buildStateMergeFuncExpr(aggFunctionInfo);

            newAggFuncInfos.add(aggFunctionInfo);
            substitutionMap.put(aggFuncExpr, stateMergeFuncExpr);
        }

        List<FunctionCallExpr> newAggFuncs = newAggFuncInfos.stream()
                .map(IVMAggFunctionInfo::newAggFunc)
                .toList();
        selectRelation.setAggregate(newAggFuncs);

        // Build the row ID function expression
        FunctionCallExpr rowIdFuncExpr = TvrOpUtils.buildRowIdFuncExpr(groupByExprs);
        SelectList selectList = selectRelation.getSelectList();
        List<SelectListItem> newItems = Lists.newArrayList();
        // add row_id func expr
        newItems.add(new SelectListItem(rowIdFuncExpr, TvrOpUtils.COLUMN_ROW_ID));
        // add original items
        selectList.getItems()
                .stream()
                .forEach(item -> {
                    Expr newExpr = substituteWithMap(item.getExpr().clone(), substitutionMap);
                    newItems.add(new SelectListItem(newExpr, item.getAlias()));
                });
        // add agg state func expr
        for (IVMAggFunctionInfo aggFunctionInfo : newAggFuncInfos) {
            newItems.add(new SelectListItem(aggFunctionInfo.newAggFunc, aggFunctionInfo.newAggFuncName));
        }
        selectList.setItems(newItems);

        List<Expr> newOutputExpressions = Lists.newArrayList();
        newOutputExpressions.add(rowIdFuncExpr);
        selectRelation.getOutputExpression()
                .stream()
                .forEach(expr -> {
                    Expr newExpr = substituteWithMap(expr.clone(), substitutionMap);
                    newOutputExpressions.add(newExpr);
                });
        // add extra exprs
        newAggFuncInfos.stream()
                .forEach(aggFunctionInfo -> newOutputExpressions.add(aggFunctionInfo.newAggFunc));
        selectRelation.setOutputExpr(newOutputExpressions);
        this.markRetractableSink();
        return true;
    }

    private Expr substituteWithMap(Expr expr, ExprSubstitutionMap substitutionMap) {
        return expr.substitute(substitutionMap);
    }

    private MaterializedView.RefreshMode getRefreshMode(CreateMaterializedViewStatement statement) {
        Map<String, String> properties = statement.getProperties();
        if (properties == null) {
            properties = Maps.newHashMap();
            statement.setProperties(properties);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE)) {
            String mode = properties.get(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE);
            return MaterializedView.RefreshMode.valueOf(mode.toUpperCase());
        } else {
            // Default to INCREMENTAL
            return MaterializedView.RefreshMode.defaultValue();
        }
    }

    private FunctionCallExpr buildIntermediateAggregateFunc(FunctionCallExpr aggFuncExpr) {
        // <func>_combine(<args>)
        String aggFuncName = aggFuncExpr.getFnName().getFunction();
        String aggStateFuncName = AggStateUtils.aggStateCombineFunctionName(aggFuncName);
        FunctionCallExpr aggStateFuncExpr = new FunctionCallExpr(aggStateFuncName, aggFuncExpr.getChildren());
        return aggStateFuncExpr;
    }

    private Expr buildStateMergeFuncExpr(IVMAggFunctionInfo aggFunctionInfo) throws AnalysisException {
        String aggFuncName = AggStateUtils.getAggFuncNameOfCombinator(aggFunctionInfo.aggFuncName);
        String stateMergeFuncName = AggStateUtils.stateMergeFunctionName(aggFuncName);
        SlotRef slotRef = new SlotRef(null, aggFunctionInfo.newAggFuncName);
        // <func>_state_merge(<slotRef>)
        FunctionCallExpr aggStateMergeFunc = new FunctionCallExpr(stateMergeFuncName, List.of(slotRef));
        // case when <aggStateMergeFunc> is null then <default_value> else <aggStateMergeFunc> end
        if (FunctionSet.isAlwaysReturnNonNullableFunction(aggFuncName)) {
            Expr isNullPredicate = new IsNullPredicate(aggStateMergeFunc, false);
            Expr defaultValue = LiteralExpr.createDefault(aggFunctionInfo.aggFunc.getType());
            CaseWhenClause caseWhenClause = new CaseWhenClause(isNullPredicate, defaultValue);
            CaseExpr caseExpr = new CaseExpr(null, Lists.newArrayList(caseWhenClause), aggStateMergeFunc);
            return caseExpr;
        } else {
            return aggStateMergeFunc;
        }
    }
}
