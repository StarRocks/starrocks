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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.combinator.AggStateUtils;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CaseWhenClause;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.ExprSubstitutionVisitor;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.LiteralExprFactory;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import com.starrocks.type.Type;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

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
                                   RowIdStrategy rowIdStrategy,
                                   MaterializedView.RefreshMode currentRefreshMode) {
        public static IVMAnalyzeResult of(QueryStatement queryStatement, RowIdStrategy rowIdStrategy,
                                          MaterializedView.RefreshMode currentRefreshMode) {
            return new IVMAnalyzeResult(queryStatement, rowIdStrategy, currentRefreshMode);
        }
    }

    // table tables that supports IVM
    public static final Set<Table.TableType> SUPPORTED_TABLE_TYPES = Set.of(
            Table.TableType.ICEBERG
    );

    // join operators that supports IVM
    public static final Set<JoinOperator> IVM_SUPPORTED_JOIN_OPS = Set.of(
            JoinOperator.INNER_JOIN,
            JoinOperator.CROSS_JOIN
    );

    // Aggregate function whitelist: (function name) -> predicate(argument types).
    // Only (function, argument-type) combinations explicitly listed here are accepted by
    // IVMAnalyzer. Unlisted combinations are rejected at CREATE time so the user gets a
    // clear error instead of silently wrong data or a refresh-time crash. Each entry
    // documents the boundary verified by tests; widen the predicate (or add a new entry)
    // only after the (combinator metadata + BE state_union) path has been validated
    // end-to-end for that type.
    public static final Map<String, Predicate<Type[]>> IVM_SUPPORTED_AGG_FUNCTIONS =
            ImmutableMap.<String, Predicate<Type[]>>builder()
                    // count(*) and count(col) — all argument types are fine; refresh state is BIGINT.
                    .put(FunctionSet.COUNT,                  args -> args.length <= 1)
                    // sum: integer / float / DECIMAL.
                    .put(FunctionSet.SUM,                    args -> isFixedOrFloat(args[0]) || args[0].isDecimalV3())
                    // avg: integer / float / DECIMAL. DECIMAL was unblocked by #73012 which
                    // preserves the typed AggStateDesc on the state_union scalar so the
                    // intermediate (sum, count) tuple keeps its DECIMAL precision/scale.
                    .put(FunctionSet.AVG,                    args -> isFixedOrFloat(args[0]) || args[0].isDecimalV3())
                    // min/max: numeric, temporal, and string. VARCHAR was unblocked by #73095
                    // which relaxed state_union's strict type-equality precondition to accept
                    // compatible string types (VARCHAR(N) vs catalog-normalized VARCHAR(65533)).
                    .put(FunctionSet.MIN,                    args -> isFixedOrFloat(args[0]) || isTemporal(args[0])
                                                                    || args[0].isStringType())
                    .put(FunctionSet.MAX,                    args -> isFixedOrFloat(args[0]) || isTemporal(args[0])
                                                                    || args[0].isStringType())
                    // array_agg: accept single-arg only. ORDER BY in the source query inlines extra
                    // children via FunctionAnalyzer.getAdjustedAnalyzedAggregateFunction, so args.length
                    // exceeds 1 for `array_agg(col ORDER BY key)`. IVM state_union is unordered, so
                    // rejecting ORDER BY variants here keeps semantics safe.
                    .put(FunctionSet.ARRAY_AGG,              args -> args.length == 1)
                    // bool_or: associative OR over booleans, no state representation issues.
                    .put(FunctionSet.BOOL_OR,                args -> args.length == 1 && args[0].isBoolean())
                    // approx_count_distinct / ndv: HLL state union is well-defined.
                    .put(FunctionSet.APPROX_COUNT_DISTINCT,  args -> args.length == 1)
                    .put(FunctionSet.NDV,                    args -> args.length == 1)
                    .build();

    private static boolean isFixedOrFloat(Type t) {
        return t.isFixedPointType() || t.isFloatingPointType();
    }

    private static boolean isTemporal(Type t) {
        return t.isDate() || t.isDatetime();
    }

    private final ConnectContext connectContext;
    private final CreateMaterializedViewStatement statement;
    private final QueryStatement queryStatement;

    public IVMAnalyzer(ConnectContext connectContext,
                       CreateMaterializedViewStatement statement,
                       QueryStatement queryStatement) {
        this.connectContext = connectContext;
        this.statement = statement;
        this.queryStatement = queryStatement;
    }

    public static boolean isTableTypeIVMSupported(Table.TableType tableType) {
        if (SUPPORTED_TABLE_TYPES.contains(tableType)) {
            return true;
        }
        return false;
    }

    /**
     * Rewrite the mv defined query to incremental refresh query.
     * - Optional.empty: cannot be applied to incremental refresh.
     * - If incremental refresh is supported, the result must not be none.
     */
    public Optional<IVMAnalyzeResult> rewrite(MaterializedView.RefreshMode refreshMode) {
        if (!refreshMode.isIncremental() && !refreshMode.isAuto()) {
            return Optional.empty();
        }

        try {
            QueryRelation queryRelation = queryStatement.getQueryRelation();
            // Retractable queries produce their own __ROW_ID__ (encode(group_by_keys)); non-retractable
            // append-only scans rely on storage AUTO_INCREMENT.
            boolean isRetractable = rewriteImpl(queryRelation);
            RowIdStrategy strategy = isRetractable
                    ? RowIdStrategy.QUERY_COMPUTED
                    : RowIdStrategy.AUTO_INCREMENT;
            // Trial-rewrite catches drift the analyzer-level checks can't: e.g. a new logical
            // operator without a matching IvmDelta*Rule, or a combinator's metadata that no
            // longer matches the BE state-union path. INCREMENTAL only.
            if (refreshMode.isIncremental()) {
                IvmTrialRewriter.runTrial(connectContext, statement, queryStatement);
            }
            IVMAnalyzeResult result = IVMAnalyzeResult.of(queryStatement, strategy, refreshMode);
            return Optional.of(result);
        } catch (SemanticException e) {
            // Already has a self-describing message (rewriteImpl or IvmTrialRewriter). Don't re-wrap.
            if (refreshMode.isIncremental()) {
                throw e;
            }
            return Optional.empty();
        } catch (Exception e) {
            if (refreshMode.isIncremental()) {
                throw new SemanticException("Failed to rewrite the query for IVM: %s", e.getMessage());
            }
            return Optional.empty();
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
        if (unionRelation.getQualifier() != SetQualifier.ALL) {
            throw new SemanticException("IVMAnalyzer only supports UNION ALL, but got: %s",
                    unionRelation.getQualifier().toString());
        }
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
        if (CollectionUtils.isNotEmpty(selectRelation.getOutputAnalytic())) {
            throw new SemanticException("IVMAnalyzer does not support window functions, " +
                    "but got: %s", selectRelation.getOutputAnalytic());
        }
        if (CollectionUtils.isNotEmpty(selectRelation.getOrderBy()) ||
                CollectionUtils.isNotEmpty(selectRelation.getOrderByExpressions())) {
            throw new SemanticException("IVMAnalyzer does not support order by clause, " +
                    "but got: %s", selectRelation.getOrderBy());
        }
        boolean isRetractable = checkAggregate(selectRelation);
        Relation innerRelation = selectRelation.getRelation();
        isRetractable |= checkRelation(innerRelation);
        return isRetractable;
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
            if (checkRelation(joinRelation.getLeft())) {
                throw new SemanticException("IVMAnalyzer does not support with retractable left input, " +
                        "but got: %s", joinRelation.getLeft());
            }
            if (checkRelation(joinRelation.getRight())) {
                throw new SemanticException("IVMAnalyzer does not support with retractable right input, " +
                        "but got: %s", joinRelation.getRight());
            }
            // Iceberg tables are currently append-only (no delete files supported) and
            // only inner/cross joins are allowed — so this branch never produces
            // retractable output. Revisit when outer-join / delete-file support lands.
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
            TableRelation tableRelation = (TableRelation) relation;
            Table table = tableRelation.getTable();
            if (!SUPPORTED_TABLE_TYPES.contains(table.getType())) {
                throw new SemanticException("IVMAnalyzer does not support table type: %s", table.getType());
            }
            return false;
        }
    }

    private boolean checkAggregate(SelectRelation selectRelation) throws AnalysisException {
        List<FunctionCallExpr> aggregateExprs = selectRelation.getAggregate();
        if (CollectionUtils.isEmpty(aggregateExprs)) {
            return false;
        }

        List<Expr> groupByExprs = selectRelation.getGroupBy();
        if (CollectionUtils.isEmpty(groupByExprs)) {
            // If there are no group by expressions, we cannot apply IVM optimizations.
            throw new SemanticException("IVMAnalyzer requires group by expressions for incremental view maintenance.");
        }
        // new aggregate functions
        List<IVMAggFunctionInfo> newAggFuncInfos = Lists.newArrayList();
        ExprSubstitutionMap substitutionMap = new ExprSubstitutionMap();
        for (FunctionCallExpr aggFuncExpr : aggregateExprs) {
            // Distinct flag is dropped by the combinator rewrite below, so incremental
            // refresh would silently state_union plain state and produce wrong values.
            if (aggFuncExpr.isDistinct()) {
                throw new SemanticException(
                        "IVMAnalyzer does not support distinct aggregate functions, but got: %s",
                        aggFuncExpr.toString());
            }
            String aggFuncName = aggFuncExpr.getFunctionName();
            // Whitelist gate: only (function, argument-type) combinations validated end-to-end
            // are allowed. Unsupported combinations would either fail at refresh time or silently
            // produce wrong data; reject them here so the user sees a clear CREATE-time error.
            checkAggregateFunctionInWhitelist(aggFuncExpr, aggFuncName);
            // build intermediate aggregate function
            FunctionCallExpr intermediateAggFuncExpr = buildIntermediateAggregateFunc(aggFuncExpr);
            String newAggFuncName = IvmOpUtils.getIvmAggStateColumnName(aggFuncExpr);

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
        int encodeRowIdVersion = IvmOpUtils.deduceEncodeRowIdVersion(groupByExprs);
        if (statement != null) {
            statement.setEncodeRowIdVersion(encodeRowIdVersion);
        }
        FunctionCallExpr rowIdFuncExpr = IvmOpUtils.buildRowIdFuncExpr(encodeRowIdVersion, groupByExprs);
        SelectList selectList = selectRelation.getSelectList();
        List<SelectListItem> newItems = Lists.newArrayList();
        // add row_id func expr
        newItems.add(new SelectListItem(rowIdFuncExpr, IvmOpUtils.COLUMN_ROW_ID));
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
        return true;
    }

    private Expr substituteWithMap(Expr expr, ExprSubstitutionMap substitutionMap) {
        return ExprSubstitutionVisitor.rewrite(expr, substitutionMap);
    }

    private static void checkAggregateFunctionInWhitelist(FunctionCallExpr aggFuncExpr, String aggFuncName) {
        Predicate<Type[]> rule = IVM_SUPPORTED_AGG_FUNCTIONS.get(aggFuncName.toLowerCase());
        if (rule == null) {
            throw new SemanticException(
                    "IVMAnalyzer does not support aggregate function: %s. Supported functions: %s",
                    aggFuncName, IVM_SUPPORTED_AGG_FUNCTIONS.keySet());
        }
        Type[] argTypes = aggFuncExpr.getChildren().stream().map(Expr::getType).toArray(Type[]::new);
        if (!rule.test(argTypes)) {
            throw new SemanticException(
                    "IVMAnalyzer does not support %s with argument types %s",
                    aggFuncName, Arrays.toString(argTypes));
        }
    }

    public static MaterializedView.RefreshMode getRefreshMode(CreateMaterializedViewStatement statement) {
        Map<String, String> properties = statement.getProperties();
        if (properties == null) {
            properties = Maps.newHashMap();
            statement.setProperties(properties);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE)) {
            String mode = properties.get(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE);
            MaterializedView.RefreshMode parsed;
            try {
                parsed = MaterializedView.RefreshMode.valueOf(mode.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new SemanticException("Invalid refresh_mode: " + mode +
                        ". Only INCREMENTAL, PCT are supported.");
            }
            // AUTO is intentionally not exposed to users; the implementation is preserved
            // internally for future revival.
            if (parsed == MaterializedView.RefreshMode.AUTO) {
                throw new SemanticException("Invalid refresh_mode: " + mode +
                        ". Only INCREMENTAL, PCT are supported.");
            }
            return parsed;
        } else {
            return MaterializedView.RefreshMode.PCT;
        }
    }

    private FunctionCallExpr buildIntermediateAggregateFunc(FunctionCallExpr aggFuncExpr) {
        // <func>_combine(<args>)
        String aggFuncName = aggFuncExpr.getFunctionName();
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
            Expr defaultValue = LiteralExprFactory.createDefault(aggFunctionInfo.aggFunc.getType());
            CaseWhenClause caseWhenClause = new CaseWhenClause(isNullPredicate, defaultValue);
            CaseExpr caseExpr = new CaseExpr(null, Lists.newArrayList(caseWhenClause), aggStateMergeFunc);
            return caseExpr;
        } else {
            return aggStateMergeFunc;
        }
    }
}
