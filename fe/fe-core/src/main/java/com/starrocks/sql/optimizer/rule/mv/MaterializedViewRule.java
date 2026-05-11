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


package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorTypeReDeriver;
import com.starrocks.sql.optimizer.rewrite.TypeReDeriveException;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.MvRewriteOutputValidator;
import com.starrocks.sql.util.Box;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Select best materialized view for olap scan node
 */
public class MaterializedViewRule extends Rule {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewRule.class);

    // For Materialized View key columns, which could hit the following functions
    private static final ImmutableList<String> KEY_COLUMN_FUNCTION_NAMES = ImmutableList.of(
            FunctionSet.MAX,
            FunctionSet.MIN,
            FunctionSet.APPROX_COUNT_DISTINCT,
            FunctionSet.MULTI_DISTINCT_COUNT
    );

    public MaterializedViewRule() {
        super(RuleType.TF_MATERIALIZED_VIEW, Pattern.create(OperatorType.PATTERN));
    }

    // each table relation id -> set<column ids>
    private final Map<Integer, Set<Integer>> queryRelIdToColumnIdsInPredicates = Maps.newHashMap();
    // each table relation id -> set<group by column ids>
    private final Map<Integer, Set<Integer>> queryRelIdToGroupByIds = Maps.newHashMap();
    // each table relation id -> set<agg functions>
    private final Map<Integer, Set<CallOperator>> queryRelIdToAggFunctions = Maps.newHashMap();
    // each table relation id -> set<aggregate column ids>
    private final ColumnRefSet queryRelIdToAggregateIds = new ColumnRefSet();
    // each table relation id -> set<column name -> column ref id>
    private final Map<Integer, Set<Integer>> queryRelIdToScanNodeOutputColumnIds = Maps.newHashMap();
    // each table relation id -> map<output column ids>
    private final Map<Integer, Map<String, Integer>> queryRelIdToColumnNameIds = Maps.newHashMap();
    // each query scan operator
    private final List<LogicalOlapScanOperator> queryScanOperators = Lists.newArrayList();

    // record the relation id -> disableSPJGMV flag
    // this can be set to true when query has count(*) or count(1)
    private final Map<Integer, Boolean> queryRelIdToEnableMVRewrite = Maps.newHashMap();
    // record the relation id -> isSPJQuery flag
    private final Map<Integer, Boolean> queryRelIdToIsSPJQuery = Maps.newHashMap();
    // record the scan node relation id which has been accessed.
    private final Set<Integer> visitedQueryRelIds = Sets.newHashSet();

    // mv index id -> each RewriteContext
    private final Map<Long, List<RewriteContext>> mvIdToRewriteContexts = Maps.newHashMap();
    // current optimize context factory
    private ColumnRefFactory factory;

    private void init(OptExpression root) {
        collectAllPredicates(root);
        collectGroupByAndAggFunctions(root);
        collectScanOutputColumns(root);
        for (Integer scanId : visitedQueryRelIds) {
            if (!queryRelIdToIsSPJQuery.containsKey(scanId) && !queryRelIdToAggFunctions.containsKey(scanId) &&
                    !queryRelIdToGroupByIds.containsKey(scanId)) {
                queryRelIdToIsSPJQuery.put(scanId, true);
            }
            if (!queryRelIdToEnableMVRewrite.containsKey(scanId)) {
                queryRelIdToEnableMVRewrite.put(scanId, false);
            }
        }
    }

    private static class Collector extends OptExpressionVisitor<Void, Void> {
        private final Set<Box<OptExpression>> candidates = Sets.newHashSet();

        public Set<Box<OptExpression>> getCandidates() {
            return candidates;
        }

        // NOTE: LOGICAL_UNION is not supported since it needs to handle agg-push-down policy
        // which is not supported totally by the current rewrite framework which means we can
        // only rewrite union's child rather than rewrite union operator itself.
        private static final Set<OperatorType> SUPPORTED_OPERATOR_TYPES = ImmutableSet.of(
                OperatorType.LOGICAL_PROJECT,
                OperatorType.LOGICAL_FILTER,
                OperatorType.LOGICAL_JOIN,
                OperatorType.LOGICAL_AGGR,
                OperatorType.LOGICAL_REPEAT,
                OperatorType.LOGICAL_TABLE_FUNCTION,
                OperatorType.LOGICAL_OLAP_SCAN
        );

        private boolean isSupported(OptExpression input) {
            if (!SUPPORTED_OPERATOR_TYPES.contains(input.getOp().getOpType())) {
                return false;
            }
            return input.getInputs().stream().allMatch(child -> isSupported(child));
        }

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            if (isSupported(optExpression)) {
                candidates.add(Box.of(optExpression));
                return null;
            }
            for (OptExpression child : optExpression.getInputs()) {
                visit(child, context);
            }
            return null;
        }
    }

    public class Rewriter extends OptExpressionVisitor<OptExpression, Void> {
        private final OptimizerContext optimizerContext;
        private final Set<Box<OptExpression>> candidates;
        Rewriter(OptimizerContext optimizerContext, Set<Box<OptExpression>> candidates) {
            this.optimizerContext = optimizerContext;
            this.candidates = candidates;
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            if (candidates.contains(Box.of(optExpression))) {
                return doTransform(optExpression, optimizerContext);
            }

            for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
                optExpression.setChild(childIdx, visit(optExpression.inputAt(childIdx), context));
            }
            return OptExpression.create(optExpression.getOp(), optExpression.getInputs());
        }
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        Collector collector = new Collector();
        collector.visit(input, null);
        Set<Box<OptExpression>> candidates = collector.getCandidates();
        if (candidates.isEmpty()) {
            return Lists.newArrayList(input);
        }
        Rewriter rewriter = new Rewriter(context, candidates);
        return Lists.newArrayList(input.getOp().accept(rewriter, input, null));
    }

    private OptExpression doTransform(OptExpression input, OptimizerContext context) {
        this.factory = context.getColumnRefFactory();
        OptExpression optExpression = input;
        if (!isExistMVs(optExpression)) {
            return optExpression;
        }

        init(optExpression);

        if (queryScanOperators.stream().anyMatch(LogicalOlapScanOperator::hasTableHints)) {
            return optExpression;
        }

        for (LogicalOlapScanOperator scan : queryScanOperators) {
            int relationId = factory.getRelationId(scan.getOutputColumns().get(0).getId());
            // clear rewrite context since we are going to handle another scan operator.
            mvIdToRewriteContexts.clear();
            Map<Long, List<Column>> candidateIndexIdToSchema = selectValidMVs(scan, relationId);
            if (candidateIndexIdToSchema.isEmpty()) {
                continue;
            }

            long bestIndex = selectBestMV(scan, candidateIndexIdToSchema, relationId);
            if (bestIndex == scan.getSelectedIndexMetaId()) {
                continue;
            }
            BestIndexRewriter bestIndexRewriter = new BestIndexRewriter(scan);
            optExpression = bestIndexRewriter.rewrite(optExpression, bestIndex);

            // BestIndexRewriter post-pass — type widening propagation.
            //
            // After BestIndexRewriter picks the MV rollup, the scan is rebuilt with new
            // ColumnRef objects whose declared types may still be the original narrow
            // types (e.g. k3 SMALLINT) while the MV column is wider (e.g. BIGINT). The
            // Project/Agg operators above still hold OLD ColumnRef objects that have
            // not been updated yet.
            //
            // Two-step fix:
            //   1. Collect the colRefId→newType changes from the new scan's
            //      colRefToColumnMetaMap.
            //   2. Walk the ENTIRE tree (scan + Project + Agg) and patch every ColumnRef
            //      object with a matching ID (covering both the new scan colRefs and the
            //      old colRefs in expressions above). Then re-derive parent ScalarOperator
            //      types bottom-up via ScalarOperatorTypeReDeriver.
            //
            // This is COMPLEMENTARY to the rewrite-time substitution wired into
            // MaterializedViewRewriter.visitLogicalProject/Scan/Aggregate, NOT redundant
            // with it. The rewriter-side substitution handles RewriteContext-driven
            // query→MV column swaps for percentile/bitmap/hll rollups; the post-pass
            // here handles the BestIndexRewriter-induced ColumnRef type drift that
            // exists for ANY rollup with widening (sum SMALLINT→BIGINT being the
            // canonical case). Removing either mechanism re-introduces real bugs:
            //   - without this post-pass, sum(if(k2=0,k3,0)) reports type SMALLINT
            //     while the BE-side expression executes against BIGINT input — see
            //     issue #72799 / SyncMvRewriteTypeConsistencyTest;
            //   - without the rewriter-side substitution, percentile/bitmap rollup
            //     fn-family remapping never happens.
            Map<Integer, com.starrocks.type.Type> typeChanges = collectScanTypeChanges(optExpression, scan);
            if (!typeChanges.isEmpty()) {
                Set<ColumnRefOperator> widenedRefs = patchColRefTypesInTree(optExpression, typeChanges);
                if (!widenedRefs.isEmpty()) {
                    optExpression = reDeriveScalarTypes(optExpression, widenedRefs);
                }
            }

            if (mvIdToRewriteContexts.containsKey(bestIndex)) {
                List<RewriteContext> rewriteContext = mvIdToRewriteContexts.get(bestIndex);
                List<RewriteContext> percentileContexts = rewriteContext.stream().
                        filter(e -> e.aggCall.getFnName().equals(FunctionSet.PERCENTILE_APPROX))
                        .collect(Collectors.toList());
                if (!percentileContexts.isEmpty()) {
                    MVProjectAggProjectScanRewrite.getInstance().rewriteOptExpressionTree(
                            factory, relationId, optExpression, percentileContexts);
                }
                rewriteContext.removeAll(percentileContexts);

                MaterializedViewRewriter rewriter = new MaterializedViewRewriter();
                for (MaterializedViewRule.RewriteContext rc : rewriteContext) {
                    optExpression = rewriter.rewrite(optExpression, rc);
                }
                // Observability only: log loudly when the rewriter or the output
                // validator detects a problem, but DO NOT drop the candidate.
                // Silently rolling back a rewrite would mask a real bug AND would
                // cause a recall regression if the validator turns out to be too
                // strict — both worse outcomes than letting the candidate proceed
                // and surfacing the issue via PlanValidator / BE.
                String mvIdent = "indexId=" + bestIndex;
                if (rewriter.substitutionFailed()) {
                    LOG.error("MV rewrite substitution failed for {} — emitting plan anyway", mvIdent);
                }
                if (!MvRewriteOutputValidator.validate(optExpression, mvIdent)) {
                    LOG.error("MvRewriteOutputValidator rejected candidate for {} — emitting plan anyway " +
                            "(likely ReDeriver coverage gap or validator false-positive)", mvIdent);
                }
            }
        }
        return optExpression;
    }

    public static boolean isExistMVs(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            if (isExistMVs(child)) {
                return true;
            }
        }

        Operator operator = root.getOp();
        if (operator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) operator;
            OlapTable olapTable = (OlapTable) scanOperator.getTable();
            return olapTable.hasMaterializedView();
        }
        return false;
    }

    private Map<Long, List<Column>> selectValidMVs(LogicalOlapScanOperator scanOperator, int relationId) {
        OlapTable table = (OlapTable) scanOperator.getTable();

        List<MaterializedIndexMeta> candidateIndexIdToMeta = table.getVisibleIndexMetas();
        // column name -> column id
        Map<String, Integer> queryScanNodeColumnNameToIds = queryRelIdToColumnNameIds.get(relationId);

        Iterator<MaterializedIndexMeta> iterator = candidateIndexIdToMeta.iterator();
        // Consider all IndexMetas of the base table so can be decided by cost strategy for better performance.
        // eg:
        // tbl1     : <a int, b  string, c string> and column `a` is short key
        // tbl1_mv  : select b, a, c from tbl1 and column `b` is short key
        // Q1: select * from tbl1 where a = 1, should choose tbl1
        // Q2: select * from tbl1 where b = 'a', should choose tbl1_mv
        while (iterator.hasNext()) {
            MaterializedIndexMeta mvMeta = iterator.next();
            long mvIdx = mvMeta.getIndexMetaId();

            // Ignore indexes which cannot be remapping with query by column names.
            List<Column> mvNonAggregatedColumns = mvMeta.getNonAggregatedColumns();
            if (mvNonAggregatedColumns.stream().anyMatch(x -> !queryScanNodeColumnNameToIds.containsKey(x.getName()))) {
                iterator.remove();
                continue;
            }

            // Check whether contains complex expressions, MV with Complex expressions will be used
            // to rewrite query by AggregatedMaterializedViewRewriter.
            if (MVUtils.containComplexExpresses(mvMeta)) {
                iterator.remove();
                continue;
            }

            // Step2: check all columns in compensating predicates are available in the view output
            if (!checkCompensatingPredicates(queryScanNodeColumnNameToIds,
                    queryRelIdToColumnIdsInPredicates.get(relationId), mvMeta)) {
                iterator.remove();
                continue;
            }

            // Step3: group by list in query is the subset of group by list in view or view contains no aggregation
            if (!checkGrouping(queryScanNodeColumnNameToIds, queryRelIdToGroupByIds.get(relationId), mvMeta,
                    queryRelIdToEnableMVRewrite.get(relationId), queryRelIdToIsSPJQuery.get(relationId))) {
                iterator.remove();
                continue;
            }

            // Step4: aggregation functions are available in the view output
            if (!checkAggregationFunction(queryScanNodeColumnNameToIds, queryRelIdToAggFunctions.get(relationId),
                    mvIdx, mvMeta, queryRelIdToEnableMVRewrite.get(relationId), queryRelIdToIsSPJQuery.get(relationId))) {
                iterator.remove();
                continue;
            }

            // Step5: columns required to compute output expr are available in the view output
            if (!checkOutputColumns(queryScanNodeColumnNameToIds, queryRelIdToScanNodeOutputColumnIds.get(relationId),
                    mvIdx, mvMeta)) {
                iterator.remove();
            }
        }

        // Step6: if table type is aggregate and the candidateIndexIdToSchema is empty,
        if ((table.getKeysType() == KeysType.AGG_KEYS || table.getKeysType() == KeysType.UNIQUE_KEYS)
                && candidateIndexIdToMeta.size() == 0) {
            // the base index will be added in the candidateIndexIdToSchema.
            /*
             * In StarRocks, it is allowed that the aggregate table should be scanned directly
             * while there is no aggregation info in query.
             * For example:
             * Aggregate tableA: k1, k2, sum(v1)
             * Query: select k1, k2, v1 from tableA
             * Allowed
             * Result: same as select k1, k2, sum(v1) from tableA group by t1, t2
             *
             * However, the query should not be selector normally.
             * The reason is that the level of group by in tableA is upper then the level of group by in query.
             * So, we need to compensate those kinds of index in following step.
             *
             */
            compensateCandidateIndex(candidateIndexIdToMeta, table.getVisibleIndexMetas(),
                    table);

            iterator = candidateIndexIdToMeta.iterator();
            while (iterator.hasNext()) {
                MaterializedIndexMeta mvMeta = iterator.next();
                Long mvIdx = mvMeta.getIndexMetaId();

                if (!checkOutputColumns(queryRelIdToColumnNameIds.get(relationId),
                        queryRelIdToScanNodeOutputColumnIds.get(relationId), mvIdx, mvMeta)) {
                    iterator.remove();
                }
            }
        }

        Map<Long, List<Column>> result = Maps.newHashMap();
        for (MaterializedIndexMeta indexMeta : candidateIndexIdToMeta) {
            result.put(indexMeta.getIndexMetaId(), indexMeta.getSchema());
        }
        return result;
    }

    private Long selectBestMV(LogicalOlapScanOperator scanOperator,
                              Map<Long, List<Column>> candidateIndexIdToSchema,
                              int tableId) {
        // Step1: the candidate indexes that satisfies the most prefix index
        final Set<Integer> equivalenceColumns = Sets.newHashSet();
        final Set<Integer> unEquivalenceColumns = Sets.newHashSet();
        splitScanConjuncts(scanOperator, equivalenceColumns, unEquivalenceColumns);

        Set<Long> indexesMatchingBestPrefixIndex =
                matchBestPrefixIndex(
                        queryRelIdToColumnNameIds.get(tableId),
                        candidateIndexIdToSchema, equivalenceColumns, unEquivalenceColumns);

        // Step2: the best index that satisfies the least number of rows
        return selectBestRowCountIndex(indexesMatchingBestPrefixIndex, (OlapTable) scanOperator.getTable());
    }

    private void collectAllPredicates(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            collectAllPredicates(child);
        }

        Operator operator = root.getOp();
        if (operator instanceof LogicalAggregationOperator) {
            return;
        }
        LogicalOperator logicalOperator = (LogicalOperator) operator;
        if (logicalOperator.getPredicate() != null) {
            ScalarOperator scalarOperator = logicalOperator.getPredicate();
            if (!scalarOperator.isConstantRef()) {
                updateTableToColumns(scalarOperator, queryRelIdToColumnIdsInPredicates);
            }
        }
        if (logicalOperator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            if (joinOperator.getOnPredicate() != null) {
                List<ScalarOperator> conjuncts = Utils.extractConjuncts(joinOperator.getOnPredicate());
                for (ScalarOperator conjunct : conjuncts) {
                    updateTableToColumns(conjunct, queryRelIdToColumnIdsInPredicates);
                }
            }
        }
    }

    private void updateTableToColumns(ScalarOperator scalarOperator,
                                      Map<Integer, Set<Integer>> tableToColumns) {
        ColumnRefSet columns = scalarOperator.getUsedColumns();
        for (int columnId : columns.getColumnIds()) {
            int table = factory.getRelationId(columnId);
            if (table != -1) {
                tableToColumns.computeIfAbsent(table, k -> Sets.newHashSet()).add(columnId);
            }
        }
    }

    private void collectGroupByAndAggFunctions(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            collectGroupByAndAggFunctions(child);
        }

        Operator operator = root.getOp();
        if (operator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) operator;
            visitedQueryRelIds.add(factory.getRelationId(scanOperator.getOutputColumns().get(0).getId()));
        }

        if (operator instanceof LogicalAggregationOperator) {
            LogicalAggregationOperator aggOperator = (LogicalAggregationOperator) operator;
            Operator childOperator = root.getInputs().get(0).getOp();
            if (!(childOperator instanceof LogicalProjectOperator) &&
                    !(childOperator instanceof LogicalOlapScanOperator) &&
                    !(childOperator instanceof LogicalRepeatOperator)) {
                return;
            }

            if (childOperator instanceof LogicalProjectOperator) {
                LogicalProjectOperator projectOperator = (LogicalProjectOperator) childOperator;
                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projectOperator.getColumnRefMap());
                List<ScalarOperator> newGroupBys = Lists.newArrayList();
                for (ColumnRefOperator groupBy : aggOperator.getGroupingKeys()) {
                    newGroupBys.add(rewriter.rewrite(groupBy));
                }

                List<CallOperator> newAggs = Lists.newArrayList();
                for (CallOperator agg : aggOperator.getAggregations().values()) {
                    // Must forbidden count(*) or count(1) for materialized view.
                    if (agg.getChildren().size() < 1 ||
                            (agg.getChildren().size() == 1 && agg.getChild(0).isConstantRef())) {
                        disableSPJGMaterializedView();
                        break;
                    }
                    newAggs.add((CallOperator) rewriter.rewrite(agg));
                }
                collectGroupByAndAggFunction(newGroupBys, newAggs);
            } else {
                collectGroupByAndAggFunction(Lists.newArrayList(aggOperator.getGroupingKeys()),
                        Lists.newArrayList(aggOperator.getAggregations().values()));
            }
        }
    }

    // Disable SPJG materialized view for traced scan node which not has aggregation
    private void disableSPJGMaterializedView() {
        for (Integer scanId : visitedQueryRelIds) {
            if (!queryRelIdToGroupByIds.containsKey(scanId) && !queryRelIdToAggFunctions.containsKey(scanId)) {
                queryRelIdToEnableMVRewrite.put(scanId, true);
            }
        }
    }

    // set table is not SPJ query
    private void disableSPJQueries(int table) {
        if (table != -1) {
            queryRelIdToIsSPJQuery.put(table, false);
        } else {
            for (Integer scanId : visitedQueryRelIds) {
                if (!queryRelIdToIsSPJQuery.containsKey(scanId)) {
                    queryRelIdToIsSPJQuery.put(scanId, false);
                }
            }
        }
    }

    private void collectGroupByAndAggFunction(List<ScalarOperator> groupBys,
                                              List<CallOperator> aggs) {
        if (groupBys.stream().map(ScalarOperator::getUsedColumns).anyMatch(queryRelIdToAggregateIds::isIntersect) ||
                aggs.stream().map(ScalarOperator::getUsedColumns).anyMatch(queryRelIdToAggregateIds::isIntersect)) {
            // Has been collect from other aggregate, only check aggregate node which is closest to scan node
            return;
        }

        for (ScalarOperator groupBy : groupBys) {
            ColumnRefSet columns = groupBy.getUsedColumns();
            for (int columnId : columns.getColumnIds()) {
                int table = factory.getRelationId(columnId);
                if (table != -1) {
                    if (queryRelIdToGroupByIds.containsKey(table)) {
                        queryRelIdToGroupByIds.get(table).add(columnId);
                    } else {
                        queryRelIdToGroupByIds.put(table, Sets.newHashSet(columnId));
                    }
                }
                // This table has group by, disable isSPJQuery
                disableSPJQueries(table);
            }
        }

        for (CallOperator agg : aggs) {
            ColumnRefSet columns = agg.getUsedColumns();
            for (int columnId : columns.getColumnIds()) {
                int table = factory.getRelationId(columnId);
                if (table != -1) {
                    if (queryRelIdToAggFunctions.containsKey(table)) {
                        queryRelIdToAggFunctions.get(table).add(agg);
                    } else {
                        queryRelIdToAggFunctions.put(table, Sets.newHashSet(agg));
                    }
                }
                // This table has aggregation, disable isSPJQuery
                disableSPJQueries(table);
            }
        }

        groupBys.stream().map(ScalarOperator::getUsedColumns).forEach(queryRelIdToAggregateIds::union);
        aggs.stream().map(ScalarOperator::getUsedColumns).forEach(queryRelIdToAggregateIds::union);
    }

    // split scan operators' conjuncts into equal and non-equal column ids
    public void splitScanConjuncts(LogicalOlapScanOperator scanOperator,
                                   Set<Integer> equivalenceColumns,
                                   Set<Integer> unEquivalenceColumns) {
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(scanOperator.getPredicate());
        // 1. Get columns which has predicate on it.
        for (ScalarOperator operator : conjuncts) {
            if (!MVUtils.isPredicateUsedForPrefixIndex(operator)) {
                continue;
            }

            if (MVUtils.isEquivalencePredicate(operator)) {
                equivalenceColumns.add(operator.getUsedColumns().getFirstId());
            } else {
                unEquivalenceColumns.add(operator.getUsedColumns().getFirstId());
            }
        }

        // TODO(kks): 2. Equal join predicates when pushing inner child.
    }

    private void collectScanOutputColumns(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            collectScanOutputColumns(child);
        }

        Operator operator = root.getOp();
        if (operator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) operator;
            queryScanOperators.add(scanOperator);

            int tableId = factory.getRelationId(scanOperator.getOutputColumns().get(0).getId());
            scanOperator.getColumnMetaToColRefMap().entrySet()
                    .stream()
                    .forEach(x -> queryRelIdToColumnNameIds
                            .computeIfAbsent(tableId, k -> new CaseInsensitiveMap())
                            .put(x.getKey().getName(), x.getValue().getId()));
            scanOperator.getColRefToColumnMetaMap().keySet()
                    .stream()
                    .forEach(x -> updateTableToColumns(x, queryRelIdToScanNodeOutputColumnIds));
        }
    }

    private long selectBestRowCountIndex(Set<Long> indexesMatchingBestPrefixIndex, OlapTable olapTable) {
        long minRowCount = Long.MAX_VALUE;
        long selectedIndexMetaId = 0;
        long baseIndexMetaId = olapTable.getBaseIndexMetaId();
        for (Long indexMetaId : indexesMatchingBestPrefixIndex) {
            long rowCount = 0;
            for (Partition partition : olapTable.getPartitions()) {
                rowCount += partition.getDefaultPhysicalPartition().getLatestIndex(indexMetaId).getRowCount();
            }
            if (rowCount < minRowCount) {
                minRowCount = rowCount;
                selectedIndexMetaId = indexMetaId;
            } else if (rowCount == minRowCount) {
                // check column number, select one minimum column number
                int selectedColumnSize = olapTable.getSchemaByIndexMetaId(selectedIndexMetaId).size();
                int currColumnSize = olapTable.getSchemaByIndexMetaId(indexMetaId).size();
                // If indexId and old selectedIndexId both have the same rowCount and columnSize,
                // prefer non baseIndexId first.
                if (currColumnSize == selectedColumnSize) {
                    selectedIndexMetaId = (indexMetaId == baseIndexMetaId) ? selectedIndexMetaId : indexMetaId;
                } else if (currColumnSize < selectedColumnSize) {
                    selectedIndexMetaId = indexMetaId;
                }
            }
        }
        return selectedIndexMetaId;
    }

    // Map MV's column to query's column id.
    private int getMVColumnToQueryColumnId(Map<String, Integer> columnToIds, Column mvColumn) {
        // Assume sync mv's column names are mapping with query scan node's columns by column name.
        // We can find the according query column id by the mv column name.
        // Once mv's column name is not the same with scan node, how can we do it?
        List<SlotRef> baseColumnRefs = mvColumn.getRefColumns();
        // To be compatible with old policy, remove this later.
        if (baseColumnRefs == null) {
            return columnToIds.get(mvColumn.getName());
        }
        Preconditions.checkState(baseColumnRefs.size() == 1);
        return columnToIds.get(baseColumnRefs.get(0).getColumnName());
    }

    private Set<Long> matchBestPrefixIndex(
            Map<String, Integer> columnToIds,
            Map<Long, List<Column>> candidateIndexIdToSchema,
            Set<Integer> equivalenceColumns,
            Set<Integer> unEquivalenceColumns) {
        if (equivalenceColumns.size() == 0 && unEquivalenceColumns.size() == 0) {
            return candidateIndexIdToSchema.keySet();
        }

        Set<Long> indexesMatchingBestPrefixIndex = Sets.newHashSet();
        int maxPrefixMatchCount = 0;
        for (Map.Entry<Long, List<Column>> entry : candidateIndexIdToSchema.entrySet()) {
            long indexMetaId = entry.getKey();
            List<Column> indexSchema = entry.getValue();

            int prefixMatchCount = 0;
            for (Column col : indexSchema) {
                Integer columnId = getMVColumnToQueryColumnId(columnToIds, col);
                if (equivalenceColumns.contains(columnId)) {
                    prefixMatchCount++;
                } else if (unEquivalenceColumns.contains(columnId)) {
                    // UnEquivalence predicate's columns can only match first columns in rollup.
                    prefixMatchCount++;
                    break;
                } else {
                    break;
                }
            }

            if (prefixMatchCount == maxPrefixMatchCount) {
                indexesMatchingBestPrefixIndex.add(indexMetaId);
            } else if (prefixMatchCount > maxPrefixMatchCount) {
                maxPrefixMatchCount = prefixMatchCount;
                indexesMatchingBestPrefixIndex.clear();
                indexesMatchingBestPrefixIndex.add(indexMetaId);
            }
        }
        return indexesMatchingBestPrefixIndex;
    }

    private boolean checkCompensatingPredicates(
            Map<String, Integer> queryScanColumnNameToIds,
            Set<Integer> columnsInPredicates,
            MaterializedIndexMeta mvMeta) {
        // When the query statement does not contain any columns in predicates, all candidate index can pass this check
        if (columnsInPredicates == null) {
            return true;
        }

        List<Column> mvNonAggregatedColumns = mvMeta.getNonAggregatedColumns();
        Set<Integer> indexNonAggregatedColumns = Sets.newHashSet();
        mvNonAggregatedColumns
                .forEach(col -> indexNonAggregatedColumns.add(getMVColumnToQueryColumnId(queryScanColumnNameToIds, col)));
        if (!indexNonAggregatedColumns.containsAll(columnsInPredicates)) {
            return false;
        }
        return true;
    }

    /**
     * View      Query        result
     * SPJ       SPJG OR SPJ  pass
     * SPJG      SPJ          fail
     * SPJG      SPJG         pass
     * 1. grouping columns in query is subset of grouping columns in view
     * 2. the empty grouping columns in query is subset of all of views
     */
    private boolean checkGrouping(Map<String, Integer> queryScanColumnNameToIds,
                                  Set<Integer> queryGroupingIds,
                                  MaterializedIndexMeta mvMeta,
                                  boolean disableSPJGMV,
                                  boolean isSPJQuery) {
        List<Column> mvNonAggregatedColumns = mvMeta.getNonAggregatedColumns();
        // Remap mv's non aggregated columns to query based.
        Set<Integer> mvNonAggregatedColumnsBasedQuery = Sets.newHashSet();
        mvNonAggregatedColumns.forEach(column ->
                mvNonAggregatedColumnsBasedQuery.add(getMVColumnToQueryColumnId(queryScanColumnNameToIds, column)));

        // If there is no aggregated column in duplicate index, the index will be SPJ.
        // For example:
        //     duplicate table (k1, k2, v1)
        //     duplicate mv index (k1, v1)
        // When the candidate index is SPJ type, it passes the verification directly
        //
        // If there is no aggregated column in aggregate index, the index will be deduplicate index.
        // For example:
        //     duplicate table (k1, k2, v1 sum)
        //     aggregate mv index (k1, k2)
        // This kind of index is SPJG which same as select k1, k2 from aggregate_table group by k1, k2.
        // It also need to check the grouping column using following steps.
        //
        // ISSUE-3016, MaterializedViewFunctionTest: testDeduplicateQueryInAgg
        List<Column> mvMetaSchema = mvMeta.getSchema();
        if (mvNonAggregatedColumns.size() == mvMetaSchema.size()
                && mvMeta.getKeysType() == KeysType.DUP_KEYS) {
            return true;
        }
        // When the query is SPJ type but the candidate index is SPJG type, it will not pass directly.
        if (disableSPJGMV || isSPJQuery) {
            return false;
        }
        // The query is SPJG. The candidate index is SPJG too.
        // The grouping columns in query is empty. For example: select sum(A) from T
        if (queryGroupingIds == null) {
            return true;
        }
        // The grouping columns in query must be subset of the grouping columns in view
        if (!mvNonAggregatedColumnsBasedQuery.containsAll(queryGroupingIds)) {
            return false;
        }
        return true;
    }

    private boolean checkAggregationFunction(Map<String, Integer> columnToIds,
                                             Set<CallOperator> aggregatedColumnsInQueryOutput,
                                             Long mvIdx,
                                             MaterializedIndexMeta candidateIndexMeta,
                                             boolean disableSPJGMV, boolean isSPJQuery) {
        List<CallOperator> indexAggColumnExprList = mvAggColumnsToExprList(columnToIds, candidateIndexMeta);
        // When the candidate index is SPJ type, it passes the verification directly
        if (indexAggColumnExprList.size() == 0 && candidateIndexMeta.getKeysType() == KeysType.DUP_KEYS) {
            return true;
        }
        // When the query is SPJ type but the candidate index is SPJG type, it will not pass directly.
        if (disableSPJGMV || isSPJQuery) {
            return false;
        }
        // The query is SPJG. The candidate index is SPJG too.
        /* Situation1: The query is deduplicate SPJG when aggregatedColumnsInQueryOutput is null.
         * For example: select a , b from table group by a, b
         * The aggregation function check should be pass directly when MV is SPJG.
         */
        if (aggregatedColumnsInQueryOutput == null) {
            return true;
        }
        keyColumnsToExprList(columnToIds, candidateIndexMeta, indexAggColumnExprList);

        // The aggregated columns in query output must be subset of the aggregated columns in view
        if (!aggFunctionsMatchAggColumns(columnToIds, candidateIndexMeta, mvIdx,
                aggregatedColumnsInQueryOutput, indexAggColumnExprList)) {
            return false;
        }
        return true;
    }

    private boolean checkOutputColumns(
            Map<String, Integer> columnToIds,
            Set<Integer> columnNamesInQueryOutput,
            Long mvIdx,
            MaterializedIndexMeta mvMeta) {
        Preconditions.checkState(columnNamesInQueryOutput != null);
        if (mvIdToRewriteContexts.containsKey(mvIdx)) {
            return true;
        }
        Set<Integer> indexColumns = Sets.newHashSet();
        List<Column> candidateIndexSchema = mvMeta.getSchema();
        candidateIndexSchema.forEach(column -> indexColumns.add(getMVColumnToQueryColumnId(columnToIds, column)));

        // The columns in query output must be subset of the columns in SPJ view
        if (!indexColumns.containsAll(columnNamesInQueryOutput)) {
            return false;
        }
        return true;
    }

    private void compensateCandidateIndex(List<MaterializedIndexMeta> candidateIndexIdToMetas,
                                          List<MaterializedIndexMeta> allVisibleIndexes,
                                          OlapTable table) {
        int keySizeOfBaseIndex = table.getKeyColumnsByIndexMetaId(table.getBaseIndexMetaId()).size();
        for (MaterializedIndexMeta index : allVisibleIndexes) {
            long mvIndexMetaId = index.getIndexMetaId();
            if (table.getKeyColumnsByIndexMetaId(mvIndexMetaId).size() == keySizeOfBaseIndex) {
                candidateIndexIdToMetas.add(index);
            }
        }
    }

    private List<CallOperator> mvAggColumnsToExprList(Map<String, Integer> columnToIds,
                                                      MaterializedIndexMeta mvMeta) {
        List<CallOperator> result = Lists.newArrayList();
        List<Column> schema = mvMeta.getSchema();
        for (int i = 0; i < schema.size(); i++) {
            Column column = schema.get(i);
            if (column.isAggregated()) {
                ColumnRefOperator columnRef = factory.getColumnRef(columnToIds.get(column.getName()));
                CallOperator fn = new CallOperator(column.getAggregationType().name().toLowerCase(),
                        column.getType(),
                        Lists.newArrayList(columnRef));
                result.add(fn);
            }
        }
        return result;
    }

    private void keyColumnsToExprList(Map<String, Integer> columnToIds, MaterializedIndexMeta mvMeta,
                                      List<CallOperator> result) {
        for (Column column : mvMeta.getSchema()) {
            if (!column.isAggregated()) {
                int baseColumnId = getMVColumnToQueryColumnId(columnToIds, column);
                ColumnRefOperator columnRef = factory.getColumnRef(baseColumnId);
                for (String function : KEY_COLUMN_FUNCTION_NAMES) {
                    CallOperator fn = new CallOperator(function,
                            column.getType(),
                            Lists.newArrayList(columnRef));
                    result.add(fn);
                }
            }
        }
    }

    private boolean aggFunctionsMatchAggColumns(Map<String, Integer> columnToIds,
                                                MaterializedIndexMeta candidateIndexMeta,
                                                Long indexId, Set<CallOperator> queryExprList,
                                                List<CallOperator> mvColumnExprList) {
        ColumnRefSet aggregateColumns = new ColumnRefSet();
        ColumnRefSet keyColumns = new ColumnRefSet();
        Set<Integer> usedBaseColumnIds = Sets.newHashSet();
        for (Column column : candidateIndexMeta.getSchema()) {
            int baseColumnId = getMVColumnToQueryColumnId(columnToIds, column);
            usedBaseColumnIds.add(baseColumnId);
            ColumnRefOperator columnRef = factory.getColumnRef(baseColumnId);
            if (!column.isAggregated()) {
                keyColumns.union(columnRef);
            } else {
                aggregateColumns.union(columnRef);
            }
        }

        /*
        If all the columns in the query are key column and materialized view is AGG_KEYS.
        We should skip this query for given materialized view.
        ISSUE-6984: https://github.com/StarRocks/starrocks/issues/6984
        */
        if (candidateIndexMeta.getKeysType() == KeysType.AGG_KEYS && queryExprList.stream()
                .anyMatch(queryExpr -> {
                    String fnName = queryExpr.getFnName();
                    return (fnName.equals(FunctionSet.COUNT) || fnName.equals(FunctionSet.SUM))
                            && keyColumns.containsAll(queryExpr.getUsedColumns());
                })) {
            return false;
        }

        return queryExprList.stream()
                .allMatch(x -> canRewriteQueryAggFunc(x, mvColumnExprList, indexId,
                        keyColumns, aggregateColumns, usedBaseColumnIds));
    }

    private boolean canRewriteQueryAggFunc(CallOperator queryExpr,
                                           List<CallOperator> mvColumnExprList,
                                           Long indexId,
                                           ColumnRefSet keyColumns,
                                           ColumnRefSet aggregateColumns,
                                           Set<Integer> usedBaseColumnIds) {
        return mvColumnExprList.stream()
                .anyMatch(x -> isMVMatchAggFunctions(indexId, queryExpr, x, keyColumns,
                        aggregateColumns, usedBaseColumnIds));
    }

    private static final ImmutableSetMultimap<String, String> COLUMN_AGG_TYPE_MATCH_FN_NAME;

    static {
        ImmutableSetMultimap.Builder<String, String> builder = ImmutableSetMultimap.builder();
        builder.put(FunctionSet.SUM, FunctionSet.SUM);
        builder.put(FunctionSet.MAX, FunctionSet.MAX);
        builder.put(FunctionSet.MIN, FunctionSet.MIN);
        builder.put(FunctionSet.SUM, FunctionSet.COUNT);
        builder.put(FunctionSet.BITMAP_AGG, FunctionSet.BITMAP_UNION);
        builder.put(FunctionSet.BITMAP_AGG, FunctionSet.BITMAP_UNION_COUNT);
        builder.put(FunctionSet.BITMAP_AGG, FunctionSet.MULTI_DISTINCT_COUNT);
        builder.put(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_AGG);
        builder.put(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_UNION);
        builder.put(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_UNION_COUNT);
        builder.put(FunctionSet.BITMAP_UNION, FunctionSet.MULTI_DISTINCT_COUNT);
        builder.put(FunctionSet.HLL_UNION, FunctionSet.HLL_UNION_AGG);
        builder.put(FunctionSet.HLL_UNION, FunctionSet.HLL_UNION);
        builder.put(FunctionSet.HLL_UNION, FunctionSet.HLL_RAW_AGG);
        builder.put(FunctionSet.HLL_UNION, FunctionSet.NDV);
        builder.put(FunctionSet.HLL_UNION, FunctionSet.APPROX_COUNT_DISTINCT);
        builder.put(FunctionSet.PERCENTILE_UNION, FunctionSet.PERCENTILE_UNION);
        builder.put(FunctionSet.PERCENTILE_UNION, FunctionSet.PERCENTILE_APPROX);
        COLUMN_AGG_TYPE_MATCH_FN_NAME = builder.build();
    }

    private static Set<String> COUNT_DISTINCT_FUNCTION_NAMES = Sets.newHashSet(
            FunctionSet.BITMAP_AGG, FunctionSet.BITMAP_UNION, FunctionSet.HLL_UNION);

    private boolean isCountDistinctCandidateFunc(CallOperator mvColumnFn) {
        return mvColumnFn.isDistinct() || COUNT_DISTINCT_FUNCTION_NAMES.contains(mvColumnFn.getFnName());
    }

    /**
     * Returns true if {@code expr} contains a shape that is unsafe for MV rewrite:
     * subquery, lambda, or a nondeterministic scalar function.
     *
     * <p>This pre-screen is applied before the column-coverage check so that we
     * never broaden eligibility to expressions whose semantics cannot be reproduced
     * by simply projecting columns out of the MV.
     */
    private static boolean containsForbiddenShape(ScalarOperator expr) {
        if (expr instanceof SubqueryOperator) {
            return true;
        }
        if (expr instanceof LambdaFunctionOperator) {
            return true;
        }
        if (expr instanceof CallOperator) {
            CallOperator c = (CallOperator) expr;
            if (FunctionSet.allNonDeterministicFunctions.contains(c.getFnName())) {
                return true;
            }
        }
        for (ScalarOperator child : expr.getChildren()) {
            if (containsForbiddenShape(child)) {
                return true;
            }
        }
        return false;
    }

    public boolean isMVMatchAggFunctions(Long indexId, CallOperator queryFn,
                                         CallOperator mvColumnFn, ColumnRefSet keyColumns,
                                         ColumnRefSet aggregateColumns, Set<Integer> usedBaseColumnIds) {
        String queryFnName = queryFn.getFnName();
        if (queryFn.getFnName().equals(FunctionSet.COUNT) && queryFn.isDistinct()) {
            queryFnName = FunctionSet.MULTI_DISTINCT_COUNT;
        }

        if (!COLUMN_AGG_TYPE_MATCH_FN_NAME.get(mvColumnFn.getFnName())
                .contains(queryFnName)) {
            return false;
        }

        if (queryFn.getFnName().equals(FunctionSet.COUNT)) {
            // needs to check queryFn and mvColumnFn's distinct
            if (queryFn.isDistinct() != isCountDistinctCandidateFunc(mvColumnFn)) {
                return false;
            }
        } else if (queryFn.isDistinct() != mvColumnFn.isDistinct()) {
            return false;
        }
        ScalarOperator queryFnChild0 = queryFn.getChild(0);
        // select x1, sum(cast(x2 as tinyint)) from table;
        // sum(x2) result of materialized view maybe greater x2, like many x2 = 100, cast(part_sum(x2) as tinyint) maybe will
        // overflow, but x2(100) isn't overflow
        if (queryFnChild0 instanceof CastOperator && (queryFnChild0.getType().isDecimalOfAnyVersion() ||
                queryFnChild0.getType().isFloatingPointType() || (queryFnChild0.getChild(0).getType().isNumericType() &&
                queryFnChild0.getType().getTypeSize() >= queryFnChild0.getChild(0).getType().getTypeSize()))) {
            queryFnChild0 = queryFnChild0.getChild(0);
        }
        ScalarOperator mvColumnFnChild0 = mvColumnFn.getChild(0);

        if (!queryFnChild0.isColumnRef()) {
            // Reject expressions with forbidden shapes (subquery, lambda, nondeterministic functions).
            if (containsForbiddenShape(queryFnChild0)) {
                return false;
            }
            // Special-case bitmap_union / hll_union / etc.: when the MV column was defined
            // with an explicit function expression (e.g. to_bitmap(k)), verify the query
            // uses the same function name before falling through to column-coverage.
            ColumnRefOperator mvColumnRef = factory.getColumnRef(mvColumnFnChild0.getUsedColumns().getFirstId());
            Column mvColumn = factory.getColumn(mvColumnRef);
            if (mvColumn.getDefineExpr() != null && mvColumn.getDefineExpr() instanceof FunctionCallExpr &&
                    queryFnChild0 instanceof CallOperator) {
                CallOperator queryCall = (CallOperator) queryFnChild0;
                String mvName = ((FunctionCallExpr) mvColumn.getDefineExpr()).getFunctionName();
                String queryName = queryCall.getFnName();
                if (!mvName.equalsIgnoreCase(queryName)) {
                    return false;
                }
            }
            // Fall through to the column-coverage check below.
        }

        if (queryFnChild0.getUsedColumns().equals(mvColumnFnChild0.getUsedColumns())) {
            return true;
        }

        // Generalized column coverage: accept any expression whose leaf ColumnRefs
        // are all present in the MV's base column id set. Type/signature coherence
        // after rewriting is handled by ScalarOperatorTypeReDeriver +
        // MvRewriteOutputValidator, so the prior shape gating is no longer needed
        // for safety.
        if (!queryFnChild0.isColumnRef()) {
            int[] queryColumnIds = queryFnChild0.getUsedColumns().getColumnIds();
            Set<Integer> mvColumnIdSet = usedBaseColumnIds.stream()
                    .collect(Collectors.toSet());
            for (int queryColumnId : queryColumnIds) {
                if (!mvColumnIdSet.contains(queryColumnId)) {
                    return false;
                }
            }
            return true;
        } else {
            // Original direct ColumnRefOperator path — unchanged.
            ColumnRefOperator queryColumnRef = factory.getColumnRef(queryFnChild0.getUsedColumns().getFirstId());
            Column queryColumn = factory.getColumn(queryColumnRef);
            ColumnRefOperator mvColumnRef = factory.getColumnRef(mvColumnFnChild0.getUsedColumns().getFirstId());
            Column mvColumn = factory.getColumn(mvColumnRef);
            if (factory.getRelationId(queryColumnRef.getId()).equals(factory.getRelationId(mvColumnRef.getId()))) {
                if (mvColumn.getAggregationType() == null) {
                    return false;
                }
                String mvFuncName = mvColumn.getAggregationType().name().toLowerCase();
                if (queryFnName.equalsIgnoreCase(FunctionSet.COUNT) && mvColumn.getDefineExpr() instanceof CaseExpr) {
                    mvFuncName = FunctionSet.COUNT;
                }
                String mvColumnName = MVUtils.getMVAggColumnName(mvFuncName, queryColumn.getName());
                if (mvColumnName.equalsIgnoreCase(mvColumn.getName())) {
                    mvIdToRewriteContexts.computeIfAbsent(indexId, k -> Lists.newArrayList())
                            .add(new RewriteContext(queryFn, queryColumnRef, mvColumnRef, mvColumn));
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Walk the plan tree to find the updated scan (the one that was re-mapped to the MV
     * rollup by BestIndexRewriter) and collect the colRef ID → new-type mapping for all
     * columns whose type changed.
     *
     * <p>Because BestIndexRewriter rebuilds the scan operator it creates NEW ColumnRef
     * objects, while the Project/Agg operators above still hold references to the OLD
     * ColumnRef objects. We therefore cannot update types via the scan's colRefs alone.
     * Instead we collect a {@code Map<colRefId, newType>} here, then apply it to every
     * ColumnRef object reachable in the whole plan via {@link #patchColRefTypesInTree}.
     *
     * <p>This is needed for queries like {@code sum(if(k2=0, k3, 0))} where the IF
     * function metadata was resolved with k3's narrow type (SMALLINT) but the MV
     * column is wider (BIGINT). Without this patch the physical IF execution sees a
     * type mismatch.
     */
    private static Map<Integer, com.starrocks.type.Type> collectScanTypeChanges(
            OptExpression root, LogicalOlapScanOperator originalScan) {
        Map<Integer, com.starrocks.type.Type> changes = new HashMap<>();
        collectScanTypeChangesImpl(root, originalScan, changes);
        return changes;
    }

    private static void collectScanTypeChangesImpl(OptExpression node, LogicalOlapScanOperator originalScan,
                                                   Map<Integer, com.starrocks.type.Type> changes) {
        if (node.getOp() instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scan = (LogicalOlapScanOperator) node.getOp();
            // Match the scan that was rewritten: same table, different index.
            if (scan.getTable().equals(originalScan.getTable())
                    && scan.getSelectedIndexMetaId() != originalScan.getSelectedIndexMetaId()) {
                for (Map.Entry<ColumnRefOperator, Column> entry : scan.getColRefToColumnMetaMap().entrySet()) {
                    ColumnRefOperator colRef = entry.getKey();
                    Column mvCol = entry.getValue();
                    if (!mvCol.getType().equals(colRef.getType())) {
                        changes.put(colRef.getId(), mvCol.getType());
                    }
                }
            }
        }
        for (OptExpression child : node.getInputs()) {
            collectScanTypeChangesImpl(child, originalScan, changes);
        }
    }

    /**
     * Walk the entire plan tree and update in-place every {@link ColumnRefOperator}
     * whose ID is in {@code typeChanges}. This covers both the scan's own colRefs and
     * the (different object) colRefs embedded in Project/Agg expressions above it.
     *
     * @return the set of widened ColumnRefOperator objects (may include both the scan's
     *         colRefs and the colRefs from expressions higher up).
     */
    private static Set<ColumnRefOperator> patchColRefTypesInTree(
            OptExpression node, Map<Integer, com.starrocks.type.Type> typeChanges) {
        Set<ColumnRefOperator> widened = new HashSet<>();
        patchColRefTypesImpl(node, typeChanges, widened);
        return widened;
    }

    private static void patchColRefTypesImpl(OptExpression node, Map<Integer, com.starrocks.type.Type> typeChanges,
                                             Set<ColumnRefOperator> widened) {
        if (node.getOp() instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scan = (LogicalOlapScanOperator) node.getOp();
            for (ColumnRefOperator colRef : scan.getColRefToColumnMetaMap().keySet()) {
                com.starrocks.type.Type newType = typeChanges.get(colRef.getId());
                if (newType != null && !newType.equals(colRef.getType())) {
                    colRef.setType(newType);
                    widened.add(colRef);
                }
            }
        } else if (node.getOp() instanceof LogicalProjectOperator) {
            LogicalProjectOperator proj = (LogicalProjectOperator) node.getOp();
            patchColRefsInScalarMap(proj.getColumnRefMap(), typeChanges, widened);
        } else if (node.getOp() instanceof LogicalAggregationOperator) {
            LogicalAggregationOperator agg = (LogicalAggregationOperator) node.getOp();
            for (ColumnRefOperator groupKey : agg.getGroupingKeys()) {
                com.starrocks.type.Type newType = typeChanges.get(groupKey.getId());
                if (newType != null && !newType.equals(groupKey.getType())) {
                    groupKey.setType(newType);
                    widened.add(groupKey);
                }
            }
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : agg.getAggregations().entrySet()) {
                patchColRefsInScalar(entry.getValue(), typeChanges, widened);
            }
        }
        for (OptExpression child : node.getInputs()) {
            patchColRefTypesImpl(child, typeChanges, widened);
        }
    }

    /**
     * Walk each value in {@code map} and patch any embedded ColumnRefOperator whose ID
     * is in {@code typeChanges}. Also patches the key ColumnRefOperator if it has changed.
     */
    private static void patchColRefsInScalarMap(Map<ColumnRefOperator, ScalarOperator> map,
                                                Map<Integer, com.starrocks.type.Type> typeChanges,
                                                Set<ColumnRefOperator> widened) {
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : map.entrySet()) {
            patchColRefsInScalar(entry.getValue(), typeChanges, widened);
        }
    }

    /**
     * Recursively walk {@code op} and patch every embedded ColumnRefOperator.
     */
    private static void patchColRefsInScalar(ScalarOperator op,
                                             Map<Integer, com.starrocks.type.Type> typeChanges,
                                             Set<ColumnRefOperator> widened) {
        if (op instanceof ColumnRefOperator) {
            ColumnRefOperator ref = (ColumnRefOperator) op;
            com.starrocks.type.Type newType = typeChanges.get(ref.getId());
            if (newType != null && !newType.equals(ref.getType())) {
                ref.setType(newType);
                widened.add(ref);
            }
            return;
        }
        for (ScalarOperator child : op.getChildren()) {
            patchColRefsInScalar(child, typeChanges, widened);
        }
    }

    /**
     * Walk the entire plan tree and re-derive the types of scalar operators in
     * {@link LogicalProjectOperator} and {@link LogicalAggregationOperator} maps whose
     * used columns intersect {@code widenedRefs}.
     *
     * <p>For project operators, this operates in-place on the mutable columnRefMap.
     * For aggregation operators, a new operator is built (since aggregations is an
     * ImmutableMap) and a new OptExpression is returned for that subtree.
     * Re-derivation failures are logged and skipped — the type mismatch will surface
     * as a runtime error, but at least we don't crash the planner.
     *
     * @return the (possibly rebuilt) OptExpression for this node
     */
    private static OptExpression reDeriveScalarTypes(OptExpression node, Set<ColumnRefOperator> widenedRefs) {
        // First, recursively handle children (bottom-up).
        // Use a mutable copy so child re-derivations can propagate newly widened colRefs
        // upward to parent nodes. For example, when a Project re-derives
        // "if(k2=0, k3, 0)" and updates output colRef #5's type from SMALLINT to BIGINT,
        // #5 must be visible to the parent Agg that holds "sum(#5)".
        List<OptExpression> newChildren = null;
        List<OptExpression> inputs = node.getInputs();
        for (int i = 0; i < inputs.size(); i++) {
            OptExpression newChild = reDeriveScalarTypes(inputs.get(i), widenedRefs);
            if (newChild != inputs.get(i)) {
                if (newChildren == null) {
                    newChildren = new ArrayList<>(inputs);
                }
                newChildren.set(i, newChild);
            }
        }

        if (node.getOp() instanceof LogicalProjectOperator) {
            LogicalProjectOperator proj = (LogicalProjectOperator) node.getOp();
            Map<ColumnRefOperator, ScalarOperator> colRefMap = proj.getColumnRefMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : colRefMap.entrySet()) {
                ScalarOperator expr = entry.getValue();
                if (expr instanceof ColumnRefOperator) {
                    // Plain pass-through — type already patched by patchColRefTypesInTree.
                    // The output key's type is already up to date; no re-derivation needed.
                    continue;
                }
                if (intersects(expr.getUsedColumns(), widenedRefs)) {
                    try {
                        ScalarOperator redrived = ScalarOperatorTypeReDeriver.reDerive(expr);
                        entry.setValue(redrived);
                        // Sync the output colRef's type to the re-derived expression type,
                        // and add it to widenedRefs so parent operators (e.g. Agg holding
                        // sum(#outputRef)) also get re-derived in this same traversal.
                        ColumnRefOperator outputRef = entry.getKey();
                        if (!redrived.getType().equals(outputRef.getType())) {
                            outputRef.setType(redrived.getType());
                            outputRef.setNullable(redrived.isNullable());
                            widenedRefs.add(outputRef);
                        }
                    } catch (TypeReDeriveException e) {
                        LOG.warn("reDeriveScalarTypes: could not re-derive project expr after MV type widen: {}",
                                e.getMessage());
                    }
                }
            }
            if (newChildren != null) {
                return OptExpression.create(node.getOp(), newChildren);
            }
            return node;
        }

        if (node.getOp() instanceof LogicalAggregationOperator) {
            LogicalAggregationOperator agg = (LogicalAggregationOperator) node.getOp();
            Map<ColumnRefOperator, CallOperator> oldAggMap = agg.getAggregations();
            Map<ColumnRefOperator, CallOperator> newAggMap = null;
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : oldAggMap.entrySet()) {
                CallOperator aggFunc = entry.getValue();
                if (intersects(aggFunc.getUsedColumns(), widenedRefs)) {
                    try {
                        ScalarOperator redrived = ScalarOperatorTypeReDeriver.reDerive(aggFunc);
                        if (redrived instanceof CallOperator) {
                            if (newAggMap == null) {
                                newAggMap = new HashMap<>(oldAggMap);
                            }
                            CallOperator redrivedCall = (CallOperator) redrived;
                            newAggMap.put(entry.getKey(), redrivedCall);
                            // Sync the output colRef's type to the re-derived call type,
                            // and propagate upward in case this Agg is nested under another.
                            ColumnRefOperator outputRef = entry.getKey();
                            if (!redrivedCall.getType().equals(outputRef.getType())) {
                                outputRef.setType(redrivedCall.getType());
                                outputRef.setNullable(redrivedCall.isNullable());
                                widenedRefs.add(outputRef);
                            }
                        }
                    } catch (TypeReDeriveException e) {
                        LOG.warn("reDeriveScalarTypes: could not re-derive agg expr after MV type widen: {}",
                                e.getMessage());
                    }
                }
            }
            if (newAggMap != null || newChildren != null) {
                LogicalAggregationOperator newAggOp = LogicalAggregationOperator.builder()
                        .withOperator(agg)
                        .setAggregations(newAggMap != null ? newAggMap : oldAggMap)
                        .build();
                return OptExpression.create(newAggOp,
                        newChildren != null ? newChildren : node.getInputs());
            }
            return node;
        }

        // For all other operator types: if children changed, rebuild this node.
        if (newChildren != null) {
            return OptExpression.create(node.getOp(), newChildren);
        }
        return node;
    }

    private static boolean intersects(com.starrocks.sql.optimizer.base.ColumnRefSet used,
                                      Set<ColumnRefOperator> refs) {
        for (ColumnRefOperator ref : refs) {
            if (used.contains(ref)) {
                return true;
            }
        }
        return false;
    }

    public static class RewriteContext {
        ColumnRefOperator queryColumnRef;
        ColumnRefOperator mvColumnRef;
        Column mvColumn;
        CallOperator aggCall;

        RewriteContext(CallOperator aggCall,
                       ColumnRefOperator queryColumnRef,
                       ColumnRefOperator mvColumnRef,
                       Column mvColumn) {
            this.aggCall = aggCall;
            this.queryColumnRef = queryColumnRef;
            this.mvColumnRef = mvColumnRef;
            this.mvColumn = mvColumn;
        }
    }
}
