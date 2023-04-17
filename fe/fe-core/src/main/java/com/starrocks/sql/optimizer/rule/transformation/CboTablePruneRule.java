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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.CPJoinGardener;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CboTablePruneRule extends TransformationRule {
    public CboTablePruneRule() {
        super(RuleType.TF_CBO_TABLE_PRUNE_RULE,
                Pattern.create(OperatorType.LOGICAL_JOIN, OperatorType.LOGICAL_OLAP_SCAN,
                        OperatorType.LOGICAL_OLAP_SCAN));
    }

    private static final CboTablePruneRule INSTANCE = new CboTablePruneRule();

    public static CboTablePruneRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOp = input.getOp().cast();
        if (joinOp.getJoinType() != JoinOperator.INNER_JOIN &&
                joinOp.getJoinType() != JoinOperator.LEFT_OUTER_JOIN &&
                joinOp.getJoinType() != JoinOperator.RIGHT_OUTER_JOIN) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOp = input.getOp().cast();
        JoinOperator joinType = joinOp.getJoinType();
        OptExpression lhs = input.inputAt(0);
        OptExpression rhs = input.inputAt(1);
        LogicalOlapScanOperator lhsScanOp = input.inputAt(0).getOp().cast();
        LogicalOlapScanOperator rhsScanOp = input.inputAt(1).getOp().cast();
        Pair<List<BinaryPredicateOperator>, List<ScalarOperator>> onPredicates =
                JoinHelper.separateEqualPredicatesFromOthers(input);
        List<BinaryPredicateOperator> eqOnPredicates = onPredicates.first;
        List<ScalarOperator> otherOnPredicates = onPredicates.second;

        if (eqOnPredicates.isEmpty()) {
            return Collections.emptyList();
        }

        if (!otherOnPredicates.isEmpty() && !joinType.isInnerJoin()) {
            return Collections.emptyList();
        }

        Set<Pair<ColumnRefOperator, ColumnRefOperator>> eqColRefPairs = Sets.newHashSet();
        for (BinaryPredicateOperator eqPredicate : eqOnPredicates) {
            ColumnRefOperator leftCol = eqPredicate.getChild(0).cast();
            ColumnRefOperator rightCol = eqPredicate.getChild(1).cast();
            eqColRefPairs.add(Pair.create(leftCol, rightCol));
        }

        List<CPJoinGardener.CPBiRel> lhsToRhsBiRels =
                CPJoinGardener.getCardinalityPreserving(input.inputAt(0), input.inputAt(1), true);

        List<CPJoinGardener.CPBiRel> rhsToLhsBiRels =
                CPJoinGardener.getCardinalityPreserving(input.inputAt(1), input.inputAt(0), false);

        List<CPJoinGardener.CPBiRel> biRels = Collections.emptyList();

        if (joinType.isLeftOuterJoin()) {
            biRels = lhsToRhsBiRels;
        } else if (joinType.isRightOuterJoin()) {
            biRels = rhsToLhsBiRels;
        } else if (joinType.isInnerJoin()) {
            biRels = lhsToRhsBiRels;
            biRels.addAll(rhsToLhsBiRels);
        }
        if (biRels.isEmpty()) {
            return Collections.emptyList();
        }
        Set<Pair<ColumnRefOperator, ColumnRefOperator>> reverseEqColRefPairs =
                eqColRefPairs.stream().map(p -> Pair.create(p.second, p.first)).collect(Collectors.toSet());
        List<CPJoinGardener.CPBiRel> matchedBiRels =
                biRels.stream().filter(biRel -> biRel.isLeftToRight() ? biRel.getPairs().equals(eqColRefPairs) :
                        biRel.getPairs().equals(reverseEqColRefPairs)).collect(
                        Collectors.toList());
        if (matchedBiRels.isEmpty()) {
            return Collections.emptyList();
        }
        boolean sameTableJoinUK = false;
        boolean hasLeftToRightFK = false;
        boolean hasRightToLeftFK = false;
        for (CPJoinGardener.CPBiRel biRel : matchedBiRels) {
            if (!biRel.isFromForeignKey()) {
                sameTableJoinUK = true;
            } else {
                if (biRel.isLeftToRight()) {
                    hasLeftToRightFK = true;
                } else {
                    hasRightToLeftFK = true;
                }
            }
        }

        boolean mutualJoinOnFK = hasLeftToRightFK & hasRightToLeftFK;

        Optional<ScalarOperator> optOtherJoinOnPredicate = otherOnPredicates.isEmpty() ?
                Optional.empty() : Optional.of(Utils.compoundAnd(otherOnPredicates));

        if (sameTableJoinUK) {
            Map<Column, ColumnRefOperator> lhsColToColRef = lhsScanOp.getColumnMetaToColRefMap();
            Map<Column, ColumnRefOperator> rhsColToColRef = rhsScanOp.getColumnMetaToColRefMap();
            Map<ColumnRefOperator, ScalarOperator> rewriteMapping = Maps.newHashMap();
            for (Map.Entry<Column, ColumnRefOperator> e : lhsColToColRef.entrySet()) {
                ColumnRefOperator lhsColRef = e.getValue();
                ColumnRefOperator rhsColRef = rhsColToColRef.get(e.getKey());
                rewriteMapping.put(lhsColRef, rhsColRef);
            }

            BiMap<Column, ColumnRefOperator> biColToColRefMap = HashBiMap.create(rhsColToColRef);
            Map<ColumnRefOperator, Column> colRefToColMap = biColToColRefMap.inverse();
            Map<ColumnRefOperator, Column> newColRefToCols = lhsScanOp.getColRefToColumnMetaMap().keySet().stream()
                    .map(col -> (ColumnRefOperator) rewriteMapping.get(col))
                    .collect(Collectors.toMap(Function.identity(), colRefToColMap::get));
            newColRefToCols.putAll(rhsScanOp.getColRefToColumnMetaMap());
            return handleInnerJoin(input, rhs, lhs, rewriteMapping, Optional.of(newColRefToCols),
                    optOtherJoinOnPredicate);
        } else if (mutualJoinOnFK || joinType.isInnerJoin()) {
            Set<Pair<ColumnRefOperator, ColumnRefOperator>> colRefPairs =
                    hasLeftToRightFK ? reverseEqColRefPairs : eqColRefPairs;
            Map<ColumnRefOperator, ScalarOperator> rewriteMapping =
                    colRefPairs.stream().collect(Collectors.toMap(e -> e.first, e -> e.second));
            if (hasLeftToRightFK) {
                return handleInnerJoin(input, lhs, rhs, rewriteMapping, Optional.empty(), optOtherJoinOnPredicate);
            } else {
                return handleInnerJoin(input, rhs, lhs, rewriteMapping, Optional.empty(), optOtherJoinOnPredicate);
            }
        } else if (joinType.isLeftOuterJoin()) {
            return handleLeftOrRightJoin(input, lhs, rhs);
        } else if (joinType.isRightOuterJoin()) {
            return handleLeftOrRightJoin(input, rhs, lhs);
        }
        return Collections.emptyList();
    }

    List<OptExpression> handleInnerJoin(OptExpression joinOp, OptExpression retainOp, OptExpression pruneOp,
                                        Map<ColumnRefOperator, ScalarOperator> rewriteMapping,
                                        Optional<Map<ColumnRefOperator, Column>> newColRefToColumnMap,
                                        Optional<ScalarOperator> optOtherOnJoinPredicate) {

        ColumnRefSet rewriteColRefSet = new ColumnRefSet(rewriteMapping.keySet());
        // if any of used column refs of both predicate and output exprs of prune-side ScanOperator
        // can not be replaced by its retain-side column refs, then prune-side can not be pruned.
        if (!rewriteColRefSet.containsAll(pruneOp.getRowOutputInfo().getUsedColumnRefSet())) {
            return Collections.emptyList();
        }

        ScalarOperator prunePredicate =
                Optional.ofNullable(pruneOp.getOp().getPredicate()).orElse(ConstantOperator.TRUE);
        if (!rewriteColRefSet.containsAll(prunePredicate.getUsedColumns())) {
            return Collections.emptyList();
        }

        // rewrite prune-side column refs and merge them into retain-side.
        ReplaceColumnRefRewriter replacer = new ReplaceColumnRefRewriter(rewriteMapping, false);
        Map<ColumnRefOperator, ScalarOperator> scanColRefMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> retainColRefMap = retainOp.getRowOutputInfo().getColumnRefMap();
        Map<ColumnRefOperator, ScalarOperator> pruneColRefMap = pruneOp.getRowOutputInfo().getColumnRefMap();
        scanColRefMap.putAll(retainColRefMap);
        pruneColRefMap.forEach((k, v) -> scanColRefMap.put(k, replacer.rewrite(v)));

        // rewrite prune-side column refs and merge it into retain-side
        ScalarOperator retainPredicate =
                Optional.ofNullable(retainOp.getOp().getPredicate()).orElse(ConstantOperator.TRUE);
        ScalarOperator newPredicate = Utils.compoundAnd(retainPredicate, replacer.rewrite(prunePredicate));

        // Try to represent exprs of join's projection via column refs of retain-side ScanOperator.
        ReplaceColumnRefRewriter scanReplacer = new ReplaceColumnRefRewriter(scanColRefMap, false);
        Map<ColumnRefOperator, ScalarOperator> newColRefMap;
        if (joinOp.getOp().getProjection() != null) {
            Map<ColumnRefOperator, ScalarOperator> joinColRefMap = joinOp.getOp().getProjection().getColumnRefMap();
            newColRefMap = joinColRefMap.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> scanReplacer.rewrite(e.getValue())));
        } else {
            newColRefMap = scanColRefMap;
        }

        // Try to represent join's predicates via column refs of retain-side ScanOperator
        ScalarOperator joinPredicate = Optional.ofNullable(joinOp.getOp().getPredicate()).orElse(ConstantOperator.TRUE);
        ScalarOperator otherOnJoinPredicate = optOtherOnJoinPredicate.orElse(ConstantOperator.TRUE);
        joinPredicate = scanReplacer.rewrite(joinPredicate);
        otherOnJoinPredicate = scanReplacer.rewrite(otherOnJoinPredicate);

        newPredicate = new ScalarOperatorRewriter().rewrite(
                Utils.compoundAnd(newPredicate, joinPredicate, otherOnJoinPredicate),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);

        newPredicate = newPredicate.equals(ConstantOperator.TRUE) ? null : newPredicate;

        // create a new ScanOperator who unifies join operator, retain-side scan operator and
        // prune-side scan operators.
        Operator.Builder newOpBuilder = OperatorBuilderFactory.build(retainOp.getOp()).withOperator(retainOp.getOp())
                .setPredicate(newPredicate)
                .setProjection(new Projection(newColRefMap));

        // set new ColumnRefToColumnMap in case that when the same tables join on UK/PK
        newColRefToColumnMap.ifPresent(
                columnRefToColumnMap -> ((LogicalOlapScanOperator.Builder) newOpBuilder).setColRefToColumnMetaMap(
                        columnRefToColumnMap));
        Operator newScan = newOpBuilder.build();
        newScan.addSalt();
        return Collections.singletonList(OptExpression.create(newScan));
    }

    List<OptExpression> handleLeftOrRightJoin(OptExpression joinOpt, OptExpression retainOpt, OptExpression pruneOpt) {
        // if prune-side have a predicate, we do not pruned
        if (pruneOpt.getOp().getPredicate() != null) {
            return Collections.emptyList();
        }

        Optional<Projection> joinProjection = Optional.ofNullable(joinOpt.getOp().getProjection());
        ColumnRefSet usedColRefSet =
                joinProjection.map(Projection::getUsedColumns).orElse(joinOpt.getRowOutputInfo().getUsedColumnRefSet());

        // If not all of used columns of output exprs of join operator only references retain-side
        // column refs, prune-side ScanOperator can not be pruned.
        if (!retainOpt.getOutputColumns().containsAll(usedColRefSet)) {
            return Collections.emptyList();
        }

        // Try to represent output exprs of join operator in retain-side column refs
        Projection newProjection = null;
        if (joinProjection.isPresent()) {
            ReplaceColumnRefRewriter replacer =
                    new ReplaceColumnRefRewriter(retainOpt.getRowOutputInfo().getColumnRefMap(), false);
            Map<ColumnRefOperator, ScalarOperator> newColRefMap = Maps.newHashMap();
            joinProjection.get().getColumnRefMap().forEach((k, v) -> newColRefMap.put(k, replacer.rewrite(v)));
            newProjection = new Projection(newColRefMap);
        }

        Operator newScan = OperatorBuilderFactory.build(retainOpt.getOp()).withOperator(retainOpt.getOp())
                .setProjection(newProjection).build();
        newScan.addSalt();
        return Collections.singletonList(OptExpression.create(newScan));
    }
}
