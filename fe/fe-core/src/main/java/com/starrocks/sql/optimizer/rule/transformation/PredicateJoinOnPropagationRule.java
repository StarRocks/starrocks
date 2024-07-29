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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarEquivalenceExtractor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class PredicateJoinOnPropagationRule extends TransformationRule {
    public PredicateJoinOnPropagationRule() {
        super(RuleType.TF_PREDICATE_JOIN_ON_PROPAGATION, Pattern.create(OperatorType.PATTERN_LEAF)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN)
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        OptExpression joinOptExpression = input.getInputs().get(0);
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) joinOptExpression.getOp();
        if (!joinOperator.getJoinType().isInnerJoin()) {
            return false;
        }

        List<BinaryPredicateOperator> joinOnPredicates = getJoinOnEqPredicate(joinOptExpression);
        List<ColumnRefOperator> joinOnColumnRefs = getJoinOnColumn(joinOptExpression);
        Collection<ScalarOperator> filterPredicateSets = getFilterPredicateSets(input);
        if (checkJoinChild(joinOptExpression.getInputs().get(0), joinOperator, joinOnPredicates,
                joinOnColumnRefs, filterPredicateSets)) {
            return true;
        }
        if (checkJoinChild(joinOptExpression.getInputs().get(1), joinOperator, joinOnPredicates,
                joinOnColumnRefs, filterPredicateSets)) {
            return true;
        }
        return false;
    }

    private List<ColumnRefOperator> getJoinOnColumn(OptExpression joinOptExpression) {
        List<BinaryPredicateOperator> joinOnEqPredicateList = getJoinOnEqPredicate(joinOptExpression);
        List<ColumnRefOperator> joinOnColumnRefs = Lists.newLinkedList();
        for (BinaryPredicateOperator eqPredicate : joinOnEqPredicateList) {
            joinOnColumnRefs.addAll(Utils.extractColumnRef(eqPredicate));
        }
        return joinOnColumnRefs;
    }

    private List<BinaryPredicateOperator> getJoinOnEqPredicate(OptExpression joinOptExpression) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) joinOptExpression.getOp();
        ScalarOperator joinOnPredicate = joinOperator.getOnPredicate();
        List<ScalarOperator> joinOnPredicateList = Utils.extractConjuncts(joinOnPredicate);
        List<BinaryPredicateOperator> joinOnConjunctsList = JoinHelper.getEqualsPredicate(
                joinOptExpression.inputAt(0).getOutputColumns(),
                joinOptExpression.inputAt(1).getOutputColumns(), joinOnPredicateList);
        return joinOnConjunctsList;
    }

    private Collection<ScalarOperator> getFilterPredicateSets(OptExpression input) {
        Collection<ScalarOperator> filterPredicateSets = Sets.newHashSet();
        if (input.getOp().getOpType() == OperatorType.LOGICAL_FILTER) {
            ScalarOperator filterPredicate = input.getOp().getPredicate();
            filterPredicateSets = Utils.extractConjuncts(filterPredicate);
        }
        return filterPredicateSets;
    }

    private boolean checkJoinChild(
            OptExpression childOptExpression, LogicalJoinOperator joinOperator, List<BinaryPredicateOperator> joinOnPredicates,
            List<ColumnRefOperator> joinOnColumnRefs, Collection<ScalarOperator> filterPredicateSet) {
        OptExpression offspringFilter = getOffspringFilter(childOptExpression);
        if (offspringFilter != null) {
            ScalarOperator offspringFilterPredicate = offspringFilter.getOp().getPredicate();
            ScalarOperator compoundPredicate = Utils.compoundAnd(joinOperator.getOnPredicate(),
                    offspringFilterPredicate, joinOperator.getPredicate());
            List<ScalarOperator> compoundPredicateList = Utils.extractConjuncts(compoundPredicate);
            ScalarEquivalenceExtractor scalarEquivalenceExtractor = new ScalarEquivalenceExtractor();
            scalarEquivalenceExtractor.union(compoundPredicateList);
            for (ColumnRefOperator joinOnColumnRef : joinOnColumnRefs) {
                for (ScalarOperator so : scalarEquivalenceExtractor.getEquivalentScalar(joinOnColumnRef)) {
                    boolean isAdditionPredicate = !so.getOpType().equals(OperatorType.IS_NULL) &&
                            !isInJoinPredicate(joinOnPredicates, so) && !filterPredicateSet.contains(so);
                    if (isAdditionPredicate) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private OptExpression getOffspringFilter(OptExpression optExpression) {
        if (optExpression.getInputs().size() == 1) {
            OptExpression subOptExpression = optExpression.getInputs().get(0);
            if (subOptExpression.getOp().getOpType() == OperatorType.LOGICAL_FILTER) {
                return subOptExpression;
            } else if (subOptExpression.getOp().getOpType() == OperatorType.LOGICAL_PROJECT) {
                if (subOptExpression.getInputs().size() == 1) {
                    OptExpression subSubOptExpression = subOptExpression.getInputs().get(0);
                    if (subSubOptExpression.getOp().getOpType() == OperatorType.LOGICAL_FILTER) {
                        return subSubOptExpression;
                    }
                }
            }
        }
        return null;
    }

    private boolean isInJoinPredicate(List<BinaryPredicateOperator> joinOnPredicates, ScalarOperator filterSo) {
        if (OperatorType.BINARY.equals(filterSo.getOpType())) {
            for (BinaryPredicateOperator joinOnPredicate : joinOnPredicates) {
                BinaryPredicateOperator filterBso = (BinaryPredicateOperator) filterSo;
                List<ScalarOperator> filterBsoChild = filterBso.getChildren();
                if ((filterBsoChild.contains(joinOnPredicate.getChildren().get(0)) &&
                        filterBsoChild.contains(joinOnPredicate.getChildren().get(1)))) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression joinOptExpression = input.getInputs().get(0);
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) joinOptExpression.getOp();
        List<BinaryPredicateOperator> joinOnPredicates = getJoinOnEqPredicate(joinOptExpression);
        List<ColumnRefOperator> joinOnColumnRefs = getJoinOnColumn(joinOptExpression);
        Collection<ScalarOperator> filterPredicateSet = getFilterPredicateSets(input);
        Set<ScalarOperator> additionFilterPredicates = Sets.newHashSet();
        collectAdditionPredicate(joinOptExpression.getInputs().get(0), joinOperator, joinOnPredicates,
                joinOnColumnRefs, filterPredicateSet, additionFilterPredicates);
        collectAdditionPredicate(joinOptExpression.getInputs().get(1), joinOperator, joinOnPredicates,
                joinOnColumnRefs, filterPredicateSet, additionFilterPredicates);

        if (input.getOp().getOpType() == OperatorType.LOGICAL_FILTER) {
            additionFilterPredicates.addAll(filterPredicateSet);
            LogicalFilterOperator filterOperator = (LogicalFilterOperator) input.getOp();
            filterOperator.setPredicate(Utils.compoundAnd(additionFilterPredicates));
        } else {
            LogicalFilterOperator newFilterOperator = new LogicalFilterOperator(Utils.compoundAnd(additionFilterPredicates));
            OptExpression newFilterOptExpression = new OptExpression(newFilterOperator);
            newFilterOptExpression.getInputs().addAll(input.getInputs());
            input.getInputs().clear();
            input.getInputs().add(newFilterOptExpression);
        }
        return Lists.newArrayList(input);
    }

    private void collectAdditionPredicate(
            OptExpression childOptExpression, LogicalJoinOperator joinOperator, List<BinaryPredicateOperator> joinOnPredicates,
            List<ColumnRefOperator> joinOnColumnRefs, Collection<ScalarOperator> filterPredicateSet,
            Set<ScalarOperator> additionFilterPredicate) {
        OptExpression offspringFilter = getOffspringFilter(childOptExpression);
        if (offspringFilter != null) {
            ScalarOperator offspringFilterPredicate = offspringFilter.getOp().getPredicate();
            ScalarOperator compoundPredicate = Utils.compoundAnd(joinOperator.getOnPredicate(),
                    offspringFilterPredicate, joinOperator.getPredicate());
            List<ScalarOperator> compoundPredicateList = Utils.extractConjuncts(compoundPredicate);
            ScalarEquivalenceExtractor scalarEquivalenceExtractor = new ScalarEquivalenceExtractor();
            scalarEquivalenceExtractor.union(compoundPredicateList);
            for (ColumnRefOperator joinOnColumnRef : joinOnColumnRefs) {
                for (ScalarOperator so : scalarEquivalenceExtractor.getEquivalentScalar(joinOnColumnRef)) {
                    boolean isAdditionPredicate = !so.getOpType().equals(OperatorType.IS_NULL) &&
                            !isInJoinPredicate(joinOnPredicates, so) && !filterPredicateSet.contains(so);
                    if (isAdditionPredicate) {
                        additionFilterPredicate.add(so);
                    }
                }
            }
        }
    }
}