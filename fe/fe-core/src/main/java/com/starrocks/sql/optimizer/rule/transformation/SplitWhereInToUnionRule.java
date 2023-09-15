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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.scalar.NegateFilterShuttle;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

// For JDBC scan operator, split in filter with large in list into multiple scans so that the in filter
// can use index to filter lots of rows.
// Required:
// 1. in filter is not nested with other filters, it should on the first level. For example:
//     (a in (1, 2, 3, 4)) AND (...) AND (...) AND ...
//     (a in (1, 2, 3, 4)) OR (...) AND (...) OR ...
//     (...) OR (a in (1, 2, 3, 4)) OR (...) OR (...)
// 2. Or in filter is nested with other predicates using AND operator(can not include any OR in the predicate tree).
// For example:
//     (...) OR (a in (1, 2, 3, 4) AND b != 1 AND C > 0) OR (...) OR (...)
public class SplitWhereInToUnionRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(SplitWhereInToUnionRule.class);

    private static final SplitWhereInToUnionRule INSTANCE = new SplitWhereInToUnionRule();

    protected SplitWhereInToUnionRule() {
        super(RuleType.TF_SPLIT_WHERE_IN, Pattern.create(OperatorType.LOGICAL_JDBC_SCAN));
    }

    public static SplitWhereInToUnionRule getInstance() {
        return INSTANCE;
    }

    public static boolean isInListExceedsThreshold(InPredicateOperator predicate, int threshold) {
        return !predicate.isSubquery()
                && !predicate.isNotIn()
                && predicate.getListChildren().size() > threshold
                && predicate.allValuesMatch(ScalarOperator::isConstantRef);
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalJDBCScanOperator scan = (LogicalJDBCScanOperator) input.getOp();
        ScalarOperator predicate = scan.getPredicate();
        if (predicate == null) {
            return false;
        }

        int inListThreshold = context.getSessionVariable().getWhereInToUnionThreshold();
        CheckInFilterVisitor checkInFilterVisitor = new CheckInFilterVisitor(inListThreshold);
        predicate.accept(checkInFilterVisitor, null);

        // only rewrite in filter with in list > threshold
        return checkInFilterVisitor.getInListExceedsThresholdPredicates() > 0;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        try {
            return transformImpl(input, context);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("input: {}, msg: {}", input.explain(), e.getMessage());
            }
            return Lists.newArrayList();
        }
    }

    private List<OptExpression> transformImpl(OptExpression input, OptimizerContext context) {
        LogicalJDBCScanOperator scan = (LogicalJDBCScanOperator) input.getOp();
        ScalarOperator predicate = scan.getPredicate();
        List<ScalarOperator> orList = Utils.extractDisjunctive(predicate);
        List<List<ScalarOperator>> orAndPredicates = Lists.newArrayList();
        for (ScalarOperator or : orList) {
            orAndPredicates.add(Utils.extractConjuncts(or));
        }

        // find first in predicate with in list exceeds threshold
        int inListThreshold = context.getSessionVariable().getWhereInToUnionThreshold();
        List<Pair<Integer, Integer>> posList = findInFilterExceedsThreshold(orAndPredicates, inListThreshold);
        if (!isValid(posList)) {
            return Lists.newArrayList();
        }

        Pair<Integer, Integer> pos = posList.get(0);
        List<ScalarOperator> foundInFilterPredicate = orAndPredicates.get(pos.first);
        orAndPredicates.remove(foundInFilterPredicate);

        InPredicateOperator inFilter = (InPredicateOperator) foundInFilterPredicate.get(pos.second);
        foundInFilterPredicate.remove(inFilter);
        List<InPredicateOperator> splits = splitInFilter(inFilter, inListThreshold, inListThreshold);

        List<ScalarOperator> newPredicates = constructNewPredicatesForScan(orAndPredicates, foundInFilterPredicate, splits);

        return Lists.newArrayList(OptExpression.create(buildUnionAllOperator(scan, newPredicates.size()),
                buildUnionAllInputs(scan, newPredicates)));
    }

    private boolean isValid(List<Pair<Integer, Integer>> posList) {
        // no in filter found
        if (posList.isEmpty()) {
            return false;
        }

        // only allow all in-filters in the same OR block and compounded with AND
        return posList.stream().map(p -> p.first).collect(Collectors.toSet()).size() == 1;
    }

    private static List<ScalarOperator> constructNewPredicatesForScan(List<List<ScalarOperator>> orAndPredicates,
                                                                      List<ScalarOperator> foundInFilterPredicate,
                                                                      List<InPredicateOperator> splits) {
        List<ScalarOperator> newPredicates = Lists.newArrayList();
        if (!orAndPredicates.isEmpty()) {
            ScalarOperator predicate = Utils.compoundOr(
                    orAndPredicates.stream().map(Utils::compoundAnd).collect(Collectors.toList()));
            newPredicates.add(predicate);

            NegateFilterShuttle shuttle = NegateFilterShuttle.getInstance();
            ScalarOperator negated = shuttle.negateFilter(predicate);
            foundInFilterPredicate.add(negated);
        }

        for (InPredicateOperator operator : splits) {
            foundInFilterPredicate.add(operator);
            ScalarOperator predicate = Utils.compoundAnd(foundInFilterPredicate);
            newPredicates.add(predicate);
            foundInFilterPredicate.remove(operator);
        }
        return newPredicates;
    }

    private List<InPredicateOperator> splitInFilter(InPredicateOperator origin, int inListSize, int threshold) {
        Preconditions.checkArgument(isInListExceedsThreshold(origin, threshold));
        // ensure no duplicated constant values, otherwise the final result will contain duplicated records
        List<ScalarOperator> children = deduplicateConstantOperators(origin.getListChildren());

        List<InPredicateOperator> splits = Lists.newArrayList();
        ScalarOperator ref = origin.getChild(0);
        for (List<ScalarOperator> group : Lists.partition(children, inListSize)) {
            List<ScalarOperator> arguments = Lists.newArrayList(ref);
            arguments.addAll(group);
            splits.add(new InPredicateOperator(origin.isNotIn(), arguments));
        }
        return splits;
    }

    private static List<ScalarOperator> deduplicateConstantOperators(List<ScalarOperator> operators) {
        // keep the original order
        Set<ScalarOperator> uniqueOperators = new LinkedHashSet<>(operators);
        return new ArrayList<>(uniqueOperators);
    }

    private List<OptExpression> buildUnionAllInputs(LogicalJDBCScanOperator scanOperator,
                                                    List<ScalarOperator> scanPredicates) {
        List<OptExpression> inputs = Lists.newArrayList();
        for (ScalarOperator scanPredicate : scanPredicates) {
            LogicalJDBCScanOperator scan = new LogicalJDBCScanOperator.Builder()
                    .withOperator(scanOperator)
                    .setPredicate(scanPredicate)
                    .build();
            inputs.add(OptExpression.create(scan));
        }
        return inputs;
    }

    private LogicalUnionOperator buildUnionAllOperator(LogicalJDBCScanOperator scanOperator, int childNum) {
        List<ColumnRefOperator> outputColumns = scanOperator.getOutputColumns();
        List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayList();
        for (int i = 0; i < childNum; i++) {
            childOutputColumns.add(outputColumns);
        }
        return new LogicalUnionOperator(outputColumns, childOutputColumns, true);
    }

    List<Pair<Integer, Integer>> findInFilterExceedsThreshold(List<List<ScalarOperator>> orAndPredicates,
                                                                       int threshold) {
        List<Pair<Integer, Integer>> pairs = Lists.newArrayList();
        for (int i = 0; i < orAndPredicates.size(); i++) {
            List<ScalarOperator> andPredicates = orAndPredicates.get(i);
            for (int j = 0; j < andPredicates.size(); j++) {
                ScalarOperator op = andPredicates.get(j);
                if (op instanceof InPredicateOperator && isInListExceedsThreshold((InPredicateOperator) op, threshold)) {
                    pairs.add(Pair.create(i, j));
                }
            }
        }
        return pairs;
    }

    private static class CheckInFilterVisitor extends ScalarOperatorVisitor<ScalarOperator, Void> {

        private final int inListThreshold;
        private int inListExceedsThresholdPredicates = 0;

        CheckInFilterVisitor(int inListThreshold) {
            this.inListThreshold = inListThreshold;
        }

        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
            return scalarOperator;
        }

        @Override
        public ScalarOperator visitInPredicate(InPredicateOperator predicate, Void context) {
            if (isInListExceedsThreshold(predicate, inListThreshold)) {
                inListExceedsThresholdPredicates++;
            }
            return predicate;
        }

        public int getInListExceedsThresholdPredicates() {
            return inListExceedsThresholdPredicates;
        }

        @Override
        public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            for (ScalarOperator operator : predicate.getChildren()) {
                operator.accept(this, null);
            }
            return predicate;
        }
    }
}
