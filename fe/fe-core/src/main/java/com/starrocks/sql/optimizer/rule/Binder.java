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


package com.starrocks.sql.optimizer.rule;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;

import java.util.List;

// Used to extract matched expression from GroupExpression
public class Binder {

    private final Pattern pattern;
    private final GroupExpression groupExpression;
    // binder status
    // search group trace
    private int groupTraceKey;
    // key: group_trace_key, value: groupExpressionIndex index(max: groupExpressions size)
    // Used to track the groupExpressions ask history of each group node
    private final List<Integer> groupExpressionIndex;

    /**
     * Extract a expression from GroupExpression which match the given pattern
     *
     * @param pattern         Bound expression should be matched
     * @param groupExpression Search this for binding. Because GroupExpression's inputs are groups,
     *                        several Expressions matched the pattern should be bound from it
     */
    public Binder(Pattern pattern, GroupExpression groupExpression) {
        this.pattern = pattern;
        this.groupExpression = groupExpression;
        groupExpressionIndex = Lists.newArrayList(0);
    }

    /*
     * Example:
     *        JOIN(j)        (Group...)
     *        /    \
     *    SCAN      SCAN     (Group...)
     *   /  |  \    /    \
     *  a   b  c   d      e  (GroupExpressions...)
     *
     * Pattern:
     *      JON
     *     /   \
     * SCAN     SCAN
     *
     * will match first: jad
     * next: jae, jbd, jbe....
     */
    public OptExpression next() {
        // For logic scan to physical scan, we only need to match once
        if (pattern.children().size() == 0 && groupExpressionIndex.get(0) > 0) {
            return null;
        }

        OptExpression expression;
        do {
            this.groupTraceKey = 0;

            // Match with the next groupExpression of the last group node
            int lastNode = this.groupExpressionIndex.size() - 1;
            int lastNodeIndex = this.groupExpressionIndex.get(lastNode);
            this.groupExpressionIndex.set(lastNode, lastNodeIndex + 1);

            expression = match(pattern, groupExpression);
        } while (expression == null && this.groupExpressionIndex.size() != 1);

        return expression;
    }

    /**
     * Pattern tree match groupExpression tree
     */
    private OptExpression match(Pattern pattern, GroupExpression groupExpression) {
        return match(pattern, groupExpression, 0);
    }

    private OptExpression match(Pattern pattern, GroupExpression groupExpression, int level) {
        if (!pattern.matchWithoutChild(groupExpression, level)) {
            return null;
        }

        // recursion match children
        List<OptExpression> resultInputs = Lists.newArrayList();

        int patternIndex = 0;
        int groupExpressionIndex = 0;

        while ((pattern.isPatternMultiJoin() || patternIndex < pattern.children().size())
                && groupExpressionIndex < groupExpression.getInputs().size()) {
            trace();
            Group group = groupExpression.getInputs().get(groupExpressionIndex);
            Pattern childPattern;
            if (pattern.isPatternMultiJoin()) {
                childPattern = Pattern.create(OperatorType.PATTERN_MULTIJOIN);
            } else {
                childPattern = pattern.childAt(patternIndex);
            }
            level = pattern.isPatternMultiJoin() && childPattern.isPatternMultiJoin() ? level + 1 : 0;
            OptExpression opt = match(childPattern, extractGroupExpression(childPattern, group), level);

            if (opt == null) {
                return null;
            } else {
                resultInputs.add(opt);
            }

            if (!(pattern.isPatternMultiJoin() || (childPattern.isPatternMultiLeaf()
                    && (groupExpression.getInputs().size() - groupExpressionIndex) >
                    (pattern.children().size() - patternIndex)))) {
                patternIndex++;
            }

            groupExpressionIndex++;
        }

        return new OptExpression(groupExpression, resultInputs);
    }

    private void trace() {
        this.groupTraceKey++;
        for (int i = this.groupExpressionIndex.size(); i < this.groupTraceKey + 1; i++) {
            this.groupExpressionIndex.add(0);
        }
    }

    /**
     * extract GroupExpression by groupExpressionIndex
     */
    private GroupExpression extractGroupExpression(Pattern pattern, Group group) {
        if ((pattern.isPatternLeaf() || pattern.isPatternMultiLeaf() || pattern.isPatternMultiJoin())
                && !shouldReturnMv(group)) {
            if (groupExpressionIndex.get(groupTraceKey) > 0) {
                groupExpressionIndex.remove(groupTraceKey);
                return null;
            }
            return group.getFirstLogicalExpression();
        } else {
            int valueIndex = groupExpressionIndex.get(groupTraceKey);
            if (valueIndex >= group.getLogicalExpressions().size()) {
                groupExpressionIndex.remove(groupTraceKey);
                return null;
            }
            return group.getLogicalExpressions().get(valueIndex);
        }
    }

    // check whether return mv group expression
    // in nested mv rewrite, agg/join may be rewritten by mv.
    // In this case, we should return mv scan operator during binding process to match the pattern
    private boolean shouldReturnMv(Group group) {
        int valueIndex = groupExpressionIndex.get(groupTraceKey);
        if (valueIndex >= group.getLogicalExpressions().size()) {
            return false;
        }
        Operator op = group.getLogicalExpressions().get(valueIndex).getOp();
        if (!(op instanceof LogicalScanOperator)) {
            return false;
        }
        LogicalScanOperator scanOperator = op.cast();
        if (!scanOperator.getTable().isMaterializedView()) {
            return false;
        }
        GroupExpression firstExpr = group.getFirstLogicalExpression();
        if (firstExpr.getOp() instanceof LogicalScanOperator) {
            LogicalScanOperator firstScan = firstExpr.getOp().cast();
            return scanOperator.getTable().getId() != firstScan.getTable().getId();
        } else {
            return true;
        }
    }

    /**
     * Extract a expression from GroupExpression which match the given pattern once
     *
     * @param pattern         Bound expression should be matched
     * @param groupExpression Search this for binding. Because GroupExpression's inputs are groups,
     *                        several Expressions matched the pattern should be bound from it
     */
    public static OptExpression bind(Pattern pattern, GroupExpression groupExpression) {
        Binder binder = new Binder(pattern, groupExpression);
        return binder.next();
    }
}
