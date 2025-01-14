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

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.MultiOpPattern;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;

import java.util.List;
import java.util.concurrent.TimeUnit;

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

    // `multiJoinBinder` is used for MULTI_JOIN pattern and is stateless so can be used in recursive.
    private final MultiJoinBinder multiJoinBinder;
    // `isPatternWithoutChildren` is to mark whether the input pattern can be used for zero child optimization.
    private final boolean isPatternWithoutChildren;
    // `nextIdx` marks the current idx which iterates calling `next()` method and it's used for MULTI_JOIN pattern
    // to optimize iteration expansions.
    private int nextIdx = 0;

    /**
     * Extract a expression from GroupExpression which match the given pattern
     *
     * @param pattern         Bound expression should be matched
     * @param groupExpression Search this for binding. Because GroupExpression's inputs are groups,
     *                        several Expressions matched the pattern should be bound from it
     */
    public Binder(OptimizerContext optimizerContext, Pattern pattern,
                  GroupExpression groupExpression, Stopwatch stopwatch) {
        this.pattern = pattern;
        this.groupExpression = groupExpression;
        this.groupExpressionIndex = Lists.newArrayList(0);

        this.multiJoinBinder = new MultiJoinBinder(optimizerContext, stopwatch);
        // MULTI_JOIN is a special pattern which can contain children groups if the input group expression
        // is not a scan node.
        this.isPatternWithoutChildren = pattern.is(OperatorType.PATTERN_MULTIJOIN)
                ? MultiOpPattern.ALL_SCAN_TYPES.contains(groupExpression.getOp().getOpType())
                : pattern.children().isEmpty();
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
        if (isPatternWithoutChildren && groupExpressionIndex.get(0) > 0) {
            return null;
        }

        OptExpression expression;
        do {
            this.groupTraceKey = 0;

            // Match with the next groupExpression of the last group node
            final int lastNode = this.groupExpressionIndex.size() - 1;
            final int lastNodeIndex = this.groupExpressionIndex.get(lastNode);
            this.groupExpressionIndex.set(lastNode, lastNodeIndex + 1);

            expression = match(pattern, groupExpression);
        } while (expression == null && this.groupExpressionIndex.size() != 1);

        nextIdx++;
        return expression;
    }

    /**
     * Pattern tree match groupExpression tree
     */
    private OptExpression match(Pattern pattern, GroupExpression groupExpression) {
        if (pattern.is(OperatorType.PATTERN_MULTIJOIN)) {
            return multiJoinBinder.match(groupExpression);
        }

        if (!pattern.matchWithoutChild(groupExpression)) {
            return null;
        }

        // recursion match children
        List<OptExpression> resultInputs = Lists.newArrayList();

        int patternIndex = 0;
        int groupExpressionIndex = 0;

        final int patternSize = pattern.children().size();
        final int geSize = groupExpression.getInputs().size();
        while (patternIndex < patternSize && groupExpressionIndex < geSize) {
            trace();

            final Group group = groupExpression.getInputs().get(groupExpressionIndex);
            final Pattern childPattern = pattern.childAt(patternIndex);
            final OptExpression opt = match(childPattern, extractGroupExpression(childPattern, group));

            if (opt == null) {
                return null;
            } else {
                resultInputs.add(opt);
            }

            if (!(childPattern.is(OperatorType.PATTERN_MULTI_LEAF) &&
                    geSize - groupExpressionIndex > patternSize - patternIndex)) {
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
        final int valueIndex = groupExpressionIndex.get(groupTraceKey);
        if (pattern.is(OperatorType.PATTERN_LEAF) || pattern.is(OperatorType.PATTERN_MULTI_LEAF)) {
            if (valueIndex > 0) {
                groupExpressionIndex.remove(groupTraceKey);
                return null;
            }
            return group.getFirstLogicalExpression();
        } else {
            if (valueIndex >= group.getLogicalExpressions().size()) {
                groupExpressionIndex.remove(groupTraceKey);
                return null;
            }
            return group.getLogicalExpressions().get(valueIndex);
        }
    }

    /**
     * Expression binding for MULTI_JOIN, which contains only JOIN/SCAN nodes
     * <p>
     * NOTE: Why not match using regular pattern ?
     * 1. Regular matching cannot bind the tree but only the partial expression. But MULTI_JOIN needs to extract
     * the entire tree
     * 2. Regular matching is extremely slow in this case since it needs to recursive descent the tree, build the
     * binding state and check the expression at the same time. But MULTI_JOIN could enumerate the GE without any check
     */
    private class MultiJoinBinder {
        // Stopwatch to void infinite loop
        private final Stopwatch watch;
        // Time limit for the entire optimization
        private final long timeLimit;
        // to avoid stop watch costing too much time, only check exhausted every CHECK_EXHAUSTED_INTERVAL times
        private static final int CHECK_EXHAUSTED_INTERVAL = 1000;
        private long loopCount = 0;

        public MultiJoinBinder(OptimizerContext optimizerContext, Stopwatch stopwatch) {
            SessionVariable sessionVariable = optimizerContext.getSessionVariable();
            this.watch = stopwatch;
            this.timeLimit = Math.min(sessionVariable.getOptimizerMaterializedViewTimeLimitMillis(),
                    sessionVariable.getOptimizerExecuteTimeout());
        }

        public OptExpression match(GroupExpression ge) {
            // 1. Check if the entire tree is MULTI_JOIN
            // 2. Enumerate GE
            if (ge == null || !isMultiJoin(ge)) {
                return null;
            }

            return enumerate(ge);
        }

        /**
         * Check whether the binder is exhausted.
         */
        private boolean exhausted() {
            if (loopCount++ % CHECK_EXHAUSTED_INTERVAL == 0) {
                final long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
                final boolean exhausted = elapsed > timeLimit;
                if (exhausted) {
                    Tracers.log(Tracers.Module.MV, args ->
                            String.format("[MV TRACE] MultiJoinBinder %s exhausted(loop:%s)\n", this, loopCount));
                }
                return exhausted;
            }
            return false;
        }

        private OptExpression enumerate(GroupExpression ge) {
            final List<OptExpression> resultInputs = Lists.newArrayList();
            final int geSize = ge.getInputs().size();

            int groupExpressionIndex = 0;
            while (groupExpressionIndex < geSize) {
                // to avoid infinite loop
                if (exhausted()) {
                    return null;
                }
                trace();

                final Group group = ge.getInputs().get(groupExpressionIndex);
                final GroupExpression nextGroupExpression = extractGroupExpression(group);
                // avoid recursive
                if (nextGroupExpression == null || !isMultiJoinOp(nextGroupExpression)) {
                    return null;
                }

                final OptExpression opt = enumerate(nextGroupExpression);
                if (opt == null) {
                    return null;
                } else {
                    resultInputs.add(opt);
                }
                groupExpressionIndex++;
            }

            return new OptExpression(ge, resultInputs);
        }

        private GroupExpression extractGroupExpression(Group group) {
            int valueIndex = groupExpressionIndex.get(groupTraceKey);
            if (valueIndex >= group.getLogicalExpressions().size()) {
                groupExpressionIndex.remove(groupTraceKey);
                return null;
            }
            final List<GroupExpression> groupExpressions = group.getLogicalExpressions();
            GroupExpression next = groupExpressions.get(valueIndex);
            if (nextIdx == 0) {
                return next;
            }

            // shortcut for no child group expression
            if (valueIndex > 0 && MultiOpPattern.ALL_SCAN_TYPES.contains(next.getOp().getOpType())) {
                groupExpressionIndex.remove(groupTraceKey);
                return null;
            }
            // directly return if next has rewritten by mv
            if (next.hasAppliedMVRules()) {
                return next;
            }

            // NOTE: To avoid iterating all children group expressions which may take a lot of time, only iterate
            // group expressions which are already rewritten by mv except the first iteration, so can be used for
            // nested mv rewritten.
            final int geSize = groupExpressions.size();
            while (++valueIndex < geSize) {
                if (exhausted()) {
                    groupExpressionIndex.remove(groupTraceKey);
                    return null;
                }

                next = group.getLogicalExpressions().get(valueIndex);
                if (next.hasAppliedMVRules()) {
                    groupExpressionIndex.set(groupTraceKey, valueIndex);
                    return next;
                }
            }
            groupExpressionIndex.remove(groupTraceKey);
            return null;
        }

        private boolean isMultiJoinOp(GroupExpression ge) {
            OperatorType operatorType = ge.getOp().getOpType();
            return operatorType.equals(OperatorType.LOGICAL_JOIN) ||
                    MultiOpPattern.ALL_SCAN_TYPES.contains(operatorType);
        }

        private boolean isMultiJoin(GroupExpression ge) {
            return ge.getOp().getOpType() == OperatorType.LOGICAL_JOIN && isMultiJoinRecursive(ge);
        }

        /**
         * Recursive check whether the GE is a MULTI_JOIN
         * For nested-mv, the original GE may be a AGG, after rewriting this group could be put an SCAN, so
         * we also need to consider this kind of GE as MULTI_JOIN
         */
        private boolean isMultiJoinRecursive(GroupExpression ge) {
            if (!isMultiJoinOp(ge)) {
                return false;
            }
            final int geSize = ge.getInputs().size();
            for (int i = 0; i < geSize; i++) {
                // to avoid infinite loop
                if (exhausted()) {
                    return false;
                }
                final Group child = ge.inputAt(i);
                if (isMultiJoinRecursive(child.getFirstLogicalExpression())) {
                    continue;
                }
                if (!hasRewrittenMvScan(child)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Check Group's logical expressions except the first has already been rewritten by mv rules.
         *
         * @param g : Group to check whether it has been rewritten by mv rules.
         * @return : true if the Group has GroupExpression which is rewritten by mv rules.
         */
        private boolean hasRewrittenMvScan(Group g) {
            return g.getLogicalExpressions().stream()
                    .skip(1) // since first logical expression has already been checked.
                    .anyMatch(ge -> ge.getOp().getOpType() == OperatorType.LOGICAL_OLAP_SCAN && ge.hasAppliedMVRules());
        }
    }
}
