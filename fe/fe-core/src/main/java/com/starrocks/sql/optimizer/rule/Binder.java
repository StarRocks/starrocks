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
import com.starrocks.sql.optimizer.operator.OperatorType;
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

    // `multiJoinBinder` is used for MULTI_JOIN pattern and is stateless so can be used in recursive.
    private final MultiJoinBinder multiJoinBinder = new MultiJoinBinder();
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
    public Binder(Pattern pattern, GroupExpression groupExpression) {
        this.pattern = pattern;
        this.groupExpression = groupExpression;
        this.groupExpressionIndex = Lists.newArrayList(0);

        // MULTI_JOIN is a special pattern which can contain children groups if the input group expression
        // is not a scan node.
        this.isPatternWithoutChildren = pattern.isPatternMultiJoin()
                ? Pattern.ALL_SCAN_TYPES.contains(groupExpression.getOp().getOpType())
                : pattern.children().size() == 0;
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
            int lastNode = this.groupExpressionIndex.size() - 1;
            int lastNodeIndex = this.groupExpressionIndex.get(lastNode);
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
        if (pattern.isPatternMultiJoin()) {
            return multiJoinBinder.match(groupExpression);
        }

        if (!pattern.matchWithoutChild(groupExpression)) {
            return null;
        }

        // recursion match children
        List<OptExpression> resultInputs = Lists.newArrayList();

        int patternIndex = 0;
        int groupExpressionIndex = 0;

        while (patternIndex < pattern.children().size() && groupExpressionIndex < groupExpression.getInputs().size()) {
            trace();
            Group group = groupExpression.getInputs().get(groupExpressionIndex);
            Pattern childPattern = pattern.childAt(patternIndex);
            OptExpression opt = match(childPattern, extractGroupExpression(childPattern, group));

            if (opt == null) {
                return null;
            } else {
                resultInputs.add(opt);
            }

            if (!(childPattern.isPatternMultiLeaf() &&
                    groupExpression.getInputs().size() - groupExpressionIndex >
                            pattern.children().size() - patternIndex)) {
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
        int valueIndex = groupExpressionIndex.get(groupTraceKey);
        if (pattern.isPatternLeaf() || pattern.isPatternMultiLeaf()) {
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
        public OptExpression match(GroupExpression ge) {
            // 1. Check if the entire tree is MULTI_JOIN
            // 2. Enumerate GE
            if (ge == null || !isMultiJoin(ge)) {
                return null;
            }

            return enumerate(ge);
        }

        private OptExpression enumerate(GroupExpression ge) {
            List<OptExpression> resultInputs = Lists.newArrayList();
            int groupExpressionIndex = 0;
            while (groupExpressionIndex < ge.getInputs().size()) {
                trace();

                Group group = ge.getInputs().get(groupExpressionIndex);
                GroupExpression nextGroupExpression = extractGroupExpression(group);
                // avoid recursive
                if (nextGroupExpression == null || !isMultiJoinOp(nextGroupExpression)) {
                    return null;
                }

                OptExpression opt = enumerate(nextGroupExpression);
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
            List<GroupExpression> groupExpressions = group.getLogicalExpressions();
            GroupExpression next = groupExpressions.get(valueIndex);
            if (nextIdx == 0) {
                return next;
            }

            // shortcut for no child group expression
            if (Pattern.ALL_SCAN_TYPES.contains(next.getOp().getOpType()) && valueIndex > 0) {
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
            // TODO: Introduce rule based join-reorder for mv rewrite, so can reduce this iteration time.
            int geSize = groupExpressions.size();
            while (++valueIndex < geSize) {
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
            return operatorType.equals(OperatorType.LOGICAL_JOIN) || Pattern.ALL_SCAN_TYPES.contains(operatorType);
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

            for (int i = 0; i < ge.getInputs().size(); i++) {
                Group child = ge.inputAt(i);
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
         * @param g : Group to check whether it has been rewritten by mv rules.
         * @return : true if the Group has GroupExpression which is rewritten by mv rules.
         */
        private boolean hasRewrittenMvScan(Group g) {
            return g.getLogicalExpressions().stream()
                    .skip(1) // since first logical expression has already been checked.
                    .anyMatch(ge -> ge.getOp().getOpType() == OperatorType.LOGICAL_OLAP_SCAN && ge.hasAppliedMVRules());
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
