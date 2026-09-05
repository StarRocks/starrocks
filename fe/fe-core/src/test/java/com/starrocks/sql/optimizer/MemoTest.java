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


package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MemoTest {
    @Test
    public void testInit(@Mocked OlapTable olapTable1,
                         @Mocked OlapTable olapTable2) {
        new Expectations() {
            {
                olapTable1.getId();
                result = 0;
                minTimes = 0;

                olapTable2.getId();
                result = 1;
                minTimes = 0;
            }
        };

        OptExpression expr = OptExpression.create(new LogicalProjectOperator(Maps.newHashMap()),
                OptExpression.create(new LogicalJoinOperator(),
                        OptExpression.create(new LogicalOlapScanOperator(olapTable1)),
                        OptExpression.create(new LogicalOlapScanOperator(olapTable2))));

        Memo memo = new Memo();
        GroupExpression groupExpression = memo.init(expr);

        assertEquals(OperatorType.LOGICAL_PROJECT, groupExpression.getOp().getOpType());
        assertEquals(OperatorType.LOGICAL_JOIN,
                groupExpression.inputAt(0).getFirstLogicalExpression().getOp().getOpType());

        assertEquals(OperatorType.LOGICAL_OLAP_SCAN,
                groupExpression.inputAt(0).getFirstLogicalExpression().inputAt(0)
                        .getFirstLogicalExpression().getOp()
                        .getOpType());
        assertEquals(OperatorType.LOGICAL_OLAP_SCAN,
                groupExpression.inputAt(0).getFirstLogicalExpression().inputAt(1)
                        .getFirstLogicalExpression().getOp()
                        .getOpType());

        assertEquals(memo.getGroups().size(), 4);
        assertEquals(memo.getGroupExpressions().size(), 4);

        assertEquals(memo.getGroups().get(0).getId(), 0);
        assertEquals(memo.getGroups().get(1).getId(), 1);
        assertEquals(memo.getGroups().get(2).getId(), 2);
        assertEquals(memo.getGroups().get(3).getId(), 3);
    }

    @Test
    public void testInsertGroupExpression(@Mocked OlapTable olapTable1,
                                          @Mocked OlapTable olapTable2) {
        new Expectations() {
            {
                olapTable1.getId();
                result = 0;
                minTimes = 0;

                olapTable2.getId();
                result = 1;
                minTimes = 0;
            }
        };

        OptExpression expr = OptExpression.create(new LogicalProjectOperator(Maps.newHashMap()),
                OptExpression.create(new LogicalJoinOperator(),
                        OptExpression.create(new LogicalOlapScanOperator(olapTable1)),
                        OptExpression.create(new LogicalOlapScanOperator(olapTable2))));

        Memo memo = new Memo();
        memo.init(expr);

        Operator projectOperator = LogicalLimitOperator.init(1, 1);
        GroupExpression newGroupExpression = new GroupExpression(projectOperator, Lists.newArrayList());

        memo.insertGroupExpression(newGroupExpression, memo.getGroups().get(3));

        assertEquals(memo.getGroups().size(), 4);
        assertEquals(memo.getGroupExpressions().size(), 5);
        assertEquals(memo.getGroups().get(3).getLogicalExpressions().size(), 2);
        assertEquals(memo.getGroups().get(3).getPhysicalExpressions().size(), 0);
    }

    /**
     * A merge must leave every group that had a stats-derived first logical expression with a
     * stats-derived first logical expression.
     * <p>
     * DeriveStatsTask reads a child group's FIRST logical expression and asserts isStatsDerived() on it,
     * while the statistics themselves are group-level and shared by every logically-equivalent expression
     * in the group. Merging rewrites the inputs of expressions all over the Memo, and the expression it
     * rewrites is taken out of its group and put back at the end, so a group that is neither the merge
     * source nor its target can end up with a different -- and never-derived -- first expression. A
     * parent DeriveStatsTask already queued on the scheduler stack would then fail that assertion.
     * <p>
     * The memo below is built by hand so the rotation is deterministic: thirdGroup holds
     * [refersToSrc, other], its first expression refers to srcGroup and is derived, and `other` is not.
     * Merging srcGroup away rewrites refersToSrc, which moves it behind `other`.
     */
    @Test
    public void testMergeKeepsStatsDerivedFirstExpression() {
        Memo memo = new Memo();

        // Leaves.
        GroupExpression leftLeaf = new GroupExpression(LogicalLimitOperator.init(1, 1), Lists.newArrayList());
        memo.insertGroupExpression(leftLeaf, null);
        Group leftGroup = leftLeaf.getGroup();

        GroupExpression rightLeaf = new GroupExpression(LogicalLimitOperator.init(2, 2), Lists.newArrayList());
        memo.insertGroupExpression(rightLeaf, null);
        Group rightGroup = rightLeaf.getGroup();

        // The group that gets merged away, plus the target it is merged into.
        Operator srcOp = LogicalLimitOperator.init(3, 3);
        GroupExpression srcExpr = new GroupExpression(srcOp, Lists.newArrayList(leftGroup, rightGroup));
        memo.insertGroupExpression(srcExpr, null);
        Group srcGroup = srcExpr.getGroup();

        GroupExpression dstExpr = new GroupExpression(LogicalLimitOperator.init(4, 4), Lists.newArrayList(srcGroup));
        memo.insertGroupExpression(dstExpr, null);
        Group dstGroup = dstExpr.getGroup();

        // A third group, parent of srcGroup as well, holding two alternatives. Its FIRST one refers to
        // srcGroup, so the merge will rewrite it and move it to the back of the list.
        Operator thirdOp = LogicalLimitOperator.init(5, 5);
        GroupExpression refersToSrc = new GroupExpression(thirdOp, Lists.newArrayList(srcGroup));
        memo.insertGroupExpression(refersToSrc, null);
        Group thirdGroup = refersToSrc.getGroup();
        GroupExpression other = new GroupExpression(thirdOp, Lists.newArrayList(rightGroup));
        memo.insertGroupExpression(other, thirdGroup);

        // Everything has had its statistics derived except `other`, which is only an alternative and has
        // never been the group's representative.
        leftLeaf.setStatsDerived();
        rightLeaf.setStatsDerived();
        srcExpr.setStatsDerived();
        dstExpr.setStatsDerived();
        refersToSrc.setStatsDerived();

        assertSame(refersToSrc, thirdGroup.getFirstLogicalExpression());
        assertTrue(thirdGroup.getFirstLogicalExpression().isStatsDerived());

        // Re-inserting an expression equal to srcGroup's into dstGroup merges srcGroup into dstGroup.
        GroupExpression duplicateOfSrc = new GroupExpression(srcOp, Lists.newArrayList(leftGroup, rightGroup));
        memo.insertGroupExpression(duplicateOfSrc, dstGroup);

        // thirdGroup survived the merge, so whatever expression now represents it must still be derived.
        assertTrue(memo.getGroups().contains(thirdGroup));
        assertTrue(thirdGroup.getFirstLogicalExpression().isStatsDerived(),
                "first logical expression of a surviving group lost its derived statistics marker");
    }

    /**
     * A merge folds a group into a group that (transitively) has it as an input, which turns the parent
     * expression into one whose own input is the group it lives in. Such an expression must not survive
     * in the Memo, and it must be marked unused: an ApplyRuleTask may already be queued on the scheduler
     * stack holding it, and JoinCommutativityRule-style rules would otherwise transform it and copy the
     * result back into its own group, tripping checkState(group != targetGroup) in copyIn.
     */
    @Test
    public void testMergeMarksSelfReferencingExpressionUnused() {
        Memo memo = new Memo();

        GroupExpression leaf = new GroupExpression(LogicalLimitOperator.init(1, 1), Lists.newArrayList());
        memo.insertGroupExpression(leaf, null);
        Group leafGroup = leaf.getGroup();

        Operator srcOp = LogicalLimitOperator.init(2, 2);
        GroupExpression srcExpr = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(srcExpr, null);
        Group srcGroup = srcExpr.getGroup();

        // parent lives in dstGroup and takes srcGroup as input, so merging srcGroup into dstGroup makes
        // parent reference dstGroup -- itself.
        GroupExpression parent = new GroupExpression(LogicalLimitOperator.init(3, 3), Lists.newArrayList(srcGroup));
        memo.insertGroupExpression(parent, null);
        Group dstGroup = parent.getGroup();
        // A second alternative so dstGroup does not become empty once parent is dropped.
        GroupExpression survivor = new GroupExpression(LogicalLimitOperator.init(4, 4), Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(survivor, dstGroup);

        GroupExpression duplicateOfSrc = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(duplicateOfSrc, dstGroup);

        assertTrue(parent.isUnused(), "self-referencing expression was dropped but not marked unused");
        for (Group group : memo.getGroups()) {
            for (GroupExpression expression : group.getLogicalExpressions()) {
                assertTrue(expression.getInputs().stream().noneMatch(input -> input == group),
                        "a self-referencing group expression survived the merge");
            }
        }
        assertFalse(memo.getGroupExpressions().containsValue(parent),
                "self-referencing expression is still registered in the Memo");
    }

    /**
     * Dropping the self-referencing expression can itself rotate the group's representative, when the
     * dropped expression was the group's first logical one. The group's statistics are still derived --
     * they are group-level -- so the marker has to move to the new representative.
     */
    @Test
    public void testDroppingSelfReferenceKeepsDerivedRepresentative() {
        Memo memo = new Memo();

        GroupExpression leaf = new GroupExpression(LogicalLimitOperator.init(1, 1), Lists.newArrayList());
        memo.insertGroupExpression(leaf, null);
        Group leafGroup = leaf.getGroup();

        Operator srcOp = LogicalLimitOperator.init(2, 2);
        GroupExpression srcExpr = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(srcExpr, null);
        Group srcGroup = srcExpr.getGroup();

        // becomesSelfReference is dstGroup's FIRST expression and is derived; laterAlternative is not.
        GroupExpression becomesSelfReference =
                new GroupExpression(LogicalLimitOperator.init(3, 3), Lists.newArrayList(srcGroup));
        memo.insertGroupExpression(becomesSelfReference, null);
        Group dstGroup = becomesSelfReference.getGroup();
        GroupExpression laterAlternative =
                new GroupExpression(LogicalLimitOperator.init(4, 4), Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(laterAlternative, dstGroup);

        leaf.setStatsDerived();
        srcExpr.setStatsDerived();
        becomesSelfReference.setStatsDerived();

        assertSame(becomesSelfReference, dstGroup.getFirstLogicalExpression());

        GroupExpression duplicateOfSrc = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(duplicateOfSrc, dstGroup);

        assertTrue(dstGroup.getFirstLogicalExpression().isStatsDerived(),
                "dropping the self-reference left the group without a derived representative");
    }

    /**
     * The compensation must not invent derivation. A group whose statistics were never derived -- which is
     * a legitimate state, e.g. a group created by copyIn with statistics carried over from
     * ReorderJoinRule but no expression put through DeriveStatsTask yet -- has to come out of a merge
     * still undelivered, otherwise its own DeriveStatsTask would return early and skip the real work.
     */
    @Test
    public void testMergeDoesNotInventDerivationForNeverDerivedGroup() {
        Memo memo = new Memo();

        GroupExpression leaf = new GroupExpression(LogicalLimitOperator.init(1, 1), Lists.newArrayList());
        memo.insertGroupExpression(leaf, null);
        Group leafGroup = leaf.getGroup();

        Operator srcOp = LogicalLimitOperator.init(2, 2);
        GroupExpression srcExpr = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(srcExpr, null);
        Group srcGroup = srcExpr.getGroup();

        GroupExpression dstExpr = new GroupExpression(LogicalLimitOperator.init(3, 3), Lists.newArrayList(srcGroup));
        memo.insertGroupExpression(dstExpr, null);
        Group dstGroup = dstExpr.getGroup();

        // A third group whose expressions were never derived at all.
        Operator thirdOp = LogicalLimitOperator.init(4, 4);
        GroupExpression neverDerivedFirst = new GroupExpression(thirdOp, Lists.newArrayList(srcGroup));
        memo.insertGroupExpression(neverDerivedFirst, null);
        Group neverDerivedGroup = neverDerivedFirst.getGroup();
        GroupExpression neverDerivedSecond = new GroupExpression(thirdOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(neverDerivedSecond, neverDerivedGroup);

        GroupExpression duplicateOfSrc = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(duplicateOfSrc, dstGroup);

        for (GroupExpression expression : neverDerivedGroup.getLogicalExpressions()) {
            assertFalse(expression.isStatsDerived(),
                    "merge marked an expression derived in a group that never derived its statistics");
        }
    }

    /**
     * The statistics themselves live on the Group ({@code Group.statistics}); a GroupExpression only
     * carries the derived marker. So moving the marker to the new representative is not a claim that
     * statistics exist -- they demonstrably already do, computed while the old representative was derived,
     * and they are what DeriveStatsTask's checkNotNull and ExpressionContext actually read.
     */
    @Test
    public void testGroupKeepsItsStatisticsAcrossMerge() {
        Memo memo = new Memo();

        GroupExpression leaf = new GroupExpression(LogicalLimitOperator.init(1, 1), Lists.newArrayList());
        memo.insertGroupExpression(leaf, null);
        Group leafGroup = leaf.getGroup();

        Operator srcOp = LogicalLimitOperator.init(2, 2);
        GroupExpression srcExpr = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(srcExpr, null);
        Group srcGroup = srcExpr.getGroup();

        GroupExpression dstExpr = new GroupExpression(LogicalLimitOperator.init(3, 3), Lists.newArrayList(srcGroup));
        memo.insertGroupExpression(dstExpr, null);
        Group dstGroup = dstExpr.getGroup();

        Operator thirdOp = LogicalLimitOperator.init(4, 4);
        GroupExpression refersToSrc = new GroupExpression(thirdOp, Lists.newArrayList(srcGroup));
        memo.insertGroupExpression(refersToSrc, null);
        Group thirdGroup = refersToSrc.getGroup();
        GroupExpression other = new GroupExpression(thirdOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(other, thirdGroup);

        // Mirror what a completed DeriveStatsTask leaves behind: statistics on the group, marker on the
        // expression that computed them.
        Statistics derived = Statistics.builder().setOutputRowCount(1234).build();
        thirdGroup.setStatistics(derived);
        refersToSrc.setStatsDerived();
        leaf.setStatsDerived();
        srcExpr.setStatsDerived();
        dstExpr.setStatsDerived();

        GroupExpression duplicateOfSrc = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(duplicateOfSrc, dstGroup);

        // The statistics are untouched by the merge; only which expression carries the marker changed.
        assertSame(derived, thirdGroup.getStatistics());
        assertTrue(thirdGroup.getFirstLogicalExpression().isStatsDerived());
        assertEquals(1234, thirdGroup.getStatistics().getOutputRowCount(), 0.0);
    }

    /**
     * The merge source is destroyed and every expression that referenced it is rewritten to reference the
     * destination, so a queued parent DeriveStatsTask that used to check the source's representative now
     * checks the destination's. Group.mergeGroup already carries the source's statistics over to a
     * destination that had none, so the derived marker has to travel with them -- otherwise the parent
     * passes the checkNotNull on the inherited statistics and then fails isStatsDerived() on the
     * destination's never-derived first expression.
     */
    @Test
    public void testMergeCarriesDerivedMarkerFromSourceToDestination() {
        Memo memo = new Memo();

        GroupExpression leaf = new GroupExpression(LogicalLimitOperator.init(1, 1), Lists.newArrayList());
        memo.insertGroupExpression(leaf, null);
        Group leafGroup = leaf.getGroup();

        // srcGroup: derived representative, and statistics to go with it.
        Operator srcOp = LogicalLimitOperator.init(2, 2);
        GroupExpression srcExpr = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(srcExpr, null);
        Group srcGroup = srcExpr.getGroup();
        srcGroup.setStatistics(Statistics.builder().setOutputRowCount(4321).build());
        leaf.setStatsDerived();
        srcExpr.setStatsDerived();

        // dstGroup: never derived, no statistics of its own, and it is a parent of srcGroup so the merge
        // rewrites its expression to point at itself and that self reference gets dropped.
        GroupExpression dstFirst = new GroupExpression(LogicalLimitOperator.init(3, 3), Lists.newArrayList(srcGroup));
        memo.insertGroupExpression(dstFirst, null);
        Group dstGroup = dstFirst.getGroup();
        GroupExpression dstSecond = new GroupExpression(LogicalLimitOperator.init(4, 4), Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(dstSecond, dstGroup);

        GroupExpression duplicateOfSrc = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(duplicateOfSrc, dstGroup);

        // The destination absorbed the source, statistics included, so it must also present a derived
        // representative to any parent that now points at it.
        assertTrue(memo.getGroups().contains(dstGroup));
        assertTrue(dstGroup.getStatistics() != null,
                "destination did not inherit the source statistics");
        assertTrue(dstGroup.getFirstLogicalExpression().isStatsDerived(),
                "destination inherited the source statistics but not a derived representative");
    }

    /**
     * When rewriting a child makes two parent expressions equal, mergeGroupImpl recursively merges their
     * groups by calling itself, which bypasses the cleanup mergeGroup does for the top-level pair. A
     * rewrite inside such a cascade can turn an expression in the cascade destination into a self
     * reference; it would stay registered because the top-level cleanup never visits that group, and a
     * queued ApplyRuleTask could feed it back into copyIn.
     */
    @Test
    public void testCascadeMergeLeavesNoSelfReference() {
        Memo memo = new Memo();

        GroupExpression leaf = new GroupExpression(LogicalLimitOperator.init(1, 1), Lists.newArrayList());
        memo.insertGroupExpression(leaf, null);
        Group leafGroup = leaf.getGroup();

        Operator srcOp = LogicalLimitOperator.init(2, 2);
        GroupExpression srcExpr = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(srcExpr, null);
        Group srcGroup = srcExpr.getGroup();

        GroupExpression dstExpr = new GroupExpression(LogicalLimitOperator.init(3, 3), Lists.newArrayList(srcGroup));
        memo.insertGroupExpression(dstExpr, null);
        Group dstGroup = dstExpr.getGroup();

        // Two parents of srcGroup, in two different groups, carrying the same operator. Once srcGroup is
        // rewritten to dstGroup they become equal, which is what queues a cascade sub-merge.
        Operator parentOp = LogicalLimitOperator.init(4, 4);
        GroupExpression parentA = new GroupExpression(parentOp, Lists.newArrayList(srcGroup));
        memo.insertGroupExpression(parentA, null);
        Group parentGroupA = parentA.getGroup();
        GroupExpression parentB = new GroupExpression(parentOp, Lists.newArrayList(dstGroup));
        memo.insertGroupExpression(parentB, null);
        Group parentGroupB = parentB.getGroup();
        // Keep both parent groups alive with a second alternative each.
        memo.insertGroupExpression(
                new GroupExpression(LogicalLimitOperator.init(5, 5), Lists.newArrayList(leafGroup)), parentGroupA);
        memo.insertGroupExpression(
                new GroupExpression(LogicalLimitOperator.init(6, 6), Lists.newArrayList(leafGroup)), parentGroupB);

        GroupExpression duplicateOfSrc = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(duplicateOfSrc, dstGroup);

        for (Group group : memo.getGroups()) {
            for (GroupExpression expression : group.getLogicalExpressions()) {
                assertTrue(expression.getInputs().stream().noneMatch(input -> input == group),
                        "a self reference survived in group " + group.getId());
            }
            for (GroupExpression expression : group.getPhysicalExpressions()) {
                assertTrue(expression.getInputs().stream().noneMatch(input -> input == group),
                        "a physical self reference survived in group " + group.getId());
            }
        }
    }

    /**
     * Physical expressions are scanned for self references too, and a merge that empties a group must
     * leave the Memo consistent rather than keeping a group nothing can represent.
     */
    @Test
    public void testMergeEmptyingAGroupLeavesNoDanglingReference() {
        Memo memo = new Memo();

        GroupExpression leaf = new GroupExpression(LogicalLimitOperator.init(1, 1), Lists.newArrayList());
        memo.insertGroupExpression(leaf, null);
        Group leafGroup = leaf.getGroup();

        Operator srcOp = LogicalLimitOperator.init(2, 2);
        GroupExpression srcExpr = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(srcExpr, null);
        Group srcGroup = srcExpr.getGroup();

        // dstGroup's only expression becomes self-referencing, so the group loses everything it had.
        GroupExpression onlyExpr = new GroupExpression(LogicalLimitOperator.init(3, 3), Lists.newArrayList(srcGroup));
        memo.insertGroupExpression(onlyExpr, null);
        Group dstGroup = onlyExpr.getGroup();

        GroupExpression duplicateOfSrc = new GroupExpression(srcOp, Lists.newArrayList(leafGroup));
        memo.insertGroupExpression(duplicateOfSrc, dstGroup);

        // Whatever survives, no registered expression may point at a group that has no logical expression,
        // because Group.getFirstLogicalExpression() would blow up on it.
        for (GroupExpression expression : memo.getGroupExpressions().values()) {
            for (Group input : expression.getInputs()) {
                assertFalse(input.getLogicalExpressions().isEmpty(),
                        "a registered group expression points at a group with no logical expression");
            }
        }
        for (Group group : memo.getGroups()) {
            assertFalse(group.getLogicalExpressions().isEmpty(),
                    "an empty group is still registered in the Memo");
        }
    }

}
