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

import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.EquivalentDescriptor;
import com.starrocks.sql.optimizer.base.RangeDistributionSpec;
import com.starrocks.sql.optimizer.task.TaskContext;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the PR-3 range-colocate-join gate in
 * {@link ChildOutputPropertyGuarantor#canRangeColocateJoin}.
 *
 * <p>Covers:
 * <ul>
 * <li>Join-type allowlist (INNER, LEFT OUTER, LEFT SEMI, LEFT ANTI) —
 *     RIGHT / FULL rejected.
 * <li>Position-preserving pairing accepts identity join keys.
 * <li>Position-preserving pairing rejects swapped-column joins.
 * <li>Partial shuffle coverage rejected.
 * <li>{@code disable_colocate_join} session kill.
 * </ul>
 */
class ChildOutputPropertyGuarantorRangeTest {

    private static RangeDistributionSpec buildSide(long tableId, int... colIds) {
        EquivalentDescriptor descriptor = new EquivalentDescriptor(tableId, List.of(1L));
        List<DistributionCol> cols = java.util.Arrays.stream(colIds)
                .mapToObj(id -> new DistributionCol(id, true))
                .collect(java.util.stream.Collectors.toList());
        descriptor.initDistributionUnionFind(cols);
        return new RangeDistributionSpec(cols, descriptor);
    }

    private static List<DistributionCol> cols(int... ids) {
        return java.util.Arrays.stream(ids)
                .mapToObj(id -> new DistributionCol(id, true))
                .collect(java.util.stream.Collectors.toList());
    }

    private static ChildOutputPropertyGuarantor newGuarantor(@Mocked TaskContext taskContext) {
        // ChildOutputPropertyGuarantor requires a TaskContext in its constructor;
        // we only invoke the private helper canRangeColocateJoin via reflection,
        // so the context internals are irrelevant.
        return new ChildOutputPropertyGuarantor(taskContext, null, null, null, null, null, 0.0);
    }

    private static boolean invokeGate(ChildOutputPropertyGuarantor guarantor,
                                      JoinOperator joinType,
                                      RangeDistributionSpec left,
                                      RangeDistributionSpec right,
                                      List<DistributionCol> leftShuffle,
                                      List<DistributionCol> rightShuffle) {
        return invokeGate(guarantor, "", joinType, left, right, leftShuffle, rightShuffle);
    }

    private static boolean invokeGate(ChildOutputPropertyGuarantor guarantor,
                                      String hint,
                                      JoinOperator joinType,
                                      RangeDistributionSpec left,
                                      RangeDistributionSpec right,
                                      List<DistributionCol> leftShuffle,
                                      List<DistributionCol> rightShuffle) {
        return Deencapsulation.invoke(guarantor, "canRangeColocateJoin",
                hint, joinType, left, right, leftShuffle, rightShuffle);
    }

    @Test
    void rejectsRightOuterJoin(@Mocked TaskContext taskContext) {
        // Join-type allowlist is the earliest gate; no ConnectContext access.
        RangeDistributionSpec left = buildSide(100L, 1, 2);
        RangeDistributionSpec right = buildSide(200L, 1, 2);
        assertFalse(invokeGate(newGuarantor(taskContext), JoinOperator.RIGHT_OUTER_JOIN,
                left, right, cols(1, 2), cols(1, 2)));
    }

    @Test
    void rejectsShuffleHint(@Mocked TaskContext taskContext) {
        // User-specified HINT_JOIN_SHUFFLE must short-circuit before ConnectContext
        // or canColocate — no ConnectContext expectations needed.
        RangeDistributionSpec left = buildSide(100L, 1, 2);
        RangeDistributionSpec right = buildSide(200L, 1, 2);
        assertFalse(invokeGate(newGuarantor(taskContext),
                HintNode.HINT_JOIN_SHUFFLE, JoinOperator.INNER_JOIN,
                left, right, cols(1, 2), cols(1, 2)));
    }

    @Test
    void rejectsSkewHint(@Mocked TaskContext taskContext) {
        RangeDistributionSpec left = buildSide(100L, 1, 2);
        RangeDistributionSpec right = buildSide(200L, 1, 2);
        assertFalse(invokeGate(newGuarantor(taskContext),
                HintNode.HINT_JOIN_SKEW, JoinOperator.INNER_JOIN,
                left, right, cols(1, 2), cols(1, 2)));
    }

    @Test
    void rejectsBucketHint(@Mocked TaskContext taskContext) {
        RangeDistributionSpec left = buildSide(100L, 1, 2);
        RangeDistributionSpec right = buildSide(200L, 1, 2);
        assertFalse(invokeGate(newGuarantor(taskContext),
                HintNode.HINT_JOIN_BUCKET, JoinOperator.INNER_JOIN,
                left, right, cols(1, 2), cols(1, 2)));
    }

    @Test
    void rejectsFullOuterJoin(@Mocked TaskContext taskContext) {
        // Join-type allowlist is the earliest gate; no ConnectContext access.
        RangeDistributionSpec left = buildSide(100L, 1, 2);
        RangeDistributionSpec right = buildSide(200L, 1, 2);
        assertFalse(invokeGate(newGuarantor(taskContext), JoinOperator.FULL_OUTER_JOIN,
                left, right, cols(1, 2), cols(1, 2)));
    }

    @Test
    void rejectsDisableColocateJoin(@Mocked TaskContext taskContext,
                                    @Mocked ConnectContext connectContext,
                                    @Mocked SessionVariable sessionVariable) {
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                connectContext.getSessionVariable();
                result = sessionVariable;
                sessionVariable.isDisableColocateJoin();
                result = true;
            }
        };
        RangeDistributionSpec left = buildSide(100L, 1, 2);
        RangeDistributionSpec right = buildSide(200L, 1, 2);
        assertFalse(invokeGate(newGuarantor(taskContext), JoinOperator.INNER_JOIN,
                left, right, cols(1, 2), cols(1, 2)));
    }

    @Test
    void rejectsDifferentShuffleArity(@Mocked TaskContext taskContext,
                                      @Mocked ConnectContext connectContext,
                                      @Mocked SessionVariable sessionVariable) {
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                connectContext.getSessionVariable();
                result = sessionVariable;
                sessionVariable.isDisableColocateJoin();
                result = false;
            }
        };
        RangeDistributionSpec left = buildSide(100L, 1, 2);
        RangeDistributionSpec right = buildSide(200L, 1, 2);
        // Left has 2 shuffle cols, right has 1 — mismatched arity.
        assertFalse(invokeGate(newGuarantor(taskContext), JoinOperator.INNER_JOIN,
                left, right, cols(1, 2), cols(1)));
    }

    @Test
    void acceptsIdentityJoinOnColocateColumns(@Mocked TaskContext taskContext,
                                              @Mocked ConnectContext connectContext,
                                              @Mocked SessionVariable sessionVariable,
                                              @Mocked GlobalStateMgr globalStateMgr,
                                              @Mocked ColocateTableIndex colocateTableIndex) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1L, 1L);
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                connectContext.getSessionVariable();
                result = sessionVariable;
                sessionVariable.isDisableColocateJoin();
                result = false;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isSameGroup(100L, 200L);
                result = true;
                colocateTableIndex.getGroup(100L);
                result = groupId;
                colocateTableIndex.getGroup(200L);
                result = groupId;
                colocateTableIndex.isGroupUnstable(groupId);
                result = false;
            }
        };
        // Both sides: colocate = (1, 2); shuffle = (1, 2) — identity pairing.
        RangeDistributionSpec left = buildSide(100L, 1, 2);
        RangeDistributionSpec right = buildSide(200L, 1, 2);
        assertTrue(invokeGate(newGuarantor(taskContext), JoinOperator.INNER_JOIN,
                left, right, cols(1, 2), cols(1, 2)));
    }

    @Test
    void rejectsSwappedColumnJoin(@Mocked TaskContext taskContext,
                                  @Mocked ConnectContext connectContext,
                                  @Mocked SessionVariable sessionVariable,
                                  @Mocked GlobalStateMgr globalStateMgr,
                                  @Mocked ColocateTableIndex colocateTableIndex) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1L, 1L);
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                connectContext.getSessionVariable();
                result = sessionVariable;
                sessionVariable.isDisableColocateJoin();
                result = false;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isSameGroup(100L, 200L);
                result = true;
                colocateTableIndex.getGroup(100L);
                result = groupId;
                colocateTableIndex.getGroup(200L);
                result = groupId;
                colocateTableIndex.isGroupUnstable(groupId);
                result = false;
            }
        };
        // Left colocate (1, 2); shuffle (1, 2) — OK.
        // Right colocate (1, 2); shuffle (2, 1) — swapped:
        //   rightColocate[0]=col1 paired with rightShuffle[0]=col2 (no match),
        //   rightColocate[0]=col1 paired with rightShuffle[1]=col1 (match at k=1),
        // but leftColocate[0]=col1 requires leftShuffle[k=1]=col2 which is not equivalent.
        // → pair not found → rejected.
        RangeDistributionSpec left = buildSide(100L, 1, 2);
        RangeDistributionSpec right = buildSide(200L, 1, 2);
        assertFalse(invokeGate(newGuarantor(taskContext), JoinOperator.INNER_JOIN,
                left, right, cols(1, 2), cols(2, 1)));
    }

    @Test
    void rejectsPartialShuffleCoverage(@Mocked TaskContext taskContext,
                                       @Mocked ConnectContext connectContext,
                                       @Mocked SessionVariable sessionVariable,
                                       @Mocked GlobalStateMgr globalStateMgr,
                                       @Mocked ColocateTableIndex colocateTableIndex) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1L, 1L);
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                connectContext.getSessionVariable();
                result = sessionVariable;
                sessionVariable.isDisableColocateJoin();
                result = false;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isSameGroup(100L, 200L);
                result = true;
                colocateTableIndex.getGroup(100L);
                result = groupId;
                colocateTableIndex.getGroup(200L);
                result = groupId;
                colocateTableIndex.isGroupUnstable(groupId);
                result = false;
            }
        };
        // Colocate cols = (1, 2); shuffle only covers (1) → partial coverage unsafe.
        RangeDistributionSpec left = buildSide(100L, 1, 2);
        RangeDistributionSpec right = buildSide(200L, 1, 2);
        assertFalse(invokeGate(newGuarantor(taskContext), JoinOperator.INNER_JOIN,
                left, right, cols(1), cols(1)));
    }

    @Test
    void acceptsLeftSemiJoin(@Mocked TaskContext taskContext,
                             @Mocked ConnectContext connectContext,
                             @Mocked SessionVariable sessionVariable,
                             @Mocked GlobalStateMgr globalStateMgr,
                             @Mocked ColocateTableIndex colocateTableIndex) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(1L, 1L);
        new Expectations() {
            {
                ConnectContext.get();
                result = connectContext;
                connectContext.getSessionVariable();
                result = sessionVariable;
                sessionVariable.isDisableColocateJoin();
                result = false;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getColocateTableIndex();
                result = colocateTableIndex;
                colocateTableIndex.isSameGroup(100L, 200L);
                result = true;
                colocateTableIndex.getGroup(100L);
                result = groupId;
                colocateTableIndex.getGroup(200L);
                result = groupId;
                colocateTableIndex.isGroupUnstable(groupId);
                result = false;
            }
        };
        RangeDistributionSpec left = buildSide(100L, 1, 2);
        RangeDistributionSpec right = buildSide(200L, 1, 2);
        assertTrue(invokeGate(newGuarantor(taskContext), JoinOperator.LEFT_SEMI_JOIN,
                left, right, cols(1, 2), cols(1, 2)));
    }
}
