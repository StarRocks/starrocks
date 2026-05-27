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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.AnyDistributionSpec;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.EquivalentDescriptor;
import com.starrocks.sql.optimizer.base.GatherDistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.RangeDistributionSpec;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Direct unit tests for the range-aware branches of {@link OutputPropertyDeriver}:
 * scan rebuild with partitions, Repeat preserve/drop, projection remap.
 * {@code visitPhysicalJoin} range emission is exercised indirectly via
 * {@link ChildOutputPropertyGuarantorRangeTest}; here we keep the tests
 * constructor-driven to avoid a full {@code GroupExpression} fixture.
 */
class OutputPropertyDeriverRangeTest {

    private static OutputPropertyDeriver newDeriver(List<PhysicalPropertySet> childrenOutputProperties) {
        return new OutputPropertyDeriver(null, null, childrenOutputProperties);
    }

    private static RangeDistributionSpec buildRangeSkeleton(long tableId, int... colIds) {
        EquivalentDescriptor descriptor = new EquivalentDescriptor(tableId, Collections.emptyList());
        List<DistributionCol> cols = Arrays.stream(colIds)
                .mapToObj(id -> new DistributionCol(id, true))
                .collect(Collectors.toList());
        descriptor.initDistributionUnionFind(cols);
        return new RangeDistributionSpec(cols, descriptor);
    }

    @Test
    void visitPhysicalOlapScanRebuildsRangeSpecWithPartitions(
            @Mocked PhysicalOlapScanOperator node,
            @Mocked OlapTable table) {
        RangeDistributionSpec skeleton = buildRangeSkeleton(100L, 1, 2);
        List<Long> selectedPartitions = List.of(10L, 20L);
        new Expectations() {
            {
                node.getDistributionSpec();
                result = skeleton;
                node.getTable();
                result = table;
                node.getSelectedPartitionId();
                result = selectedPartitions;
                table.getId();
                result = 100L;
            }
        };

        OutputPropertyDeriver deriver = newDeriver(Collections.emptyList());
        // visitPhysicalOlapScan is public; call it directly. ExpressionContext
        // is unused for the range branch.
        PhysicalPropertySet output = deriver.visitPhysicalOlapScan(node, null);

        DistributionSpec spec = output.getDistributionProperty().getSpec();
        assertInstanceOf(RangeDistributionSpec.class, spec);
        RangeDistributionSpec rebuilt = (RangeDistributionSpec) spec;
        assertEquals(skeleton.getColocateColumns(), rebuilt.getColocateColumns());
        assertEquals(100L, rebuilt.getEquivalentDescriptor().getTableId());
        assertEquals(selectedPartitions, rebuilt.getEquivalentDescriptor().getPartitionIds());
    }

    @Test
    void visitPhysicalRepeatPreservesRangeWhenIntersectionCoversColocateCols(
            @Mocked PhysicalRepeatOperator node) {
        // Repeat intersection includes every colocate column id → preserve.
        RangeDistributionSpec childSpec = buildRangeSkeleton(100L, 1, 2);
        PhysicalPropertySet childProperty =
                new PhysicalPropertySet(DistributionProperty.createProperty(childSpec));

        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        ColumnRefOperator col3 = new ColumnRefOperator(3, IntegerType.INT, "c", true);
        // Both grouping subsets contain {1, 2} → intersection = {1, 2}.
        new Expectations() {
            {
                node.getRepeatColumnRef();
                result = Arrays.asList(
                        Lists.newArrayList(col1, col2),
                        Lists.newArrayList(col1, col2, col3));
            }
        };

        OutputPropertyDeriver deriver = newDeriver(List.of(childProperty));
        PhysicalPropertySet output = deriver.visitPhysicalRepeat(node, null);
        assertInstanceOf(RangeDistributionSpec.class,
                output.getDistributionProperty().getSpec());
    }

    @Test
    void visitPhysicalRepeatDropsRangeWhenColocateColNotInIntersection(
            @Mocked PhysicalRepeatOperator node) {
        // Intersection drops column 2 (only in first subset) → range dropped.
        RangeDistributionSpec childSpec = buildRangeSkeleton(100L, 1, 2);
        PhysicalPropertySet childProperty =
                new PhysicalPropertySet(DistributionProperty.createProperty(childSpec));

        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        new Expectations() {
            {
                node.getRepeatColumnRef();
                result = Arrays.asList(
                        Lists.newArrayList(col1, col2),
                        Lists.newArrayList(col1));
            }
        };

        OutputPropertyDeriver deriver = newDeriver(List.of(childProperty));
        PhysicalPropertySet output = deriver.visitPhysicalRepeat(node, null);
        // Output distribution should be EMPTY (AnyDistributionSpec); not range.
        assertSame(AnyDistributionSpec.INSTANCE,
                output.getDistributionProperty().getSpec());
    }

    @Test
    void updatePropertyWithProjectionExtendsRangeEquivalenceOnAlias(
            @Mocked Projection projection) {
        RangeDistributionSpec childSpec = buildRangeSkeleton(100L, 1);
        PhysicalPropertySet childProperty =
                new PhysicalPropertySet(DistributionProperty.createProperty(childSpec));

        // Projection renames column 1 → column 100 (identity ref).
        ColumnRefOperator aliased = new ColumnRefOperator(100, IntegerType.INT, "a_alias", true);
        ColumnRefOperator existing = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ImmutableMap<ColumnRefOperator, ScalarOperator> map = ImmutableMap.of(aliased, existing);
        new Expectations() {
            {
                projection.getColumnRefMap();
                result = map;
            }
        };

        OutputPropertyDeriver deriver = newDeriver(Collections.emptyList());
        Deencapsulation.invoke(deriver, "updatePropertyWithProjection", projection, childProperty);

        // The range spec's EquivalentDescriptor should now link col 1 with col 100
        // so a later lookup with col 100 finds col 1's class.
        EquivalentDescriptor equivDesc = childSpec.getEquivalentDescriptor();
        DistributionCol colocateCol = childSpec.getColocateColumns().get(0);
        DistributionCol aliasCol = new DistributionCol(100, true);
        assertTrue(equivDesc.isConnected(aliasCol, colocateCol),
                "alias should be connected to the colocate column in the descriptor");
    }

    @Test
    void updatePropertyWithProjectionNoOpOnNullProjection() throws Exception {
        RangeDistributionSpec childSpec = buildRangeSkeleton(100L, 1);
        PhysicalPropertySet childProperty =
                new PhysicalPropertySet(DistributionProperty.createProperty(childSpec));
        OutputPropertyDeriver deriver = newDeriver(Collections.emptyList());
        // null projection — early-return path. Use reflection directly since
        // JMockit's Deencapsulation.invoke rejects null args.
        Method method = OutputPropertyDeriver.class.getDeclaredMethod(
                "updatePropertyWithProjection", Projection.class, PhysicalPropertySet.class);
        method.setAccessible(true);
        method.invoke(deriver, null, childProperty);
        // No assertion needed beyond "did not throw".
    }

    @Test
    void updatePropertyWithProjectionNoOpOnUnsupportedSpec(@Mocked Projection projection) {
        // Pass an AnyDistributionSpec-backed property. updatePropertyWithProjection
        // should return without touching projection.
        PhysicalPropertySet childProperty = PhysicalPropertySet.EMPTY;
        OutputPropertyDeriver deriver = newDeriver(Collections.emptyList());
        Deencapsulation.invoke(deriver, "updatePropertyWithProjection", projection, childProperty);
        // No assertion needed beyond "did not throw and did not read projection".
    }

    // ---- F2 throw-path coverage ----

    private static HashDistributionSpec buildHashSpec(HashDistributionDesc.SourceType source, int... colIds) {
        EquivalentDescriptor descriptor = new EquivalentDescriptor(200L, Collections.emptyList());
        List<DistributionCol> cols = Arrays.stream(colIds)
                .mapToObj(id -> new DistributionCol(id, true))
                .collect(Collectors.toList());
        return new HashDistributionSpec(new HashDistributionDesc(cols, source), descriptor);
    }

    private static PhysicalPropertySet propertyOf(DistributionSpec spec) {
        return new PhysicalPropertySet(DistributionProperty.createProperty(spec));
    }

    /**
     * Stubs the join-operator and expression-context methods that
     * {@code OutputPropertyDeriver.visitPhysicalJoin} touches before it inspects
     * the child distribution specs.  With these stubs the throw paths under
     * test (lines 377, 419, 422) are reachable without a full Memo fixture.
     */
    private static void stubInnerJoinWithEmptyColumns(PhysicalHashJoinOperator node,
                                                      ExpressionContext context) {
        new Expectations() {
            {
                node.getJoinType();
                result = JoinOperator.INNER_JOIN;
                minTimes = 0;
                node.getOnPredicate();
                result = null;
                minTimes = 0;
                node.getJoinHint();
                result = null;
                minTimes = 0;
                context.getChildOutputColumns(0);
                result = new ColumnRefSet();
                minTimes = 0;
                context.getChildOutputColumns(1);
                result = new ColumnRefSet();
                minTimes = 0;
            }
        };
    }

    private static StarRocksPlannerException invokeVisitPhysicalJoinExpectingThrow(
            OutputPropertyDeriver deriver, PhysicalHashJoinOperator node, ExpressionContext context) {
        // visitPhysicalJoin is private; reflect to invoke it.  Wrap the
        // InvocationTargetException so JUnit's assertThrows sees the real cause.
        return assertThrows(StarRocksPlannerException.class, () -> {
            try {
                Method method = OutputPropertyDeriver.class.getDeclaredMethod(
                        "visitPhysicalJoin",
                        com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator.class,
                        ExpressionContext.class);
                method.setAccessible(true);
                method.invoke(deriver, node, context);
            } catch (java.lang.reflect.InvocationTargetException e) {
                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                }
                throw new RuntimeException(e.getCause());
            }
        });
    }

    @Test
    void visitPhysicalJoinThrowsOnMixedRangeHash(
            @Mocked PhysicalHashJoinOperator node,
            @Mocked ExpressionContext context) {
        stubInnerJoinWithEmptyColumns(node, context);
        OutputPropertyDeriver deriver = newDeriver(List.of(
                propertyOf(buildRangeSkeleton(100L, 1)),
                propertyOf(buildHashSpec(HashDistributionDesc.SourceType.SHUFFLE_JOIN, 2))));

        StarRocksPlannerException ex = invokeVisitPhysicalJoinExpectingThrow(deriver, node, context);
        assertTrue(ex.getMessage().contains("Mixed range-distribution"),
                "expected the mixed-range-hash diagnostic, got: " + ex.getMessage());
    }

    @Test
    void visitPhysicalJoinThrowsOnInnerInvalidHashCombo(
            @Mocked PhysicalHashJoinOperator node,
            @Mocked ExpressionContext context) {
        stubInnerJoinWithEmptyColumns(node, context);
        // Both are HashDistributionSpec → outer isShuffle && isShuffle is true.
        // (BUCKET, LOCAL) does not match colocate (L is not local), bucket-join
        // (R is not bucket), or shuffle-join (L/R neither shuffle nor enforce),
        // so falls through to the inner-else typed exception.
        OutputPropertyDeriver deriver = newDeriver(List.of(
                propertyOf(buildHashSpec(HashDistributionDesc.SourceType.BUCKET, 1)),
                propertyOf(buildHashSpec(HashDistributionDesc.SourceType.LOCAL, 2))));

        StarRocksPlannerException ex = invokeVisitPhysicalJoinExpectingThrow(deriver, node, context);
        assertTrue(ex.getMessage().contains("Children output property distribution error"),
                "expected the children-distribution diagnostic, got: " + ex.getMessage());
    }

    @Test
    void visitPhysicalJoinThrowsOnOuterNonShuffle(
            @Mocked PhysicalHashJoinOperator node,
            @Mocked ExpressionContext context) {
        stubInnerJoinWithEmptyColumns(node, context);
        // Two GatherDistributionSpec children: type=GATHER, so neither is
        // isShuffle() — falls through to the outer-else typed exception.
        OutputPropertyDeriver deriver = newDeriver(List.of(
                propertyOf(new GatherDistributionSpec()),
                propertyOf(new GatherDistributionSpec())));

        StarRocksPlannerException ex = invokeVisitPhysicalJoinExpectingThrow(deriver, node, context);
        assertTrue(ex.getMessage().contains("Children output property distribution error"),
                "expected the children-distribution diagnostic, got: " + ex.getMessage());
    }
}
