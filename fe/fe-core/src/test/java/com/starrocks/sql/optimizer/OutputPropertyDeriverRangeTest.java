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
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.sql.optimizer.base.AnyDistributionSpec;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.EquivalentDescriptor;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.RangeDistributionSpec;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

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
        List<DistributionCol> cols = java.util.Arrays.stream(colIds)
                .mapToObj(id -> new DistributionCol(id, true))
                .collect(java.util.stream.Collectors.toList());
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
                new PhysicalPropertySet(
                        com.starrocks.sql.optimizer.base.DistributionProperty.createProperty(childSpec));

        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        ColumnRefOperator col3 = new ColumnRefOperator(3, IntegerType.INT, "c", true);
        // Both grouping subsets contain {1, 2} → intersection = {1, 2}.
        new Expectations() {
            {
                node.getRepeatColumnRef();
                result = java.util.Arrays.asList(
                        com.google.common.collect.Lists.newArrayList(col1, col2),
                        com.google.common.collect.Lists.newArrayList(col1, col2, col3));
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
                new PhysicalPropertySet(
                        com.starrocks.sql.optimizer.base.DistributionProperty.createProperty(childSpec));

        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        new Expectations() {
            {
                node.getRepeatColumnRef();
                result = java.util.Arrays.asList(
                        com.google.common.collect.Lists.newArrayList(col1, col2),
                        com.google.common.collect.Lists.newArrayList(col1));
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
                new PhysicalPropertySet(
                        com.starrocks.sql.optimizer.base.DistributionProperty.createProperty(childSpec));

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
        org.junit.jupiter.api.Assertions.assertTrue(
                equivDesc.isConnected(aliasCol, colocateCol),
                "alias should be connected to the colocate column in the descriptor");
    }

    @Test
    void updatePropertyWithProjectionNoOpOnNullProjection() throws Exception {
        RangeDistributionSpec childSpec = buildRangeSkeleton(100L, 1);
        PhysicalPropertySet childProperty =
                new PhysicalPropertySet(
                        com.starrocks.sql.optimizer.base.DistributionProperty.createProperty(childSpec));
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
}
