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

import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.EquivalentDescriptor;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OutputPropertyDeriverTest extends PlanTestNoneDBBase {
    @Mocked
    private ExpressionContext context;

    @ParameterizedTest
    @EnumSource(value = HashDistributionDesc.SourceType.class, names = {"LOCAL", "BUCKET"})
    public void testFullOuterJoinPreservesChildEquivalence(HashDistributionDesc.SourceType rightSource) {
        DistributionCol bucketCol = new DistributionCol(1, true);
        DistributionCol leftJoinCol = new DistributionCol(2, true);
        DistributionCol rightJoinCol = new DistributionCol(3, true);
        EquivalentDescriptor leftDescriptor = new EquivalentDescriptor(100L, List.of(10L));
        leftDescriptor.unionDistributionCols(bucketCol, leftJoinCol);
        HashDistributionSpec leftSpec = new HashDistributionSpec(
                new HashDistributionDesc(List.of(bucketCol), HashDistributionDesc.SourceType.LOCAL), leftDescriptor);
        HashDistributionSpec rightSpec = new HashDistributionSpec(
                new HashDistributionDesc(List.of(rightJoinCol), rightSource));
        ColumnRefOperator leftKey = new ColumnRefOperator(2, IntegerType.INT, "left_key", true);
        ColumnRefOperator rightKey = new ColumnRefOperator(3, IntegerType.INT, "right_key", true);
        PhysicalHashJoinOperator join = new PhysicalHashJoinOperator(JoinOperator.FULL_OUTER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, leftKey, rightKey),
                "", -1, null, null, null, null, null);
        new Expectations() {
            {
                context.getChildOutputColumns(0);
                result = new ColumnRefSet(leftKey.getId());
                context.getChildOutputColumns(1);
                result = new ColumnRefSet(rightKey.getId());
            }
        };
        OutputPropertyDeriver deriver = new OutputPropertyDeriver(null, PhysicalPropertySet.EMPTY, List.of(
                new PhysicalPropertySet(DistributionProperty.createProperty(leftSpec)),
                new PhysicalPropertySet(DistributionProperty.createProperty(rightSpec))));

        PhysicalPropertySet output = deriver.visitPhysicalHashJoin(join, context);
        HashDistributionSpec outputSpec = (HashDistributionSpec) output.getDistributionProperty().getSpec();
        EquivalentDescriptor outputDescriptor = outputSpec.getEquivDesc();

        // The child remains usable by other memo alternatives after the parent's NULL extension.
        assertTrue(leftDescriptor.isConnected(leftJoinCol, bucketCol));
        assertFalse(leftDescriptor.isConnected(rightJoinCol.getNullRelaxCol(), bucketCol));
        assertNotSame(leftDescriptor, outputDescriptor);
        assertEquals(leftDescriptor.getTableId(), outputDescriptor.getTableId());
        assertEquals(leftDescriptor.getPartitionIds(), outputDescriptor.getPartitionIds());
        assertFalse(outputSpec.isAllNullStrict());
        assertFalse(outputDescriptor.isConnected(leftJoinCol, bucketCol));
        assertTrue(outputDescriptor.isConnected(rightJoinCol.getNullRelaxCol(), bucketCol.getNullRelaxCol()));
    }
}
