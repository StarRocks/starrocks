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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Maps;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.EquivalentDescriptor;
import com.starrocks.sql.optimizer.base.RangeDistributionSpec;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.type.IntegerType;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Direct unit test for
 * {@code OptExpressionDuplicator.OptExpressionDuplicatorVisitor#processRangeDistributionSpec}.
 * MV rewrite duplicates the scan with a new {@link ColumnRefFactory}; this
 * helper rewires the colocate columns and equivalence descriptor.
 */
class OptExpressionDuplicatorRangeTest {

    @Test
    void processRangeDistributionSpecRewritesColumnIdsAndEquivalence(
            @Mocked OptimizerContext optimizerContext) throws Exception {
        // Arrange: prevColumnRefFactory has two column refs at ids 1 and 2.
        ColumnRefFactory prevFactory = new ColumnRefFactory();
        ColumnRefOperator oldA = prevFactory.create("a", IntegerType.INT, true);
        ColumnRefOperator oldB = prevFactory.create("b", IntegerType.INT, true);

        // Column mapping: old ref → new ref with a fresh id.
        ColumnRefOperator newA = new ColumnRefOperator(1001, IntegerType.INT, "a", true);
        ColumnRefOperator newB = new ColumnRefOperator(1002, IntegerType.INT, "b", true);
        Map<ColumnRefOperator, ColumnRefOperator> columnMapping = Maps.newHashMap();
        columnMapping.put(oldA, newA);
        columnMapping.put(oldB, newB);

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(columnMapping);

        // Original range spec uses the old column ids.
        List<DistributionCol> oldCols = List.of(
                new DistributionCol(oldA.getId(), true),
                new DistributionCol(oldB.getId(), true));
        EquivalentDescriptor oldDesc = new EquivalentDescriptor(100L, List.of(10L, 20L));
        oldDesc.initDistributionUnionFind(oldCols);
        RangeDistributionSpec oldSpec = new RangeDistributionSpec(oldCols, oldDesc);

        // Construct the duplicator + access the inner visitor class.
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(prevFactory, optimizerContext);
        Class<?> visitorClass =
                Class.forName("com.starrocks.sql.optimizer.rule.transformation.materialization."
                        + "OptExpressionDuplicator$OptExpressionDuplicatorVisitor");
        Object visitor = Deencapsulation.newInnerInstance(
                visitorClass, duplicator,
                optimizerContext, columnMapping, rewriter, prevFactory, false, false);

        // Act: invoke the private helper.
        RangeDistributionSpec newSpec =
                Deencapsulation.invoke(visitor, "processRangeDistributionSpec", oldSpec);

        // Assert: column ids remapped, descriptor carries new ids but same
        // tableId and partition ids.
        assertNotNull(newSpec);
        assertEquals(2, newSpec.getColocateColumns().size());
        assertEquals(newA.getId(), newSpec.getColocateColumns().get(0).getColId());
        assertEquals(newB.getId(), newSpec.getColocateColumns().get(1).getColId());
        assertEquals(100L, newSpec.getEquivalentDescriptor().getTableId());
        assertEquals(List.of(10L, 20L), newSpec.getEquivalentDescriptor().getPartitionIds());
    }
}
