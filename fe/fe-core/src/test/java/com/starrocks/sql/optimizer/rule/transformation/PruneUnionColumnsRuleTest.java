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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PruneUnionColumnsRuleTest {

    /**
     * Test that pruneProjectionAndPropagateRequired correctly preserves base
     * output columns that are used by the Union's embedded projection.
     */
    @Test
    public void testPruneUnionWithEmbeddedProjectionPreservesUsedColumns() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, VarcharType.VARCHAR, "k2", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, DateType.DATETIME, "v2", true);

        ColumnRefOperator dateResult = new ColumnRefOperator(3, DateType.DATE, "date_result", true);
        CallOperator dateCall = new CallOperator("date", DateType.DATE, ImmutableList.of(col2));

        ColumnRefOperator child1Col1 = new ColumnRefOperator(4, VarcharType.VARCHAR, "c1_k2", true);
        ColumnRefOperator child1Col2 = new ColumnRefOperator(5, DateType.DATETIME, "c1_v2", true);
        ColumnRefOperator child2Col1 = new ColumnRefOperator(6, VarcharType.VARCHAR, "c2_k2", true);
        ColumnRefOperator child2Col2 = new ColumnRefOperator(7, DateType.DATETIME, "c2_v2", true);

        List<ColumnRefOperator> outputs = Lists.newArrayList(col1, col2);
        List<List<ColumnRefOperator>> childOutputs = Lists.newArrayList(
                Lists.newArrayList(child1Col1, child1Col2),
                Lists.newArrayList(child2Col1, child2Col2)
        );

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new HashMap<>();
        projectionMap.put(dateResult, dateCall);
        projectionMap.put(col1, col1);

        LogicalUnionOperator unionOp = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(outputs)
                .setChildOutputColumns(childOutputs)
                .isUnionAll(true)
                .setProjection(new Projection(projectionMap))
                .build();

        // Required: dateResult and col1; col2 is NOT directly required but IS used by date(col2)
        ColumnRefSet requiredColumns = new ColumnRefSet();
        requiredColumns.union(dateResult);
        requiredColumns.union(col1);

        PruneUnionColumnsRule rule = new PruneUnionColumnsRule();
        rule.pruneProjectionAndPropagateRequired(unionOp, requiredColumns);

        // col2 must now be in requiredColumns because the projection uses date(col2)
        Assertions.assertTrue(requiredColumns.contains(col2),
                "col2 should be added to required columns because projection uses date(col2)");
        Assertions.assertTrue(requiredColumns.contains(col1),
                "col1 should remain in required columns");

        // Projection should still contain both entries
        Projection resultProjection = unionOp.getProjection();
        Assertions.assertNotNull(resultProjection);
        Assertions.assertTrue(resultProjection.getColumnRefMap().containsKey(dateResult));
        Assertions.assertTrue(resultProjection.getColumnRefMap().containsKey(col1));
    }

    /**
     * Test that unrequired projection entries are properly pruned from the
     * Union's embedded projection.
     */
    @Test
    public void testPruneUnionRemovesUnrequiredProjectionEntries() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "k1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, DateType.DATETIME, "v1", true);

        ColumnRefOperator projOut1 = new ColumnRefOperator(3, DateType.DATE, "proj1", true);
        ColumnRefOperator projOut2 = new ColumnRefOperator(4, IntegerType.INT, "proj2", true);
        CallOperator dateCall = new CallOperator("date", DateType.DATE, ImmutableList.of(col2));

        List<ColumnRefOperator> outputs = Lists.newArrayList(col1, col2);
        List<List<ColumnRefOperator>> childOutputs = Lists.newArrayList();
        childOutputs.add(Lists.newArrayList(
                new ColumnRefOperator(5, IntegerType.INT, "c1_k1", true),
                new ColumnRefOperator(6, DateType.DATETIME, "c1_v1", true)
        ));

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new HashMap<>();
        projectionMap.put(projOut1, dateCall);
        projectionMap.put(projOut2, col1);

        LogicalUnionOperator unionOp = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(outputs)
                .setChildOutputColumns(childOutputs)
                .isUnionAll(true)
                .setProjection(new Projection(projectionMap))
                .build();

        // Only projOut2 is required (not projOut1)
        ColumnRefSet requiredColumns = new ColumnRefSet();
        requiredColumns.union(projOut2);

        PruneUnionColumnsRule rule = new PruneUnionColumnsRule();
        rule.pruneProjectionAndPropagateRequired(unionOp, requiredColumns);

        // col1 should be in required (used by projOut2), col2 should NOT (only used by unrequired projOut1)
        Assertions.assertTrue(requiredColumns.contains(col1),
                "col1 should be required (used by kept projOut2)");
        Assertions.assertFalse(requiredColumns.contains(col2),
                "col2 should NOT be required (only used by pruned projOut1)");

        // Projection should only contain projOut2
        Projection resultProjection = unionOp.getProjection();
        Assertions.assertNotNull(resultProjection);
        Assertions.assertFalse(resultProjection.getColumnRefMap().containsKey(projOut1),
                "projOut1 should be removed from projection");
        Assertions.assertTrue(resultProjection.getColumnRefMap().containsKey(projOut2),
                "projOut2 should be kept in projection");
    }

    /**
     * Test that pruneProjectionAndPropagateRequired is a no-op when Union
     * has no embedded projection.
     */
    @Test
    public void testPruneUnionWithoutProjectionUnchanged() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "k1", true);

        List<ColumnRefOperator> outputs = Lists.newArrayList(col1);
        List<List<ColumnRefOperator>> childOutputs = Lists.newArrayList();
        childOutputs.add(Lists.newArrayList(
                new ColumnRefOperator(2, IntegerType.INT, "c1_k1", true)
        ));

        LogicalUnionOperator unionOp = new LogicalUnionOperator(outputs, childOutputs, true);

        ColumnRefSet requiredColumns = new ColumnRefSet();
        requiredColumns.union(col1);

        PruneUnionColumnsRule rule = new PruneUnionColumnsRule();
        rule.pruneProjectionAndPropagateRequired(unionOp, requiredColumns);

        // No projection means no change
        Assertions.assertNull(unionOp.getProjection());
        Assertions.assertTrue(requiredColumns.contains(col1));
    }
}
