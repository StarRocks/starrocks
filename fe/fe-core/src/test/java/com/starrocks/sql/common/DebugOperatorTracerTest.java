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

package com.starrocks.sql.common;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.operator.AggType.GLOBAL;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DebugOperatorTracerTest {

    @Test
    public void testVisitPhysicalHashAggregateWithProjection() {
        // Create a PhysicalHashAggregateOperator with projection for testing
        List<ColumnRefOperator> groupByColumns = new ArrayList<>();
        ColumnRefOperator groupByCol = new ColumnRefOperator(1, Type.INT, "col1", true);
        groupByColumns.add(groupByCol);

        Map<ColumnRefOperator, CallOperator> aggregations = Maps.newHashMap();
        ColumnRefOperator aggCol = new ColumnRefOperator(2, Type.BIGINT, "sum_col1", true);
        CallOperator aggFunc = new CallOperator(
                "sum", Type.BIGINT, List.of(groupByCol), null, false);
        aggregations.put(aggCol, aggFunc);

        Map<ColumnRefOperator, ScalarOperator> projection = Maps.newHashMap();
        ColumnRefOperator projCol = new ColumnRefOperator(3, Type.INT, "proj_col1", true);
        projection.put(projCol, groupByCol);

        PhysicalHashAggregateOperator aggregateOperator = new PhysicalHashAggregateOperator(
                GLOBAL, groupByColumns, null, aggregations, true, 1, null,
                new Projection(projection));

        DebugOperatorTracer tracer = new DebugOperatorTracer();
        String result = tracer.visitPhysicalHashAggregate(aggregateOperator, null);

        // Verify that the result contains projection information
        assertTrue(result.contains("projection="));
    }
}