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

package com.starrocks.sql.optimizer.operator.operator;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class PhysicalTopNOperatorTest {

    @Test
    public void testUsedColumns() {
        ColumnRefOperator orderColumn = new ColumnRefOperator(1, IntegerType.INT, "order_col", true);
        ColumnRefOperator partitionColumn = new ColumnRefOperator(2, IntegerType.INT, "partition_col", true);
        ColumnRefOperator preAggInputColumn = new ColumnRefOperator(3, IntegerType.INT, "pre_agg_input_col", true);
        ColumnRefOperator preAggOutputColumn = new ColumnRefOperator(4, IntegerType.BIGINT, "pre_agg_output_col", true);
        PhysicalTopNOperator topN = new PhysicalTopNOperator(
                new OrderSpec(List.of(new Ordering(orderColumn, true, true))),
                10,
                0,
                List.of(partitionColumn),
                1,
                SortPhase.PARTIAL,
                TopNType.ROW_NUMBER,
                false,
                false,
                false,
                null,
                null,
                Map.of(preAggOutputColumn,
                        new CallOperator(FunctionSet.SUM, IntegerType.BIGINT, List.of(preAggInputColumn))));

        ColumnRefSet usedColumns = topN.getUsedColumns();
        Assertions.assertTrue(usedColumns.contains(orderColumn));
        Assertions.assertTrue(usedColumns.contains(partitionColumn));
        Assertions.assertTrue(usedColumns.contains(preAggInputColumn));
        Assertions.assertFalse(usedColumns.contains(preAggOutputColumn));
    }
}
