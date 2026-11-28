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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonAnnTopNRuleTest {
    private final ArrayType vectorType = new ArrayType(FloatType.FLOAT);
    private final ColumnRefOperator vectorColumn = new ColumnRefOperator(1, vectorType, "vec", false);
    private final ColumnRefOperator outputScore = new ColumnRefOperator(2, FloatType.FLOAT, "score", false);
    private final ArrayOperator vector = new ArrayOperator(vectorType, false,
            List.of(ConstantOperator.createFloat(1), ConstantOperator.createFloat(2)));
    private final PaimonAnnTopNRule rule = new PaimonAnnTopNRule();

    private OptExpression input(String function, boolean ascending, long limit, long offset, boolean filtered) {
        Column column = new Column("embedding", vectorType);
        var scan = new LogicalPaimonScanOperator(mock(PaimonTable.class), Map.of(vectorColumn, column),
                Map.of(column, vectorColumn), -1L, filtered ? ConstantOperator.createBoolean(true) : null);
        scan.setProjection(new Projection(Map.of(outputScore,
                new CallOperator(function, FloatType.FLOAT, List.of(vectorColumn, vector)))));
        var topN = new LogicalTopNOperator(List.of(new Ordering(outputScore, ascending, ascending)), limit, offset);
        return OptExpression.create(topN, OptExpression.create(scan));
    }

    private static OptimizerContext context(String mode) {
        var context = mock(OptimizerContext.class);
        var variables = new SessionVariable();
        variables.setPaimonGlobalIndexMode(mode);
        when(context.getSessionVariable()).thenReturn(variables);
        return context;
    }

    @Test
    void keepsScoreAndTopNWhileBindingTheActualColumn() {
        var original = input(FunctionSet.APPROX_L2_DISTANCE, true, 3, 2, false);
        var rewritten = rule.transform(original, context("auto")).get(0);
        assertSame(original.getOp(), rewritten.getOp());
        LogicalPaimonScanOperator scan = rewritten.inputAt(0).getOp().cast();
        assertSame(original.inputAt(0).getOp().getProjection(), scan.getProjection());
        assertEquals("embedding", scan.getAnnQuery().columnName());
        assertEquals(List.of(1F, 2F), scan.getAnnQuery().queryVector());
        assertEquals(5, scan.getAnnQuery().k());
        assertEquals("l2", scan.getAnnQuery().metric());
        assertNotEquals(original.inputAt(0).getOp(), scan);
        assertEquals(scan.getAnnQuery(), new PhysicalPaimonScanOperator(scan).getAnnQuery());
    }

    @Test
    void onlyRewritesCompatibleMetricDirectionAndQueryShape() {
        var auto = context("auto");
        for (String metric : List.of(FunctionSet.APPROX_COSINE_SIMILARITY, FunctionSet.APPROX_INNER_PRODUCT)) {
            assertEquals(1, rule.transform(input(metric, false, 3, 0, false), auto).size());
            assertTrue(rule.transform(input(metric, true, 3, 0, false), auto).isEmpty());
        }
        assertTrue(rule.transform(input(FunctionSet.APPROX_L2_DISTANCE, false, 3, 0, false), auto).isEmpty());
        assertTrue(rule.transform(input(FunctionSet.APPROX_L2_DISTANCE, true, 3, 0, true), auto).isEmpty());
        assertTrue(rule.transform(input(FunctionSet.APPROX_L2_DISTANCE, true, Integer.MAX_VALUE, 1, false), auto).isEmpty());
        assertTrue(rule.transform(input(FunctionSet.APPROX_L2_DISTANCE, true, 3, 0, false), context("off")).isEmpty());
    }
}
