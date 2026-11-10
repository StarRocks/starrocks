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

package com.starrocks.sql.optimizer.rule.join;

import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ReorderJoinRuleTest {
    @BeforeEach
    public void setUp() {
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPruneUnionProjectionWithMissingChildStatistics(boolean firstChildHasStatistics) {
        // GIVEN
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(ConnectContext.get(), columnRefFactory);
        ColumnRefOperator leftKey = columnRefFactory.create("left_key", Type.INT, false);
        ColumnRefOperator rightKey = columnRefFactory.create("right_key", Type.INT, false);
        ColumnRefOperator unionKey = columnRefFactory.create("union_key", Type.INT, false);
        ColumnRefOperator unused = columnRefFactory.create("unused", Type.INT, false);
        ColumnRefOperator joinKey = columnRefFactory.create("join_key", Type.INT, false);

        // Projects make the missing statistics extend beyond the union's immediate children.
        OptExpression left = OptExpression.create(new LogicalProjectOperator(Map.of(leftKey, leftKey)),
                OptExpression.create(new LogicalValuesOperator(List.of(leftKey),
                        List.of(List.of(ConstantOperator.createInt(1))))));
        OptExpression right = OptExpression.create(new LogicalProjectOperator(Map.of(rightKey, rightKey)),
                OptExpression.create(new LogicalValuesOperator(List.of(rightKey),
                        List.of(List.of(ConstantOperator.createInt(2)), List.of(ConstantOperator.createInt(3))))));
        LogicalUnionOperator unionOperator = new LogicalUnionOperator(List.of(unionKey),
                List.of(List.of(leftKey), List.of(rightKey)), true);
        unionOperator.setProjection(new Projection(Map.of(unionKey, unionKey, unused, ConstantOperator.createInt(0))));
        OptExpression union = OptExpression.create(unionOperator, left, right);
        OptExpression join = OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN,
                        new BinaryPredicateOperator(BinaryType.EQ, unionKey, joinKey)), union,
                OptExpression.create(new LogicalValuesOperator(List.of(joinKey),
                        List.of(List.of(ConstantOperator.createInt(1))))));
        MvUtils.deriveLogicalProperty(join);
        if (firstChildHasStatistics) {
            Utils.calculateStatistics(left, context);
        }
        assertNull(union.getStatistics());
        assertNull(right.getStatistics());
        assertNull(right.inputAt(0).getStatistics());

        // WHEN
        // Reproduces the stack through visitLogicalJoin -> rewrite -> deriveNewOptExpression.
        OptExpression rewritten = new ReorderJoinRule.OutputColumnsPrune(context)
                .rewrite(join, new ColumnRefSet(List.of(unionKey)));

        // THEN
        OptExpression prunedUnion = rewritten.inputAt(0);
        assertEquals(Map.of(unionKey, unionKey), prunedUnion.getOp().getProjection().getColumnRefMap());
        assertEquals(new ColumnRefSet(List.of(unionKey)), prunedUnion.getOutputColumns());
        assertNotNull(rewritten.getStatistics());
        assertEquals(3, prunedUnion.getStatistics().getOutputRowCount(), 0.001);
        assertEquals(1, prunedUnion.getStatistics().getColumnStatistics().size());
        assertNotNull(prunedUnion.getStatistics().getColumnStatistic(unionKey));
        assertEquals(1, left.getStatistics().getOutputRowCount(), 0.001);
        assertEquals(2, right.getStatistics().getOutputRowCount(), 0.001);
        assertNotNull(left.inputAt(0).getStatistics());
        assertNotNull(right.inputAt(0).getStatistics());
    }
}
