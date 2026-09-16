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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.Explain;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.UKFKConstraints;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.validate.ColumnReuseChecker;
import com.starrocks.sql.optimizer.validate.ConditionalTypeChecker;
import com.starrocks.type.DateType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AIProjectVisitorTest {
    @Test
    public void testExplainShowsPhysicalAIProjectWithoutRedispatchRecursion() {
        ColumnRefOperator input = new ColumnRefOperator(1, VarcharType.VARCHAR, "input", true);
        OptExpression root = physicalAIProject(Map.of(input, input), Map.of(), input);

        String explain = new Explain(false, false, "    ", "    ").print(root, List.of(input));
        Assertions.assertTrue(explain.contains("AI PROJECT"), explain);
    }

    @Test
    public void testConditionalTypeCheckerChecksPhysicalAISlotAndCommonMaps() {
        ColumnRefOperator date = new ColumnRefOperator(1, DateType.DATETIME, "date", true);
        ColumnRefOperator string = new ColumnRefOperator(2, VarcharType.VARCHAR, "string", true);
        ColumnRefOperator output = new ColumnRefOperator(3, VarcharType.VARCHAR, "output", true);
        ColumnRefOperator common = new ColumnRefOperator(4, VarcharType.VARCHAR, "common", true);
        CallOperator malformed = new CallOperator(FunctionSet.COALESCE, VarcharType.VARCHAR,
                List.of(date, string));

        Assertions.assertThrows(StarRocksPlannerException.class,
                () -> ConditionalTypeChecker.getInstance().validate(
                        physicalAIProject(Map.of(output, malformed), Map.of(), date, string), null));
        Assertions.assertThrows(StarRocksPlannerException.class,
                () -> ConditionalTypeChecker.getInstance().validate(
                        physicalAIProject(Map.of(output, output), Map.of(common, malformed), date, string), null));
    }

    @Test
    public void testColumnReuseCheckerChecksLogicalAndPhysicalAIMaps() {
        ColumnRefOperator input = new ColumnRefOperator(1, VarcharType.VARCHAR, "input", true);
        ColumnRefOperator output1 = new ColumnRefOperator(2, VarcharType.VARCHAR, "output1", true);
        ColumnRefOperator output2 = new ColumnRefOperator(3, VarcharType.VARCHAR, "output2", true);
        Map<ColumnRefOperator, ScalarOperator> reused = new LinkedHashMap<>();
        reused.put(output1, input);
        reused.put(output2, input);

        LogicalAIProjectOperator logical = new LogicalAIProjectOperator(reused, Map.of());
        OptExpression logicalRoot = OptExpression.create(logical, logicalLeaf(input));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ColumnReuseChecker.getInstance().validate(logicalRoot, null));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ColumnReuseChecker.getInstance().validate(
                        physicalAIProject(Map.of(output1, output1), reused, input), null));
    }

    @Test
    public void testUKFKCollectorInheritsThroughLogicalAndPhysicalAIProject() {
        ColumnRefOperator input = new ColumnRefOperator(1, VarcharType.VARCHAR, "input", false);
        UKFKConstraints childConstraints = new UKFKConstraints();
        childConstraints.addUniqueKey(input.getId(), new UKFKConstraints.UniqueConstraintWrapper(
                null, new ColumnRefSet(), true, ColumnRefSet.of(input)));

        OptExpression logicalChild = logicalLeaf(input);
        logicalChild.setConstraints(childConstraints);
        OptExpression logicalRoot = OptExpression.create(
                new LogicalAIProjectOperator(Map.of(input, input)), logicalChild);
        logicalRoot.deriveLogicalPropertyItself();
        UKFKConstraintsCollector.collectColumnConstraintsForce(logicalRoot);
        Assertions.assertNotNull(logicalRoot.getConstraints().getUniqueConstraint(input.getId()));

        OptExpression physicalChild = physicalLeaf(input);
        physicalChild.setConstraints(childConstraints);
        OptExpression physicalRoot = OptExpression.create(
                new PhysicalAIProjectOperator(Map.of(input, input), Map.of()), physicalChild);
        physicalRoot.setLogicalProperty(new LogicalProperty(ColumnRefSet.of(input)));
        UKFKConstraintsCollector.collectColumnConstraintsForce(physicalRoot);
        Assertions.assertNotNull(physicalRoot.getConstraints().getUniqueConstraint(input.getId()));
    }

    private static OptExpression physicalAIProject(Map<ColumnRefOperator, ScalarOperator> slots,
                                                   Map<ColumnRefOperator, ScalarOperator> common,
                                                   ColumnRefOperator... inputs) {
        return OptExpression.create(new PhysicalAIProjectOperator(slots, common), physicalLeaf(inputs));
    }

    private static OptExpression physicalLeaf(ColumnRefOperator... inputs) {
        List<ColumnRefOperator> columns = List.of(inputs);
        List<ScalarOperator> values = columns.stream()
                .map(column -> (ScalarOperator) ConstantOperator.createNull(column.getType())).toList();
        OptExpression leaf = OptExpression.create(new PhysicalValuesOperator(
                columns, List.of(values), Operator.DEFAULT_LIMIT, null, null));
        leaf.setLogicalProperty(new LogicalProperty(new ColumnRefSet(columns)));
        return leaf;
    }

    private static OptExpression logicalLeaf(ColumnRefOperator input) {
        OptExpression leaf = OptExpression.create(new MockOperator(OperatorType.LOGICAL_VALUES));
        leaf.setLogicalProperty(new LogicalProperty(ColumnRefSet.of(input)));
        return leaf;
    }
}
