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

package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Operator.Builder.withOperator copies predicate; it must copy predicateCommonOperators together with it.
// ScalarOperatorsReuseRule may extract a common sub-expression out of a predicate into predicateCommonOperators,
// so the predicate references columns defined only there. If withOperator kept the predicate but dropped
// predicateCommonOperators, the rebuilt operator would reference undefined columns and fail InputDependenciesChecker.
public class OperatorBuilderPredicateCommonOperatorsTest {

    @Test
    void withOperatorCopiesPredicateCommonOperators() {
        ColumnRefOperator reuseCol = new ColumnRefOperator(100, IntegerType.BIGINT, "cse", true);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, reuseCol, ConstantOperator.createBigint(1L));
        Map<ColumnRefOperator, ScalarOperator> common = Map.of(reuseCol, ConstantOperator.createBigint(1L));

        LogicalFilterOperator src = new LogicalFilterOperator(predicate);
        src.setPredicateCommonOperators(common);

        LogicalFilterOperator copy = new LogicalFilterOperator.Builder().withOperator(src).build();

        assertEquals(common, copy.getPredicateCommonOperators());
    }
}