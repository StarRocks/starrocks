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

package com.starrocks.sql.optimizer.rule.transformation.materialization.common;

import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
<<<<<<< HEAD
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
=======
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
>>>>>>> 6cf4b2f ([BugFix] Fix avg() on DECIMAL256 returning wrong result after aggregate push down (backport #77832) (#78393))
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AggregatePushDownUtilsTest {

    private static CallOperator buildAvg(ScalarType argType, ScalarType retType) {
        ColumnRefOperator arg = new ColumnRefOperator(1, argType, "c1", true);
        return new CallOperator(FunctionSet.AVG, retType, Lists.newArrayList(arg));
    }

    private static CallOperator rewriteAvg(ScalarType argType, ScalarType retType) {
        CallOperator avgFunc = buildAvg(argType, retType);
        ColumnRefOperator sumColRef = new ColumnRefOperator(2, retType, "sum_c1", true);
<<<<<<< HEAD
        ColumnRefOperator countColRef = new ColumnRefOperator(3, Type.BIGINT, "count_c1", true);
=======
        ColumnRefOperator countColRef = new ColumnRefOperator(3, IntegerType.BIGINT, "count_c1", true);
>>>>>>> 6cf4b2f ([BugFix] Fix avg() on DECIMAL256 returning wrong result after aggregate push down (backport #77832) (#78393))
        return AggregatePushDownUtils.createAvgBySumCount(avgFunc, sumColRef, countColRef);
    }

    @Test
    public void testCreateAvgBySumCountOnDecimal128() {
<<<<<<< HEAD
        ScalarType argType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 10);
=======
        ScalarType argType = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 10);
>>>>>>> 6cf4b2f ([BugFix] Fix avg() on DECIMAL256 returning wrong result after aggregate push down (backport #77832) (#78393))
        CallOperator newAvg = rewriteAvg(argType, argType);

        Assertions.assertEquals(FunctionSet.DIVIDE, newAvg.getFnName());
        // count is cast to the widest DECIMAL128 so that the divide's two operands share the same width
<<<<<<< HEAD
        Assertions.assertEquals(ScalarType.createDecimalV3NarrowestType(38, 0), newAvg.getChild(1).getType());
=======
        Assertions.assertEquals(TypeFactory.createDecimalV3NarrowestType(38, 0), newAvg.getChild(1).getType());
>>>>>>> 6cf4b2f ([BugFix] Fix avg() on DECIMAL256 returning wrong result after aggregate push down (backport #77832) (#78393))
    }

    @Test
    public void testCreateAvgBySumCountOnDecimal256() {
<<<<<<< HEAD
        ScalarType argType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL256, 76, 10);
=======
        ScalarType argType = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL256, 76, 10);
>>>>>>> 6cf4b2f ([BugFix] Fix avg() on DECIMAL256 returning wrong result after aggregate push down (backport #77832) (#78393))
        CallOperator newAvg = rewriteAvg(argType, argType);

        Assertions.assertEquals(FunctionSet.DIVIDE, newAvg.getFnName());
        // BE dispatches the divide on the expression type and reads both operands at that width, so a
        // DECIMAL256 divide must get a DECIMAL256 count - a DECIMAL128 one makes avg() return garbage.
        Assertions.assertTrue(newAvg.getChild(1).getType().isDecimal256(),
                "count operand should be DECIMAL256, but was " + newAvg.getChild(1).getType());
<<<<<<< HEAD
        Assertions.assertEquals(ScalarType.createDecimalV3NarrowestType(76, 0), newAvg.getChild(1).getType());
=======
        Assertions.assertEquals(TypeFactory.createDecimalV3NarrowestType(76, 0), newAvg.getChild(1).getType());
>>>>>>> 6cf4b2f ([BugFix] Fix avg() on DECIMAL256 returning wrong result after aggregate push down (backport #77832) (#78393))
    }
}
