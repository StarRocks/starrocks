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

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class ConstantOperatorUtilsTest {


    @Test
    public void getDoubleValue() {

        ConstantOperator constant0 = ConstantOperator.createTinyInt((byte) 1);
        ConstantOperator constant1 = ConstantOperator.createInt(1000);
        ConstantOperator constant2 = ConstantOperator.createSmallInt((short) 12);
        ConstantOperator constant3 = ConstantOperator.createBigint(1000000);

        ConstantOperator constant4 = ConstantOperator.createFloat(1.5);
        ConstantOperator constant5 = ConstantOperator.createDouble(6.789);

        ConstantOperator constant6 = ConstantOperator.createBoolean(true);
        ConstantOperator constant7 = ConstantOperator.createBoolean(false);

        ConstantOperator constant8 = ConstantOperator.createDate(LocalDateTime.of(2003, 10, 11, 23, 56, 25));
        ConstantOperator constant9 = ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 11, 23, 56, 25));
        ConstantOperator constant10 = ConstantOperator.createTime(124578990d);

        ConstantOperator constant11 = ConstantOperator.createVarchar("123");

        assertEquals(ConstantOperatorUtils.getDoubleValue(constant0), 1, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant1), 1000, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant2), 12, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant3), 1000000, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant4), 1.5, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant5), 6.789, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant6), 1, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant7), 0, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant8), 1065887785, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant9), 1065887785, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant10), 124578990, 0.1);

        assertEquals(ConstantOperatorUtils.getDoubleValue(constant11), Double.NaN, 0.0);

    }

}
