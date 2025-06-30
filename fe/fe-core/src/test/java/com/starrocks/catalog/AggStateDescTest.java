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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.catalog.combinator.AggStateDesc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AggStateDescTest {
    @Test
    public void testNewAggStateDesc() {
        AggregateFunction sum = AggregateFunction.createBuiltin(FunctionSet.SUM,
                Lists.<Type>newArrayList(Type.INT), Type.BIGINT, Type.BIGINT, false, true, false);
        AggStateDesc aggStateDesc = new AggStateDesc(sum);
        Assertions.assertEquals(1, aggStateDesc.getArgTypes().size());
        Assertions.assertEquals(Type.INT, aggStateDesc.getArgTypes().get(0));
        Assertions.assertEquals(Type.BIGINT, aggStateDesc.getReturnType());
        Assertions.assertEquals(FunctionSet.SUM, aggStateDesc.getFunctionName());
        Assertions.assertEquals(true, aggStateDesc.getResultNullable());
        Assertions.assertEquals("sum(int(11))", aggStateDesc.toSql());
        Assertions.assertEquals("sum(int(11))", aggStateDesc.toString());
        Assertions.assertNotNull(aggStateDesc.toThrift());
        try {
            Assertions.assertNotNull(aggStateDesc.getAggregateFunction());
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testCompareAggStateDesc() {
        AggregateFunction sum1 = AggregateFunction.createBuiltin(FunctionSet.SUM,
                Lists.<Type>newArrayList(Type.INT), Type.BIGINT, Type.BIGINT, false, true, false);
        AggStateDesc aggStateDesc1 = new AggStateDesc(sum1);
        AggregateFunction sum2 = AggregateFunction.createBuiltin(FunctionSet.SUM,
                Lists.<Type>newArrayList(Type.INT), Type.BIGINT, Type.BIGINT, false, true, false);
        AggStateDesc aggStateDesc2 = new AggStateDesc(sum2);
        Assertions.assertEquals(aggStateDesc1, aggStateDesc2);
        AggregateFunction sum3 = AggregateFunction.createBuiltin(FunctionSet.SUM,
                Lists.<Type>newArrayList(Type.FLOAT), Type.DOUBLE, Type.DOUBLE, false, true, false);
        AggStateDesc aggStateDesc3 = new AggStateDesc(sum3);
        Assertions.assertNotEquals(aggStateDesc3, aggStateDesc2);

        AggStateDesc cloned3 = aggStateDesc3.clone();
        Assertions.assertEquals(aggStateDesc3, cloned3);
    }
}
