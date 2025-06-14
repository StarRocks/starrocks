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
import org.junit.Assert;
import org.junit.Test;

public class AggStateDescTest {
    @Test
    public void testNewAggStateDesc() {
        AggregateFunction sum = AggregateFunction.createBuiltin(FunctionSet.SUM,
                Lists.<Type>newArrayList(Type.INT), Type.BIGINT, Type.BIGINT, false, true, false);
        AggStateDesc aggStateDesc = new AggStateDesc(sum);
        Assert.assertEquals(1, aggStateDesc.getArgTypes().size());
        Assert.assertEquals(Type.INT, aggStateDesc.getArgTypes().get(0));
        Assert.assertEquals(Type.BIGINT, aggStateDesc.getReturnType());
        Assert.assertEquals(FunctionSet.SUM, aggStateDesc.getFunctionName());
        Assert.assertEquals(true, aggStateDesc.getResultNullable());
        Assert.assertEquals("sum(int(11))", aggStateDesc.toSql());
        Assert.assertEquals("sum(int(11))", aggStateDesc.toString());
        Assert.assertNotNull(aggStateDesc.toThrift());
        try {
            Assert.assertNotNull(aggStateDesc.getAggregateFunction());
        } catch (Exception e) {
            Assert.fail();
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
        Assert.assertEquals(aggStateDesc1, aggStateDesc2);
        AggregateFunction sum3 = AggregateFunction.createBuiltin(FunctionSet.SUM,
                Lists.<Type>newArrayList(Type.FLOAT), Type.DOUBLE, Type.DOUBLE, false, true, false);
        AggStateDesc aggStateDesc3 = new AggStateDesc(sum3);
        Assert.assertNotEquals(aggStateDesc3, aggStateDesc2);

        AggStateDesc cloned3 = aggStateDesc3.clone();
        Assert.assertEquals(aggStateDesc3, cloned3);
    }
}
