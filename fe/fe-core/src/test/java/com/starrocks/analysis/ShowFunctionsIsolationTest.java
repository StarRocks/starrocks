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

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.thrift.TFunctionBinaryType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// Regression coverage for the Properties shown by SHOW [FULL] FUNCTIONS (Function#getProperties)
// exposing the "isolation" entry so users can tell whether a UDF/UDAF was created with
// isolation = "shared". isolationType == false means shared; true (default) means isolated.
public class ShowFunctionsIsolationTest {

    private static ScalarFunction scalarUdf(boolean isolationType) {
        Type[] args = new Type[] {Type.INT};
        return ScalarFunction.createUdf(new FunctionName("db", "my_udf"), args, Type.INT,
                false, TFunctionBinaryType.SRJAR, "objectFile", "symbol", "", "", isolationType);
    }

    private static AggregateFunction aggregateUdf(boolean isolationType) {
        AggregateFunction fn = new AggregateFunction(new FunctionName("db", "my_agg"),
                Lists.newArrayList(Type.INT), Type.INT, Type.INT, false);
        fn.setBinaryType(TFunctionBinaryType.SRJAR);
        fn.setIsolationType(isolationType);
        return fn;
    }

    @Test
    public void testSharedScalarFunctionShowsIsolation() {
        String props = scalarUdf(false).getProperties();
        Assertions.assertTrue(props.contains("\"isolation\":\"shared\""), props);
    }

    @Test
    public void testIsolatedScalarFunctionShowsIsolation() {
        String props = scalarUdf(true).getProperties();
        Assertions.assertTrue(props.contains("\"isolation\":\"isolated\""), props);
    }

    @Test
    public void testSharedAggregateFunctionShowsIsolation() {
        String props = aggregateUdf(false).getProperties();
        Assertions.assertTrue(props.contains("\"isolation\":\"shared\""), props);
    }

    @Test
    public void testIsolatedAggregateFunctionShowsIsolation() {
        String props = aggregateUdf(true).getProperties();
        Assertions.assertTrue(props.contains("\"isolation\":\"isolated\""), props);
    }
}
