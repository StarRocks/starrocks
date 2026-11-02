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

package com.starrocks.sql.plan;

import org.junit.jupiter.api.Test;

public class LambdaTopNRuntimeFilterTest extends PlanTestBase {

    // ORDER BY plus LIMIT builds a TopN node, whose runtime-filter construction asks whether each
    // expression is bound by a set of tuples. That walk descends into the nested lambda body and meets
    // the slot the planner materialised for the cast around array_length() -- a slot with no column
    // that never joined a tuple. Dereferencing its absent parent threw
    // "Cannot invoke TupleDescriptor.getId() because SlotDescriptor.getParent() is null".
    private static final String NESTED_LAMBDA =
            "array_map(a -> array_map(b -> array_length(a) + b, a), ARRAY<ARRAY<TINYINT>>[ARRAY<TINYINT>[1,2]])";

    @Test
    public void testNestedLambdaUnderTopN() throws Exception {
        // Needs all of: ORDER BY, LIMIT, and an inner lambda referencing the outer parameter.
        getFragmentPlan("SELECT " + NESTED_LAMBDA + " ORDER BY 1 LIMIT 1");
        getFragmentPlan("SELECT " + NESTED_LAMBDA + " ORDER BY " + NESTED_LAMBDA + " DESC LIMIT 7, 100");
        getFragmentPlan("SELECT " + NESTED_LAMBDA + " ORDER BY 1 ASC LIMIT 100 OFFSET 7");
    }

    @Test
    public void testNeighbouringShapesStillPlan() throws Exception {
        // Each one drops a single ingredient, and none of them ever reached the null parent.
        getFragmentPlan("SELECT " + NESTED_LAMBDA);
        getFragmentPlan("SELECT " + NESTED_LAMBDA + " ORDER BY 1");
        getFragmentPlan("SELECT " + NESTED_LAMBDA + " LIMIT 1");
        getFragmentPlan("SELECT array_map(a -> array_map(b -> b, a), ARRAY<ARRAY<TINYINT>>[ARRAY<TINYINT>[1,2]]) "
                + "ORDER BY 1 LIMIT 1");
        getFragmentPlan("SELECT array_map(a -> a + 1, ARRAY<TINYINT>[1,2]) ORDER BY 1 LIMIT 1");
    }
}
