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

package com.starrocks.common;

import com.google.common.collect.ImmutableMap;
import com.starrocks.thrift.TVectorSearchOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VectorSearchOptionsTest {
    @Test
    public void testQueryParamsToThrift() {
        VectorSearchOptions options = new VectorSearchOptions();
        options.setEnableUseANN(true);
        options.setLimitK(10);
        options.setDistanceColumnName("__vector_distance");
        options.setDistanceSlotId(7);
        options.setQueryVector(List.of("1.0", "2.0"));
        options.setQueryParams(ImmutableMap.of("lance.vector_column", "embedding", "lance.nprobes", "8"));

        TVectorSearchOptions thriftOptions = options.toThrift();

        Assertions.assertTrue(thriftOptions.isEnable_use_ann());
        Assertions.assertEquals(10, thriftOptions.getVector_limit_k());
        Assertions.assertEquals("__vector_distance", thriftOptions.getVector_distance_column_name());
        Assertions.assertEquals(7, thriftOptions.getVector_slot_id());
        Assertions.assertEquals(List.of("1.0", "2.0"), thriftOptions.getQuery_vector());
        Assertions.assertEquals("embedding", thriftOptions.getQuery_params().get("lance.vector_column"));
        Assertions.assertEquals("8", thriftOptions.getQuery_params().get("lance.nprobes"));
    }

    @Test
    public void testCopyDefensivelyCopiesMutableFields() {
        VectorSearchOptions options = new VectorSearchOptions();
        List<String> queryVector = new ArrayList<>(List.of("1.0"));
        Map<String, String> queryParams = new java.util.HashMap<>();
        queryParams.put("lance.vector_column", "embedding");
        options.setQueryVector(queryVector);
        options.setQueryParams(queryParams);

        VectorSearchOptions copy = options.copy();
        queryVector.add("2.0");
        queryParams.put("lance.vector_column", "other");

        Assertions.assertEquals(List.of("1.0"), copy.getQueryVector());
        Assertions.assertEquals("embedding", copy.getQueryParams().get("lance.vector_column"));
    }
}
