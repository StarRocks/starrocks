// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.common;

import com.starrocks.thrift.TVectorSearchOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VectorSearchOptionsTest {
    @Test
    public void testRangePresenceDistinguishesNegativeBoundFromAbsent() {
        VectorSearchOptions options = new VectorSearchOptions();

        TVectorSearchOptions withoutRange = options.toThrift();
        Assertions.assertFalse(withoutRange.isHas_vector_range());
        Assertions.assertTrue(options.getExplainString("").contains("Predicate Range: N/A"));

        options.setPredicateRange(-1.0);
        TVectorSearchOptions withNegativeRange = options.toThrift();
        Assertions.assertTrue(withNegativeRange.isHas_vector_range());
        Assertions.assertEquals(-1.0, withNegativeRange.getVector_range());
        Assertions.assertTrue(options.getExplainString("").contains("Predicate Range: -1.0"));
    }
}
