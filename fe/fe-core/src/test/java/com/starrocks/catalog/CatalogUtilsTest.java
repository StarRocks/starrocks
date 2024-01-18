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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CatalogUtilsTest {
    @Test
    public void testDivisibleByTwo() {
        Assertions.assertEquals(1, CatalogUtils.divisibleBucketNum(1));
        Assertions.assertEquals(2, CatalogUtils.divisibleBucketNum(2));
        Assertions.assertEquals(3, CatalogUtils.divisibleBucketNum(3));
        Assertions.assertEquals(4, CatalogUtils.divisibleBucketNum(4));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(5));
        Assertions.assertEquals(6, CatalogUtils.divisibleBucketNum(6));
        Assertions.assertEquals(7, CatalogUtils.divisibleBucketNum(7));
        Assertions.assertEquals(4, CatalogUtils.divisibleBucketNum(8));
        Assertions.assertEquals(3, CatalogUtils.divisibleBucketNum(9));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(10));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(11));
        Assertions.assertEquals(6, CatalogUtils.divisibleBucketNum(12));
        Assertions.assertEquals(6, CatalogUtils.divisibleBucketNum(13));
        Assertions.assertEquals(7, CatalogUtils.divisibleBucketNum(14));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(15));
        Assertions.assertEquals(4, CatalogUtils.divisibleBucketNum(16));
        Assertions.assertEquals(4, CatalogUtils.divisibleBucketNum(17));
        Assertions.assertEquals(3, CatalogUtils.divisibleBucketNum(18));
        Assertions.assertEquals(3, CatalogUtils.divisibleBucketNum(19));
        Assertions.assertEquals(5, CatalogUtils.divisibleBucketNum(20));
        Assertions.assertEquals(7, CatalogUtils.divisibleBucketNum(21));
    }
}
