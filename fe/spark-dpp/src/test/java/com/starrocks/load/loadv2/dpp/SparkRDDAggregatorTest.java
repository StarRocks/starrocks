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

package com.starrocks.load.loadv2.dpp;

import org.junit.Assert;
import org.junit.Test;

public class SparkRDDAggregatorTest {

    @Test
    public void testBitmapUnionAggregator() {
        // init null
        BitmapUnionAggregator aggregator = new BitmapUnionAggregator();
        BitmapValue value = aggregator.init(null);
        Assert.assertEquals(BitmapValue.EMPTY, value.getBitmapType());

        // init normal value 1
        aggregator = new BitmapUnionAggregator();
        value = aggregator.init(1);
        Assert.assertEquals(BitmapValue.SINGLE_VALUE, value.getBitmapType());
        Assert.assertEquals("{1}", value.toString());

        // init byte[]
        byte[] bytes = new byte[] {1, 1, 0, 0, 0};
        value = aggregator.init(bytes);
        Assert.assertEquals(BitmapValue.SINGLE_VALUE, value.getBitmapType());
        Assert.assertEquals("{1}", value.toString());
    }

    @Test
    public void testHllUnionAggregator() {
        HllUnionAggregator aggregator = new HllUnionAggregator();
        Hll value = aggregator.init(null);
        Assert.assertEquals(Hll.HLL_DATA_EMPTY, value.getType());
    }
}
