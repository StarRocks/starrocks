// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.load.loadv2.dpp;

import com.starrocks.types.BitmapValue;
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
