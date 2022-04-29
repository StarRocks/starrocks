// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.common.util;

import org.junit.Assert;
import org.junit.Test;

public class NetUtilsTest {

    @Test
    public void testValidIPAddress() {
        boolean isIP1 = NetUtils.validIPAddress("127.0.0.1");
        Assert.assertTrue(isIP1);
        boolean isIP2 = NetUtils.validIPAddress("327.0.0.1");
        Assert.assertTrue(!isIP2);
        boolean isIP3 = NetUtils.validIPAddress("san.d.b.ox");
        Assert.assertTrue(!isIP3);
        boolean isIP4 = NetUtils.validIPAddress("1274.0.0.1");
        Assert.assertTrue(!isIP4);
        boolean isIP5 = NetUtils.validIPAddress("01.01.0.1");
        Assert.assertTrue(!isIP5);
        boolean isIP6 = NetUtils.validIPAddress("-127.0.0.1");
        Assert.assertTrue(!isIP6);
        boolean isIP7 = NetUtils.validIPAddress(".0.0.1");
        Assert.assertTrue(!isIP7);
        boolean isIP8 = NetUtils.validIPAddress("258.0.0.1");
        Assert.assertTrue(!isIP8);
    }
}
