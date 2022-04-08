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
        boolean isIP3 = NetUtils.validIPAddress("sandbox");
        Assert.assertTrue(!isIP3);
    }
}
