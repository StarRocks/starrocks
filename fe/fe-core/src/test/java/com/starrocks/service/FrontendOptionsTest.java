// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.service;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FrontendOptionsTest {

    @Test
    public void CIDRTest() {

        List<String> priorityCidrs = FrontendOptions.priorityCidrs;
        priorityCidrs.add("192.168.5.136/32");

        FrontendOptions frontendOptions = new FrontendOptions();
        boolean inPriorNetwork = frontendOptions.isInPriorNetwork("127.0.0.1");
        Assert.assertEquals(false, inPriorNetwork);

        inPriorNetwork = frontendOptions.isInPriorNetwork("192.168.5.136");
        Assert.assertEquals(true, inPriorNetwork);

    }


}
