// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.service;

import org.junit.Assert;
import org.junit.Test;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import com.starrocks.common.Config;

public class FrontendOptionsTest {

    @Mocked
    InetAddress addr;

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

    @Test
    public void enableFQDNTest() throws UnknownHostException {

        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getLocalHost() throws UnknownHostException {
                return addr;
            }
        };

        new Expectations(){
            {
                addr.getHostAddress();
                result = "127.0.0.10";
                addr.getCanonicalHostName();
                result = "sandbox";
            }
        };

        Config.enable_fqdn = true;
        FrontendOptions frontendOptions = new FrontendOptions();
        frontendOptions.init();
        InetAddress localAddr = FrontendOptions.getLocalHost();
        Assert.assertTrue(localAddr.getHostAddress().equals("127.0.0.10"));
        Assert.assertTrue(localAddr.getCanonicalHostName().equals("sandbox"));
    }

}
