// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.service;

import org.junit.Assert;
import org.junit.Test;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

import java.lang.reflect.Field;
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
    public void enableFQDNTest() throws UnknownHostException, 
                                        NoSuchFieldException, 
                                        SecurityException, 
                                        IllegalArgumentException, 
                                        IllegalAccessException {

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


        Field field = FrontendOptions.class.getDeclaredField("localAddr");
        field.setAccessible(true);
        field.set(null, addr);
        Field field1 = FrontendOptions.class.getDeclaredField("useFqdn");
        field1.setAccessible(true);
        
        field1.set(null, true);
        Assert.assertTrue(FrontendOptions.getLocalHostAddress().equals("sandbox"));
        field1.set(null, false);
        Assert.assertTrue(FrontendOptions.getLocalHostAddress().equals("127.0.0.10"));
    }

}
