package com.starrocks.catalog;

import com.staros.client.StarClient;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.runners.statements.ExpectException;

public class StarOSAgentTest {
    private StarOSAgent starosAgent;

    @Mocked
    StarClient client;

    @Before
    public void setUp() throws Exception {
        starosAgent = new StarOSAgent();
    }

    @Test
    public void testRegisterAndBootstrapService() throws Exception {
        new Expectations() {
            {
                client.registerService("starrocks");
                minTimes = 0;
                result = null;

                client.bootstrapService("starrocks", "123");
                minTimes = 0;
                result = 1;
            }
        };
        starosAgent.registerAndBootstrapService("123");
        Assert.assertEquals(1, starosAgent.getServiceIdforTest());
    }

    @Test
    public void testGetServiceId() throws Exception {
        new Expectations() {
            {
                client.getServiceInfo("123").getServiceId();
                minTimes = 0;
                result = 2;
            }
        };

        starosAgent.getServiceId("123");
        Assert.assertEquals(2, starosAgent.getServiceIdforTest());
    }

     @Test
    public void TestAddWorker() throws Exception {
         new Expectations() {
             {
                 client.addWorker(1, "127.0.0.1:8090");
                 minTimes = 0;
                 result = 10;
             }
         };

        String workerHost = "127.0.0.1:8090";
        starosAgent.setServiceId(1);
        starosAgent.addWorker(5, workerHost);
        Assert.assertEquals(10, starosAgent.getWorkerId(workerHost));
    }
}
