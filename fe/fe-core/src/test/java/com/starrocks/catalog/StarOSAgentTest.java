// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.staros.client.StarClient;
import com.staros.client.StarClientException;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        Assert.assertEquals(1, starosAgent.getServiceId());
    }

    @Test
    public void testRegisterServiceException() throws Exception {
        new Expectations() {
            {
                client.registerService("starrocks");
                minTimes = 0;
                result = new StarClientException(StarClientException.ExceptionCode.ALREADY_EXIST,
                        "service already exists!");

                client.bootstrapService("starrocks", "123");
                minTimes = 0;
                result = 3;
            }
        };
        starosAgent.registerAndBootstrapService("123");
        Assert.assertEquals(3, starosAgent.getServiceId());
    }

    @Test
    public void testBootstrapServiceException() throws Exception {
        new Expectations() {
            {
                client.bootstrapService("starrocks", "123");
                minTimes = 0;
                result = new StarClientException(StarClientException.ExceptionCode.ALREADY_EXIST,
                        "service already exists!");

                client.getServiceInfo("123").getServiceId();
                minTimes = 0;
                result = 4;
            }
        };
        starosAgent.registerAndBootstrapService("123");
        Assert.assertEquals(4, starosAgent.getServiceId());
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
        Assert.assertEquals(2, starosAgent.getServiceId());
    }

    @Test
    public void testAddAndRemoveWorker() throws Exception {
         new Expectations() {
             {
                 client.addWorker(1, "127.0.0.1:8090");
                 minTimes = 0;
                 result = 10;

                 client.removeWorker(1, 10);
                 minTimes = 0;
                 result = null;
             }
         };

        String workerHost = "127.0.0.1:8090";
        starosAgent.setServiceId(1);
        starosAgent.addWorker(5, workerHost);
        Assert.assertEquals(10, starosAgent.getWorkerId(workerHost));

        starosAgent.removeWorker(workerHost);
        Assert.assertEquals(-1, starosAgent.getWorkerIdByBackendId(5));
    }


    @Test
    public void testAddWorkerException() throws Exception  {
        new Expectations() {
            {
                client.addWorker(1, "127.0.0.1:8090");
                minTimes = 0;
                result = new StarClientException(StarClientException.ExceptionCode.ALREADY_EXIST,
                        "worker already exists");

                client.getWorkerInfo(1, "127.0.0.1:8090").getWorkerId();
                minTimes = 0;
                result = 6;

            }
        };

        String workerHost = "127.0.0.1:8090";
        starosAgent.setServiceId(1);
        starosAgent.addWorker(5, workerHost);
        Assert.assertEquals(6, starosAgent.getWorkerId(workerHost));
        Assert.assertEquals(6, starosAgent.getWorkerIdByBackendId(5));
    }
}
