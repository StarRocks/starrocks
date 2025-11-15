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


package com.staros.service;

import com.staros.exception.ExceptionCode;
import com.staros.exception.StarException;
import com.staros.proto.ServiceInfo;
import com.staros.proto.ServiceState;
import com.staros.service.ServiceManager;
import com.staros.util.LockCloseable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class ServiceManagerTest {
    private String serviceTemplateName = "ServiceManagerTest";
    private String serviceName = "ServiceManagerTest-0";

    @BeforeClass
    public static void prepare() {
    }

    @Test
    public void testRegisterService() {
        ServiceManager serviceManager = new ServiceManager();

        serviceManager.registerService(serviceTemplateName, null);
        Assert.assertEquals(serviceManager.getServiceTemplateCount(), 1);

        try {
            serviceManager.registerService(serviceTemplateName, null);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.ALREADY_EXIST);
        }
    }

    @Test
    public void testDeregisterService() {
        ServiceManager serviceManager = new ServiceManager();

        serviceManager.registerService(serviceTemplateName, null);

        serviceManager.deregisterService(serviceTemplateName);
        Assert.assertEquals(serviceManager.getServiceTemplateCount(), 0);

        try {
            serviceManager.deregisterService(serviceTemplateName);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testBootstrapService() {
        ServiceManager serviceManager = new ServiceManager();

        String serviceId = "";
        try {
            serviceId = serviceManager.bootstrapService(serviceTemplateName, serviceName);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        serviceManager.registerService(serviceTemplateName, null);

        serviceId = serviceManager.bootstrapService(serviceTemplateName, serviceName);
        Assert.assertEquals(serviceManager.getServiceCount(), 1);

        UUID.fromString(serviceId);

        try {
            serviceId = serviceManager.bootstrapService(serviceTemplateName, serviceName);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.ALREADY_EXIST);
        }
    }

    @Test
    public void testShutdownService() {
        ServiceManager serviceManager = new ServiceManager();

        String serviceId = "1";
        try {
            serviceManager.shutdownService(serviceId);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        serviceManager.registerService(serviceTemplateName, null);

        try {
            serviceManager.shutdownService(serviceId);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        serviceId = serviceManager.bootstrapService(serviceTemplateName, serviceName);

        serviceManager.shutdownService(serviceId);
        Assert.assertEquals(serviceManager.getServiceCount(), 1);
    }

    @Test
    public void testGetService() {
        ServiceManager serviceManager = new ServiceManager();

        serviceManager.registerService(serviceTemplateName, null);

        try {
            serviceManager.getServiceInfoById("1");
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        String serviceId = serviceManager.bootstrapService(serviceTemplateName, serviceName);

        // test get service by id
        {
            ServiceInfo serviceInfo = serviceManager.getServiceInfoById(serviceId);
            Assert.assertEquals(serviceInfo.getServiceTemplateName(), serviceTemplateName);
            Assert.assertEquals(serviceInfo.getServiceName(), serviceName);
            Assert.assertEquals(serviceInfo.getServiceId(), serviceId);
            Assert.assertEquals(serviceInfo.getServiceState(), ServiceState.RUNNING);
        }

        // test get service by name
        {
            ServiceInfo serviceInfo = serviceManager.getServiceInfoByName(serviceName);
            Assert.assertEquals(serviceInfo.getServiceTemplateName(), serviceTemplateName);
            Assert.assertEquals(serviceInfo.getServiceName(), serviceName);
            Assert.assertEquals(serviceInfo.getServiceId(), serviceId);
            Assert.assertEquals(serviceInfo.getServiceState(), ServiceState.RUNNING);
        }

        try {
            serviceManager.getServiceInfoByName(serviceName + "aaa");
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testDeregisterServiceWhenServiceRunning() {
        ServiceManager serviceManager = new ServiceManager();

        serviceManager.registerService(serviceTemplateName, null);

        String serviceId = serviceManager.bootstrapService(serviceTemplateName, serviceName);

        try {
            serviceManager.deregisterService(serviceTemplateName);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_ALLOWED);
        }
    }

    @Test
    public void testLockServiceManager() {
        ServiceManager serviceManager = new ServiceManager();

        try (LockCloseable lock1 = new LockCloseable(serviceManager.readLock())) {
            try (LockCloseable lock2 = new LockCloseable(serviceManager.readLock())) {
                // leave empty by intention
            }
        }
        try (LockCloseable lock = new LockCloseable(serviceManager.writeLock())) {
            // leave empty by intention
        }
    }
}
