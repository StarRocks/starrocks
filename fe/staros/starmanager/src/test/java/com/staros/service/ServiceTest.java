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

import com.staros.proto.ServiceInfo;
import com.staros.proto.ServiceState;
import com.staros.shard.ShardManager;
import org.junit.Assert;
import org.junit.Test;

public class ServiceTest {
    @Test
    public void testService() {
        String serviceTemplateName = "ServiceTest";
        String serviceName = "ServiceTest-0";
        String serviceId = "1";
        Service service = new Service(serviceTemplateName, serviceName, "1");

        Assert.assertEquals(service.getServiceTemplateName(), serviceTemplateName);
        Assert.assertEquals(service.getServiceName(), serviceName);
        Assert.assertEquals(service.getServiceId(), serviceId);
        Assert.assertEquals(service.getState(), ServiceState.RUNNING);

        Assert.assertEquals(service.getShardManager(), null);
        ShardManager shardManager = new ShardManager(serviceId, null, null, null);
        service.setShardManager(shardManager);
        Assert.assertEquals(service.getShardManager(), shardManager);

        Assert.assertEquals(service.setState(ServiceState.SHUTDOWN), true);
        Assert.assertEquals(service.setState(ServiceState.SHUTDOWN), false);
        Assert.assertEquals(service.getState(), ServiceState.SHUTDOWN);
    }

    @Test
    public void testSerialization() {
        String serviceTemplateName = "ServiceTest";
        String serviceName = "ServiceTest-0";
        String serviceId = "1";
        Service service1 = new Service(serviceTemplateName, serviceName, "1");

        // serialization
        ServiceInfo info = service1.toProtobuf();

        // deserialization
        Service service2 = Service.fromProtobuf(info);

        Assert.assertEquals(service1.getServiceTemplateName(), service2.getServiceTemplateName());
        Assert.assertEquals(service1.getServiceName(), service2.getServiceName());
        Assert.assertEquals(service1.getServiceId(), service2.getServiceId());
        Assert.assertEquals(service1.getState(), service2.getState());
    }
}
