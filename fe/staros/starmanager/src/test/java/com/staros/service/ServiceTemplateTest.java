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

import com.staros.proto.ServiceTemplateInfo;
import com.staros.service.ServiceTemplate;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ServiceTemplateTest {
    @Test
    public void testServiceTemplate() {
        String serviceTemplateName = "ServiceTemplateTest";
        ServiceTemplate serviceTemplate = new ServiceTemplate(serviceTemplateName, null);

        Assert.assertEquals(serviceTemplate.getServiceTemplateName(), serviceTemplateName);
    }

    @Test
    public void testSerialization() {
        String serviceTemplateName = "ServiceTemplateTest";
        List<String> serviceComponents = new ArrayList<>();
        serviceComponents.add("aaa");
        serviceComponents.add("bbb");
        ServiceTemplate serviceTemplate1 = new ServiceTemplate(serviceTemplateName, serviceComponents);

        // serialization
        ServiceTemplateInfo info = serviceTemplate1.toProtobuf();

        // deserialization
        ServiceTemplate serviceTemplate2 = ServiceTemplate.fromProtobuf(info);

        Assert.assertEquals(serviceTemplate1.getServiceTemplateName(), serviceTemplate2.getServiceTemplateName());
        Assert.assertEquals(serviceTemplate1.getServiceComponents().size(), serviceTemplate2.getServiceComponents().size());
    }
}
