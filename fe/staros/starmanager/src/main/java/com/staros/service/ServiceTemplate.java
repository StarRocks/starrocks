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

import com.staros.proto.ServiceComponentInfo;
import com.staros.proto.ServiceTemplateInfo;
import com.staros.util.Text;
import com.staros.util.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ServiceTemplate implements Writable {
    private final String serviceTemplateName;
    private final List<ServiceComponent> serviceComponents;

    public ServiceTemplate(String serviceTemplateName, List<String> serviceComponentNames) {
        this.serviceTemplateName = serviceTemplateName;
        this.serviceComponents = new ArrayList<>();
        if (serviceComponentNames != null) {
            for (String name : serviceComponentNames) {
                this.serviceComponents.add(new ServiceComponent(name));
            }
        }
    }

    public String getServiceTemplateName() {
        return serviceTemplateName;
    }

    public List<ServiceComponent> getServiceComponents() {
        return serviceComponents;
    }

    public ServiceTemplateInfo toProtobuf() {
        ServiceTemplateInfo.Builder builder = ServiceTemplateInfo.newBuilder();
        builder.setServiceTemplateName(serviceTemplateName);
        for (ServiceComponent sc : serviceComponents) {
            builder.addServiceComponentInfo(sc.toProtobuf());
        }
        return builder.build();
    }

    public static ServiceTemplate fromProtobuf(ServiceTemplateInfo info) {
        String name = info.getServiceTemplateName();
        List<String> componentNames = new ArrayList<>();
        List<ServiceComponentInfo> components = info.getServiceComponentInfoList();
        for (ServiceComponentInfo com : components) {
            componentNames.add(com.getServiceComponentName());
        }
        return new ServiceTemplate(name, componentNames);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = toProtobuf().toByteArray();
        Text.writeBytes(out, bytes);
    }

    public static ServiceTemplate read(DataInput in) throws IOException {
        byte[] bytes = Text.readBytes(in);
        ServiceTemplateInfo info = ServiceTemplateInfo.parseFrom(bytes);
        return ServiceTemplate.fromProtobuf(info);
    }
}
