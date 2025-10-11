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

import com.staros.filestore.FileStoreMgr;
import com.staros.proto.ServiceInfo;
import com.staros.proto.ServiceState;
import com.staros.shard.ShardManager;
import com.staros.util.Text;
import com.staros.util.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Service implements Writable {
    private final String serviceTemplateName;
    private final String serviceName;
    private final String serviceId;
    private ServiceState state;
    private ShardManager shardManager;
    private FileStoreMgr fileStoreMgr;

    public Service(String serviceTemplateName, String serviceName, String serviceId) {
        this.serviceTemplateName = serviceTemplateName;
        this.serviceName = serviceName;
        this.serviceId = serviceId;
        this.state = ServiceState.RUNNING;
        this.shardManager = null;
    }

    public String getServiceTemplateName() {
        return serviceTemplateName;
    }
    public String getServiceName() {
        return serviceName;
    }
    public String getServiceId() {
        return serviceId;
    }

    public ServiceState getState() {
        return state;
    }

    public boolean setState(ServiceState state) {
        if (this.state == state) {
            return false;
        }
        this.state = state;
        return true;
    }

    public ShardManager getShardManager() {
        return shardManager;
    }

    public void setShardManager(ShardManager shardManager) {
        this.shardManager = shardManager;
    }

    public void setFileStoreMgr(FileStoreMgr fileStoreMgr) {
        this.fileStoreMgr = fileStoreMgr;
    }

    public FileStoreMgr getFileStoreMgr() {
        return fileStoreMgr;
    }

    public ServiceInfo toProtobuf() {
        ServiceInfo.Builder builder = ServiceInfo.newBuilder()
                .setServiceTemplateName(serviceTemplateName)
                .setServiceName(serviceName)
                .setServiceId(serviceId)
                .setServiceState(state);
        return builder.build();
    }

    public static Service fromProtobuf(ServiceInfo info) {
        String serviceTemplateName = info.getServiceTemplateName();
        String serviceName = info.getServiceName();
        String serviceId = info.getServiceId();
        ServiceState state = info.getServiceState();
        Service service = new Service(serviceTemplateName, serviceName, serviceId);
        service.setState(state);
        return service;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = toProtobuf().toByteArray();
        Text.writeBytes(out, bytes);
    }

    public static Service read(DataInput in) throws IOException {
        byte[] bytes = Text.readBytes(in);
        ServiceInfo info = ServiceInfo.parseFrom(bytes);
        return Service.fromProtobuf(info);
    }
}
