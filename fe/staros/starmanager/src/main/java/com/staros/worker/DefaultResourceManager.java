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

package com.staros.worker;

import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.NotExistStarException;
import com.staros.proto.DeleteResourceRequest;
import com.staros.proto.DeleteResourceResponse;
import com.staros.proto.NodeInfo;
import com.staros.proto.ProvisionResourceRequest;
import com.staros.proto.ProvisionResourceResponse;
import com.staros.proto.ResourceProvisionerGrpc;
import com.staros.proto.ScaleResourceRequest;
import com.staros.proto.ScaleResourceResponse;
import com.staros.proto.Status;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerGroupState;
import com.staros.util.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * make RPC call to remote resource manager server to provision/release worker resources
 */
public class DefaultResourceManager implements ResourceManager {
    private static final Logger LOG = LogManager.getLogger(DefaultResourceManager.class);
    private static final String DEFAULT_RESOURCE_FILE = "default_worker_group_spec.json";

    private final WorkerManager workerManager;
    private final ResourceProvisionerGrpc.ResourceProvisionerBlockingStub blockingStub;
    private final ManagedChannel channel;
    private final Map<String, Map<String, String>> specMap;

    public DefaultResourceManager(WorkerManager workerManager, String rpcServerAddress) {
        this.workerManager = workerManager;
        if (rpcServerAddress.isEmpty()) {
            throw new InvalidArgumentStarException("Empty provision service address!");
        }

        String resourceFile = Config.RESOURCE_MANAGER_WORKER_GROUP_SPEC_RESOURCE_FILE;
        if (resourceFile.isEmpty()) {
            resourceFile = DEFAULT_RESOURCE_FILE;
        }
        try {
            LOG.info("Load resource file:{} for worker group spec definition.", resourceFile);
            String jsonString = Resources.toString(Resources.getResource(resourceFile), StandardCharsets.UTF_8);
            this.specMap = new Gson().fromJson(jsonString, new TypeToken<Map<String, Map<String, String>>>(){}.getType());
        } catch (IOException exception) {
            LOG.warn("Fail to load resource file: {}", resourceFile, exception);
            throw new InvalidArgumentStarException("Invalid resource file:{} content!", resourceFile);
        }

        LOG.info("Using {} as resource provision server!", rpcServerAddress);
        this.channel = ManagedChannelBuilder.forTarget(rpcServerAddress)
                .maxInboundMessageSize(Config.GRPC_CHANNEL_MAX_MESSAGE_SIZE)
                .usePlaintext()
                .build();
        this.blockingStub = ResourceProvisionerGrpc.newBlockingStub(channel);
    }

    @Override
    public void provisionResource(String serviceId, long groupId, WorkerGroupSpec spec, String owner) {
        Map<String, String> sizeConfig = getSpecDetails(spec);
        int numOfNodes = Integer.parseInt(sizeConfig.get("num_of_nodes"));
        ProvisionResourceRequest request = ProvisionResourceRequest.newBuilder()
                .setName(String.valueOf(groupId))
                .setNumOfNodes(numOfNodes)
                .setCpus(sizeConfig.get("cpus"))
                .setMemories(sizeConfig.get("memories"))
                .setImage(sizeConfig.get("image"))
                .build();
        try {
            ProvisionResourceResponse response = blockingStub.provisionResource(request);
            Status status = response.getStatus();
            if (status.getCode() != 0) {
                LOG.error("provisionResource RPC fails for request service:{}, group:{},  error with code:{}, message:{}",
                        serviceId, groupId, status.getCode(), status.getMessage());
                return;
            }
            List<NodeInfo> nodes = response.getInfosList();
            workerManager.addWorkers(serviceId, groupId, nodes.stream().map(NodeInfo::getHost).collect(Collectors.toList()));
            WorkerGroup group = workerManager.getWorkerGroupNoException(serviceId, groupId);
            if (group != null) {
                // TODO: whether need wait until new added workers are ON?
                group.updateState(WorkerGroupState.READY);
            }
            // TODO: rollback the resource provision or have GC thread takes care of the rest?
        } catch (Exception exception) {
            LOG.warn("Fail to send provisioning request for service:{}, group:{}. Error:", serviceId, groupId, exception);
        }
    }

    @Override
    public void alterResourceSpec(String serviceId, long groupId, WorkerGroupSpec spec) {
        Map<String, String> sizeConfig = getSpecDetails(spec);
        int numOfNodes = Integer.parseInt(sizeConfig.get("num_of_nodes"));
        ScaleResourceRequest request = ScaleResourceRequest.newBuilder()
                .setName(String.valueOf(groupId))
                .setNumOfNodes(numOfNodes)
                .setCpus(sizeConfig.get("cpus"))
                .setMemories(sizeConfig.get("memories"))
                .setImage(sizeConfig.get("image"))
                .build();
        try {
            ScaleResourceResponse response = blockingStub.scaleResource(request);
            Status status = response.getStatus();
            if (status.getCode() != 0) {
                LOG.error("ScaleResource RPC fails for request service:{}, group:{},  error with code:{}, message:{}",
                        serviceId, groupId, status.getCode(), status.getMessage());
                return;
            }
            List<NodeInfo> nodes = response.getInfosList();
            for (NodeInfo node : nodes) {
                try {
                    workerManager.getWorkerInfo(node.getHost());
                } catch (NotExistStarException exception) {
                    workerManager.addWorker(serviceId, groupId, node.getHost());
                }
            }
            // remove Non-exist workers
            List<String> hosts = nodes.stream().map(NodeInfo::getHost).collect(Collectors.toList());
            for (long id : workerManager.getWorkerGroup(serviceId, groupId).getAllWorkerIds(false)) {
                if (!hosts.contains(workerManager.getWorker(id).getIpPort())) {
                    workerManager.removeWorker(serviceId, groupId, id);
                }
            }
            WorkerGroup group = workerManager.getWorkerGroupNoException(serviceId, groupId);
            if (group != null) {
                // TODO: whether need wait until new added workers are ON?
                group.updateState(WorkerGroupState.READY);
            }
            // TODO: rollback the resource provision or have GC thread takes care of the rest?
        } catch (Exception exception) {
            LOG.warn("Fail to send provisioning request for service:{}, group:{}. Error:", serviceId, groupId, exception);
        }
    }

    @Override
    public void releaseResource(String serviceId, long groupId) {
        DeleteResourceRequest request = DeleteResourceRequest.newBuilder()
                .setName(String.valueOf(groupId))
                .build();
        try {
            DeleteResourceResponse response = blockingStub.deleteResource(request);
            Status status = response.getStatus();
            if (status.getCode() != 0) {
                LOG.error("releaseResource RPC fails for request service:{}, group:{},  error with code:{}, message:{}",
                        serviceId, groupId, status.getCode(), status.getMessage());
            } else {
                LOG.info("releaseResource RPC success for request, service:{}, group:{}", serviceId, groupId);
            }
        } catch (Exception exception) {
            LOG.warn("Fail to send provisioning request for service:{}, group:{}. Error:", serviceId, groupId, exception);
        }
    }

    @Override
    public boolean isValidSpec(WorkerGroupSpec spec) {
        return specMap.containsKey(spec.getSize());
    }

    private Map<String, String> getSpecDetails(WorkerGroupSpec spec) {
        if (!isValidSpec(spec)) {
            throw new InvalidArgumentStarException("Unsupported workerGroup spec: {}", spec.getSize());
        }
        return specMap.get(spec.getSize());
    }

    @Override
    public void stop() {
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (Exception exception) {
            LOG.info("Excepted while waiting for channel shutdown.", exception);
        }
    }
}
