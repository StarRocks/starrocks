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

package com.staros.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.staros.exception.StarException;
import com.staros.manager.StarManagerServer;
import com.staros.proto.AddFileStoreRequest;
import com.staros.proto.AddFileStoreResponse;
import com.staros.proto.AddWorkerRequest;
import com.staros.proto.AddWorkerResponse;
import com.staros.proto.AllocateFilePathRequest;
import com.staros.proto.AllocateFilePathResponse;
import com.staros.proto.BootstrapServiceRequest;
import com.staros.proto.BootstrapServiceResponse;
import com.staros.proto.CreateMetaGroupInfo;
import com.staros.proto.CreateMetaGroupRequest;
import com.staros.proto.CreateMetaGroupResponse;
import com.staros.proto.CreateShardGroupInfo;
import com.staros.proto.CreateShardGroupRequest;
import com.staros.proto.CreateShardGroupResponse;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.CreateShardRequest;
import com.staros.proto.CreateShardResponse;
import com.staros.proto.CreateWorkerGroupRequest;
import com.staros.proto.CreateWorkerGroupResponse;
import com.staros.proto.DeleteMetaGroupInfo;
import com.staros.proto.DeleteMetaGroupRequest;
import com.staros.proto.DeleteShardGroupInfo;
import com.staros.proto.DeleteShardGroupRequest;
import com.staros.proto.DeleteShardRequest;
import com.staros.proto.DeleteWorkerGroupRequest;
import com.staros.proto.DeregisterServiceRequest;
import com.staros.proto.DumpRequest;
import com.staros.proto.DumpResponse;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.GetFileStoreRequest;
import com.staros.proto.GetFileStoreResponse;
import com.staros.proto.GetMetaGroupRequest;
import com.staros.proto.GetMetaGroupResponse;
import com.staros.proto.GetServiceRequest;
import com.staros.proto.GetServiceResponse;
import com.staros.proto.GetShardGroupRequest;
import com.staros.proto.GetShardGroupResponse;
import com.staros.proto.GetShardRequest;
import com.staros.proto.GetShardResponse;
import com.staros.proto.GetWorkerRequest;
import com.staros.proto.GetWorkerResponse;
import com.staros.proto.LeaderInfo;
import com.staros.proto.ListFileStoreRequest;
import com.staros.proto.ListFileStoreResponse;
import com.staros.proto.ListMetaGroupRequest;
import com.staros.proto.ListMetaGroupResponse;
import com.staros.proto.ListShardGroupRequest;
import com.staros.proto.ListShardGroupResponse;
import com.staros.proto.ListShardRequest;
import com.staros.proto.ListShardResponse;
import com.staros.proto.ListWorkerGroupRequest;
import com.staros.proto.ListWorkerGroupResponse;
import com.staros.proto.MetaGroupInfo;
import com.staros.proto.QueryMetaGroupStableRequest;
import com.staros.proto.QueryMetaGroupStableResponse;
import com.staros.proto.RegisterServiceRequest;
import com.staros.proto.RemoveFileStoreRequest;
import com.staros.proto.RemoveWorkerRequest;
import com.staros.proto.ReplaceFileStoreRequest;
import com.staros.proto.ReplicationType;
import com.staros.proto.S3FileStoreInfo;
import com.staros.proto.ServiceInfo;
import com.staros.proto.ServiceTemplateInfo;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.ShardInfoList;
import com.staros.proto.ShutdownServiceRequest;
import com.staros.proto.StarManagerGrpc;
import com.staros.proto.StarStatus;
import com.staros.proto.StatusCode;
import com.staros.proto.UpdateFileStoreRequest;
import com.staros.proto.UpdateMetaGroupInfo;
import com.staros.proto.UpdateMetaGroupRequest;
import com.staros.proto.UpdateShardGroupInfo;
import com.staros.proto.UpdateShardGroupRequest;
import com.staros.proto.UpdateShardInfo;
import com.staros.proto.UpdateShardRequest;
import com.staros.proto.UpdateWorkerGroupRequest;
import com.staros.proto.UpdateWorkerGroupResponse;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerInfo;
import com.staros.util.LockCloseable;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class StarClient {
    private static final Logger LOG = LogManager.getLogger(StarClient.class);

    protected interface RpcCallable<V> {
        V call(StarManagerGrpc.StarManagerBlockingStub stub) throws StarClientException;
    }

    protected interface IpcCallable<V> {
        V call() throws StarException;
    }

    public static final long DEFAULT_ID = 0L;
    public static final int GRPC_CHANNEL_MAX_MESSAGE_SIZE = 64 * 1024 * 1024;

    private String defaultServerAddress = null;
    private ManagedChannel readChannel = null;
    private ManagedChannel writeChannel = null;
    private StarManagerGrpc.StarManagerBlockingStub readStub;
    private StarManagerGrpc.StarManagerBlockingStub writeStub;

    // when client and server are in the same process,
    // use server here to do direct memory access instead of rpc call.
    // only used for getShardInfo now
    private final StarManagerServer server;

    // protect the leader related info update
    private final ReentrantLock leaderLock = new ReentrantLock();
    private String leaderAddress = null;
    private ManagedChannel leaderReadChannel = null;
    private StarManagerGrpc.StarManagerBlockingStub leaderReadStub;
    private ManagedChannel leaderWriteChannel = null;
    private StarManagerGrpc.StarManagerBlockingStub leaderWriteStub;

    private int clientReadTimeoutSec = 15;
    private int clientListTimeoutSec = 30;
    private int clientWriteTimeoutSec = 30;
    private double clientReadMaxRetryCount = 0; // do not retry by default

    public static final FileStoreType FS_NOT_SET = FileStoreType.INVALID;

    // FOR TEST
    public StarClient() {
        this(null);
    }

    public StarClient(StarManagerServer server) {
        this.server = server;
    }

    public synchronized void resetStub(String address, boolean readOnly, boolean isLeader) {
        // prepare read stub
        ManagedChannel oldReadChannel = null;
        ManagedChannel oldWriteChannel = null;
        ManagedChannelBuilder readBuilder = ManagedChannelBuilder.forTarget(address)
                                                                .maxInboundMessageSize(GRPC_CHANNEL_MAX_MESSAGE_SIZE)
                                                                .usePlaintext();
        if (clientReadMaxRetryCount > 1) {
            Map<String, ?> retryPolicy = ImmutableMap.<String, Object>builder()
                    .put("maxAttempts", (Double) clientReadMaxRetryCount)
                    .put("initialBackoff", "1s")
                    .put("maxBackoff", "5s")
                    .put("backoffMultiplier", 1.5D)
                    .put("retryableStatusCodes", ImmutableList.of("UNAVAILABLE"))
                    .build();
            Map<String, ?> methodConfig = ImmutableMap.of(
                    "name", ImmutableList.of(ImmutableMap.of()), "retryPolicy", retryPolicy);
            Map<String, ?> serviceConfig =
                    ImmutableMap.of("methodConfig", ImmutableList.of(methodConfig));
            readBuilder.defaultServiceConfig(serviceConfig)
                       .maxRetryAttempts((int) clientReadMaxRetryCount)
                       .enableRetry();
        }
        if (!isLeader) {
            oldReadChannel = readChannel;
            readChannel = readBuilder.build();
            readStub = StarManagerGrpc.newBlockingStub(readChannel);
        } else {
            oldReadChannel = leaderReadChannel;
            leaderReadChannel = readBuilder.build();
            leaderReadStub = StarManagerGrpc.newBlockingStub(leaderReadChannel);
        }

        ManagedChannel oldReadChannelCopy = oldReadChannel;
        if (readOnly) {
            new Thread(() -> stopChannel(oldReadChannelCopy)).start();
            return;
        }

        // prepare write stub
        if (!isLeader) {
            oldWriteChannel = writeChannel;
            writeChannel = ManagedChannelBuilder.forTarget(address)
                                                .maxInboundMessageSize(GRPC_CHANNEL_MAX_MESSAGE_SIZE)
                                                .usePlaintext()
                                                .build();
            writeStub = StarManagerGrpc.newBlockingStub(writeChannel);
        } else {
            oldWriteChannel = leaderWriteChannel;
            leaderWriteChannel = ManagedChannelBuilder.forTarget(address)
                                                      .maxInboundMessageSize(GRPC_CHANNEL_MAX_MESSAGE_SIZE)
                                                      .usePlaintext()
                                                      .build();
            leaderWriteStub = StarManagerGrpc.newBlockingStub(leaderWriteChannel);
        }

        ManagedChannel oldWriteChannelCopy = oldWriteChannel;
        new Thread(() -> {
            stopChannel(oldReadChannelCopy);
            stopChannel(oldWriteChannelCopy);
        }).start();
    }

    public void connectServer(String serverIpPort) {
        this.defaultServerAddress = serverIpPort;

        resetStub(this.defaultServerAddress, false /* readOnly */, false /* isLeader */);
    }

    public void stop() {
        stopChannel(readChannel);
        stopChannel(writeChannel);
        stopChannel(leaderReadChannel);
        stopChannel(leaderWriteChannel);
    }

    private static void stopChannel(ManagedChannel ch) {
        if (ch == null) {
            return;
        }
        ch.shutdownNow();
        try {
            ch.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            // ignore the exception
        }
    }

    public void registerService(String serviceTemplateName) throws StarClientException {
        ServiceTemplateInfo serviceTemplate =
                ServiceTemplateInfo.newBuilder().setServiceTemplateName(serviceTemplateName).build();
        RegisterServiceRequest request =
                RegisterServiceRequest.newBuilder().setServiceTemplateInfo(serviceTemplate).build();
        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS).registerService(request));
    }

    public void deregisterService(String serviceTemplateName) throws StarClientException {
        DeregisterServiceRequest request =
                DeregisterServiceRequest.newBuilder().setServiceTemplateName(serviceTemplateName).build();
        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS).deregisterService(request));
    }

    public String bootstrapService(String serviceTemplateName, String serviceName) throws StarClientException {
        BootstrapServiceRequest request =
                BootstrapServiceRequest.newBuilder()
                        .setServiceTemplateName(serviceTemplateName)
                        .setServiceName(serviceName)
                        .build();
        BootstrapServiceResponse response = internalWriteRpcCall(
                (x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                        .bootstrapService(request));
        return response.getServiceId();
    }

    public void shutdownService(String serviceId) throws StarClientException {
        ShutdownServiceRequest request =
                ShutdownServiceRequest.newBuilder().setServiceId(serviceId).build();
        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS).shutdownService(request));
    }

    public ServiceInfo getServiceInfoById(String serviceId) throws StarClientException {
        GetServiceRequest request =
                GetServiceRequest.newBuilder()
                        .setServiceId(serviceId)
                        .build();
        GetServiceResponse response = internalReadRpcCall((x) -> x.withDeadlineAfter(clientReadTimeoutSec, TimeUnit.SECONDS)
                                                                  .getService(request));
        return response.getServiceInfo();
    }

    public ServiceInfo getServiceInfoByName(String serviceName) throws StarClientException {
        GetServiceRequest request =
                GetServiceRequest.newBuilder()
                        .setServiceName(serviceName)
                        .build();
        GetServiceResponse response = internalReadRpcCall((x) -> x.withDeadlineAfter(clientReadTimeoutSec, TimeUnit.SECONDS)
                                                                  .getService(request));
        return response.getServiceInfo();
    }

    public WorkerGroupDetailInfo createWorkerGroup(String serviceId, String owner, WorkerGroupSpec spec,
                                                   Map<String, String> labels, Map<String, String> properties)
            throws StarClientException {
        CreateWorkerGroupRequest request = CreateWorkerGroupRequest.newBuilder()
                .setServiceId(serviceId)
                .setOwner(owner)
                .setSpec(spec)
                .putAllLabels(labels)
                .putAllProperties(properties)
                .build();

        CreateWorkerGroupResponse response = internalWriteRpcCall(
                (x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                        .createWorkerGroup(request));
        return response.getGroupInfo();
    }

    public WorkerGroupDetailInfo createWorkerGroup(String serviceId, String owner, WorkerGroupSpec spec,
                                                   Map<String, String> labels, Map<String, String> properties,
                                                   int replicaNumber, ReplicationType replicationType)
            throws StarClientException {
        return createWorkerGroup(serviceId, owner, spec, labels, properties, replicaNumber, replicationType,
                WarmupLevel.WARMUP_NOT_SET);
    }

    public WorkerGroupDetailInfo createWorkerGroup(String serviceId, String owner, WorkerGroupSpec spec,
                                                   Map<String, String> labels, Map<String, String> properties,
                                                   int replicaNumber, ReplicationType replicationType,
                                                   WarmupLevel warmupLevel)
            throws StarClientException {
        CreateWorkerGroupRequest request = CreateWorkerGroupRequest.newBuilder()
                .setServiceId(serviceId)
                .setOwner(owner)
                .setSpec(spec)
                .putAllLabels(labels)
                .putAllProperties(properties)
                .setReplicaNumber(replicaNumber)
                .setReplicationType(replicationType)
                .setWarmupLevel(warmupLevel)
                .build();

        CreateWorkerGroupResponse response = internalWriteRpcCall(
                (x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                        .createWorkerGroup(request));
        return response.getGroupInfo();
    }

    public List<WorkerGroupDetailInfo> listWorkerGroup(String serviceId, List<Long> groupIds,
                                                       boolean includeWorkersInfo)
            throws StarClientException {
        return listWorkerGroupInternal(serviceId, groupIds, Collections.emptyMap(), includeWorkersInfo);
    }

    public List<WorkerGroupDetailInfo> listWorkerGroup(String serviceId, Map<String, String> filterLabels)
            throws StarClientException {
        return listWorkerGroupInternal(serviceId, Collections.emptyList(), filterLabels, false);
    }

    private List<WorkerGroupDetailInfo> listWorkerGroupInternal(
            String serviceId, List<Long> groupIds, Map<String, String> filterLabels, boolean includeWorkersInfo)
            throws StarClientException {
        if (server == null) {
            return listWorkerGroupInternalRPC(serviceId, groupIds, filterLabels, includeWorkersInfo);
        } else {
            return listWorkerGroupInternalIPC(serviceId, groupIds, filterLabels, includeWorkersInfo);
        }
    }

    private List<WorkerGroupDetailInfo> listWorkerGroupInternalRPC(
            String serviceId, List<Long> groupIds, Map<String, String> filterLabels, boolean includeWorkersInfo)
            throws StarClientException {
        ListWorkerGroupRequest request = ListWorkerGroupRequest.newBuilder()
                .setServiceId(serviceId)
                .addAllGroupIds(groupIds)
                .setIncludeWorkersInfo(includeWorkersInfo)
                .putAllFilterLabels(filterLabels)
                .build();
        ListWorkerGroupResponse response = internalReadRpcCall((x) -> x.withDeadlineAfter(clientListTimeoutSec, TimeUnit.SECONDS)
                                                                       .listWorkerGroup(request));
        return response.getGroupsInfoList();
    }

    private List<WorkerGroupDetailInfo> listWorkerGroupInternalIPC(
            String serviceId, List<Long> groupIds, Map<String, String> filterLabels, boolean includeWorkersInfo)
            throws StarClientException {

        try {
            return internalIpcCall(() -> server.getStarManager().listWorkerGroups(serviceId, groupIds,
                    filterLabels, includeWorkersInfo));
        } catch (StarClientException exception) {
            if (exception.getCode() == StatusCode.NOT_LEADER && leaderReadStub != null) {
                return listWorkerGroupInternalRPC(serviceId, groupIds,
                        filterLabels, includeWorkersInfo);
            } else {
                throw exception;
            }
        }
    }

    public WorkerGroupDetailInfo updateWorkerGroup(String serviceId, long groupId, Map<String, String> labels,
                                                   Map<String, String> properties, int replicaNumber,
                                                   ReplicationType replicationType)
            throws StarClientException {
        return updateWorkerGroup(serviceId, groupId, labels, properties, replicaNumber, replicationType,
                WarmupLevel.WARMUP_NOT_SET);
    }

    public WorkerGroupDetailInfo updateWorkerGroup(String serviceId, long groupId, Map<String, String> labels,
                                                   Map<String, String> properties, int replicaNumber,
                                                   ReplicationType replicationType, WarmupLevel warmupLevel)
                                                   throws StarClientException {
        UpdateWorkerGroupRequest.Builder reqBuilder = UpdateWorkerGroupRequest.newBuilder();
        reqBuilder.setGroupId(groupId).setServiceId(serviceId);
        if (labels != null && !labels.isEmpty()) {
            reqBuilder.putAllLabels(labels);
        }
        if (properties != null && !properties.isEmpty()) {
            reqBuilder.putAllProperties(properties);
        }
        if (replicaNumber > 0) {
            reqBuilder.setReplicaNumber(replicaNumber);
        }
        if (replicationType != ReplicationType.NO_SET) {
            reqBuilder.setReplicationType(replicationType);
        }
        if (warmupLevel != WarmupLevel.WARMUP_NOT_SET) {
            reqBuilder.setWarmupLevel(warmupLevel);
        }
        UpdateWorkerGroupResponse response = internalWriteRpcCall(
                (x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                        .updateWorkerGroup(reqBuilder.build()));
        return response.getGroupInfo();
    }

    /**
     * Specific interface for change WorkerGroup spec only. This will usually involve resource change.
     *
     * @param serviceId service id
     * @param groupId   group id
     * @param spec      new group spec
     * @return WorkerGroup detail info after spec changed.
     * @throws StarClientException throw StarClientException when the server responds error
     */
    public WorkerGroupDetailInfo alterWorkerGroupSpec(String serviceId, long groupId, WorkerGroupSpec spec)
            throws StarClientException {
        UpdateWorkerGroupRequest request = UpdateWorkerGroupRequest.newBuilder()
                .setServiceId(serviceId)
                .setSpec(spec)
                .setGroupId(groupId)
                .build();
        UpdateWorkerGroupResponse response = internalWriteRpcCall(
                (x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                        .updateWorkerGroup(request));
        return response.getGroupInfo();
    }

    public void deleteWorkerGroup(String serviceId, long groupId) throws StarClientException {
        DeleteWorkerGroupRequest request = DeleteWorkerGroupRequest.newBuilder()
                .setServiceId(serviceId)
                .setGroupId(groupId)
                .build();
        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS).deleteWorkerGroup(request));
    }

    public long addWorker(String serviceId, String workerIpPort) throws StarClientException {
        return addWorker(serviceId, workerIpPort, DEFAULT_ID);
    }

    public long addWorker(String serviceId, String workerIpPort, long workerGroupId) throws StarClientException {
        AddWorkerRequest request = AddWorkerRequest.newBuilder()
                .setServiceId(serviceId)
                .setIpPort(workerIpPort)
                .setGroupId(workerGroupId)
                .build();

        AddWorkerResponse response = internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                                                                  .addWorker(request));
        return response.getWorkerId();
    }

    public void removeWorker(String serviceId, long workerId) throws StarClientException {
        removeWorker(serviceId, workerId, DEFAULT_ID);
    }

    public void removeWorker(String serviceId, long workerId, long workerGroupId)
            throws StarClientException {
        RemoveWorkerRequest request =
                RemoveWorkerRequest.newBuilder()
                        .setServiceId(serviceId)
                        .setWorkerId(workerId)
                        .setGroupId(workerGroupId)
                        .build();

        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS).removeWorker(request));
    }

    public WorkerInfo getWorkerInfo(String serviceId, long workerId) throws StarClientException {
        GetWorkerRequest request =
                GetWorkerRequest.newBuilder()
                        .setServiceId(serviceId)
                        .setWorkerId(workerId)
                        .build();

        GetWorkerResponse response = internalReadRpcCall((x) -> x.withDeadlineAfter(clientReadTimeoutSec, TimeUnit.SECONDS)
                                                                 .getWorker(request));
        return response.getWorkerInfo();
    }

    public WorkerInfo getWorkerInfo(String serviceId, String ipPort) throws StarClientException {
        GetWorkerRequest request =
                GetWorkerRequest.newBuilder()
                        .setServiceId(serviceId)
                        .setIpPort(ipPort)
                        .build();

        GetWorkerResponse response = internalReadRpcCall((x) -> x.withDeadlineAfter(clientReadTimeoutSec, TimeUnit.SECONDS)
                                                                 .getWorker(request));
        return response.getWorkerInfo();
    }

    public List<ShardInfo> createShard(String serviceId, List<CreateShardInfo> createShardInfos)
            throws StarClientException {
        if (createShardInfos.isEmpty()) {
            throw new StarClientException(StatusCode.INVALID_ARGUMENT,
                    "shard info can not be empty.");
        }
        CreateShardRequest request =
                CreateShardRequest.newBuilder()
                        .setServiceId(serviceId)
                        .addAllCreateShardInfos(createShardInfos)
                        .build();

        CreateShardResponse response = internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                                                                    .createShard(request));
        return response.getShardInfoList();
    }

    public void updateShard(String serviceId, List<UpdateShardInfo> updateShardInfos)
            throws StarClientException {
        UpdateShardRequest request =
                UpdateShardRequest.newBuilder()
                        .setServiceId(serviceId)
                        .addAllUpdateShardInfos(updateShardInfos)
                        .build();

        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                                     .updateShard(request));
    }

    public void deleteShard(String serviceId, Set<Long> shardIds) throws StarClientException {
        if (shardIds.isEmpty()) {
            throw new StarClientException(StatusCode.INVALID_ARGUMENT,
                    "shard id can not be empty.");
        }
        List<Long> shardIdsFinal = new ArrayList<>(shardIds);
        DeleteShardRequest request =
                DeleteShardRequest.newBuilder()
                        .setServiceId(serviceId)
                        .addAllShardId(shardIdsFinal)
                        .build();

        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                                     .deleteShard(request));
    }

    public List<ShardInfo> getShardInfo(String serviceId, List<Long> shardIds) throws StarClientException {
        return getShardInfo(serviceId, shardIds, DEFAULT_ID);
    }

    /**
     * Get shard info of the specific service for the list of shards, optionally in specific worker group
     *
     * @param serviceId     target service id
     * @param shardIds      list of shards
     * @param workerGroupId target worker group.
     * @return shard info, includes replica info in the specific worker group
     * @throws StarClientException StarClientException
     * @apiNote When workerGroupId == DEFAULT_ID (0), the actual behavior is depended on server side configuration
     * Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY.
     * - true:  workerGroup-0 exists, get shard replica info from workerGroup-0
     * - false: workerGroup-0 doesn't exist, let server side determine which is the default workerGroup, and return
     * shards replica info from that worker group.
     */
    public List<ShardInfo> getShardInfo(String serviceId, List<Long> shardIds, long workerGroupId)
            throws StarClientException {
        if (shardIds.isEmpty()) {
            throw new StarClientException(StatusCode.INVALID_ARGUMENT, "shard id can not be empty.");
        }
        if (server == null) {
            return getShardInfoInternalRPC(serviceId, shardIds, workerGroupId, false);
        } else { // go directly with IPC call
            return getShardInfoInternalIPC(serviceId, shardIds, workerGroupId);
        }
    }

    private List<ShardInfo> getShardInfoInternalRPC(String serviceId, List<Long> shardIds, long workerGroupId,
                                                    boolean useLeader)
            throws StarClientException {
        GetShardRequest request = GetShardRequest.newBuilder()
                .setServiceId(serviceId)
                .addAllShardId(shardIds)
                .setWorkerGroupId(workerGroupId)
                .build();
        GetShardResponse response = internalReadRpcCallWithLeader(
                (x) -> x.withDeadlineAfter(clientReadTimeoutSec, TimeUnit.SECONDS)
                        .getShard(request), useLeader);
        return response.getShardInfoList();
    }

    private List<ShardInfo> getShardInfoInternalIPC(String serviceId, List<Long> shardIds, long workerGroupId)
            throws StarClientException {
        try {
            return internalIpcCall(() -> server.getStarManager().getShardInfo(serviceId, shardIds, workerGroupId));
        } catch (StarClientException exception) {
            if (exception.getCode() == StatusCode.NOT_LEADER && leaderReadStub != null) {
                return getShardInfoInternalRPC(serviceId, shardIds, workerGroupId, true);
            } else {
                throw exception;
            }
        }
    }

    public List<List<ShardInfo>> listShard(String serviceId, List<Long> groupIds) throws StarClientException {
        return listShard(serviceId, groupIds, DEFAULT_ID, false /* withoutReplicaInfo */);
    }

    /**
     * List shard info of the specific service for the list of shard groups, optionally in specific worker group
     *
     * @param serviceId     target service id
     * @param groupIds      list of shard group id
     * @param workerGroupId target worker group.
     * @return shard info, includes replica info in the specific worker group
     * @throws StarClientException StarClientException
     * @apiNote When workerGroupId == DEFAULT_ID (0), the actual behavior is depended on server side configuration
     * Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY.
     * - true:  workerGroup-0 exists, get shard replica info from workerGroup-0
     * - false: workerGroup-0 doesn't exist, let server side determine which is the default workerGroup, and return
     * shards replica info from that worker group.
     */
    public List<List<ShardInfo>> listShard(String serviceId, List<Long> groupIds, long workerGroupId,
            boolean withoutReplicaInfo) throws StarClientException {
        if (groupIds.isEmpty()) {
            throw new StarClientException(StatusCode.INVALID_ARGUMENT, "group id can not be empty.");
        }
        ListShardRequest request = ListShardRequest.newBuilder()
                .setServiceId(serviceId)
                .addAllGroupIds(groupIds)
                .setWorkerGroupId(workerGroupId)
                .setWithoutReplicaInfo(withoutReplicaInfo)
                .build();
        ListShardResponse response = internalReadRpcCall((x) -> x.withDeadlineAfter(clientListTimeoutSec, TimeUnit.SECONDS)
                                                                 .listShard(request));
        List<ShardInfoList> shardInfoLists = response.getShardInfoListsList();
        List<List<ShardInfo>> shardInfos = new ArrayList<>(shardInfoLists.size());
        for (ShardInfoList shardInfoList : shardInfoLists) {
            shardInfos.add(shardInfoList.getShardInfosList());
        }
        return shardInfos;
    }

    public List<ShardGroupInfo> createShardGroup(String serviceId, List<CreateShardGroupInfo> createShardGroupInfos)
            throws StarClientException {
        if (createShardGroupInfos.isEmpty()) {
            throw new StarClientException(StatusCode.INVALID_ARGUMENT,
                    "shard group info can not be empty.");
        }

        CreateShardGroupRequest request =
                CreateShardGroupRequest.newBuilder()
                        .setServiceId(serviceId)
                        .addAllCreateShardGroupInfos(createShardGroupInfos)
                        .build();

        CreateShardGroupResponse response = internalWriteRpcCall(
                (x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                        .createShardGroup(request));
        return response.getShardGroupInfosList();
    }

    public void deleteShardGroup(String serviceId, List<Long> groupIds, boolean deleteShards)
            throws StarClientException {
        if (groupIds.isEmpty()) {
            throw new StarClientException(StatusCode.INVALID_ARGUMENT,
                    "shard group id can not be empty.");
        }
        DeleteShardGroupRequest request =
                DeleteShardGroupRequest.newBuilder()
                        .setServiceId(serviceId)
                        .setDeleteInfo(
                                DeleteShardGroupInfo.newBuilder()
                                        .addAllGroupIds(groupIds)
                                        .setCascadeDeleteShard(deleteShards)
                                        .build())
                        .build();

        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                                     .deleteShardGroup(request));
    }

    public void updateShardGroup(String serviceId, List<UpdateShardGroupInfo> updateShardGroupInfos)
            throws StarClientException {
        UpdateShardGroupRequest request =
                UpdateShardGroupRequest.newBuilder()
                        .setServiceId(serviceId)
                        .addAllUpdateShardGroupInfos(updateShardGroupInfos)
                        .build();

        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                                     .updateShardGroup(request));
    }

    // return Pair<shardGroupInfo, nextGroupId>
    public Pair<List<ShardGroupInfo>, Long> listShardGroup(String serviceId, long startGroupId)
            throws StarClientException {
        ListShardGroupRequest request = ListShardGroupRequest.newBuilder()
                .setServiceId(serviceId)
                .setIncludeAnonymousGroup(false) // expose later
                .setStartGroupId(startGroupId)
                .build();
        ListShardGroupResponse response = internalReadRpcCall((x) ->
                x.withDeadlineAfter(clientListTimeoutSec, TimeUnit.SECONDS)
                .listShardGroup(request));
        return Pair.of(response.getShardGroupInfosList(), response.getNextGroupId());
    }

    public List<ShardGroupInfo> listShardGroup(String serviceId) throws StarClientException {
        long startGroupId = 0;
        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>();
        for (;;) {
            Pair<List<ShardGroupInfo>, Long> pair = listShardGroup(serviceId, startGroupId);
            shardGroupInfos.addAll(pair.getKey());
            startGroupId = pair.getValue();
            if (startGroupId == 0L) {
                break;
            }
        }
        return shardGroupInfos;
    }

    public List<ShardGroupInfo> getShardGroup(String serviceId, List<Long> groupIds)
            throws StarClientException {
        if (groupIds.isEmpty()) {
            throw new StarClientException(StatusCode.INVALID_ARGUMENT, "group id can not be empty.");
        }
        GetShardGroupRequest request = GetShardGroupRequest.newBuilder()
                .setServiceId(serviceId)
                .addAllShardGroupId(groupIds)
                .build();
        GetShardGroupResponse response = internalReadRpcCall((x) -> x.withDeadlineAfter(clientReadTimeoutSec, TimeUnit.SECONDS)
                                                                     .getShardGroup(request));
        return response.getShardGroupInfoList();
    }

    public MetaGroupInfo createMetaGroup(String serviceId, CreateMetaGroupInfo info) throws StarClientException {
        CreateMetaGroupRequest request = CreateMetaGroupRequest.newBuilder()
                .setServiceId(serviceId)
                .setCreateMetaGroupInfo(info)
                .build();

        CreateMetaGroupResponse response = internalWriteRpcCall(
                (x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                        .createMetaGroup(request));
        return response.getMetaGroupInfo();
    }

    public void deleteMetaGroup(String serviceId, DeleteMetaGroupInfo info) throws StarClientException {
        DeleteMetaGroupRequest request = DeleteMetaGroupRequest.newBuilder()
                .setServiceId(serviceId)
                .setDeleteMetaGroupInfo(info)
                .build();

        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS).deleteMetaGroup(request));
    }

    public void updateMetaGroup(String serviceId, UpdateMetaGroupInfo info) throws StarClientException {
        UpdateMetaGroupRequest request = UpdateMetaGroupRequest.newBuilder()
                .setServiceId(serviceId)
                .setUpdateMetaGroupInfo(info)
                .build();

        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS).updateMetaGroup(request));
    }

    public MetaGroupInfo getMetaGroupInfo(String serviceId, long metaGroupId) throws StarClientException {
        GetMetaGroupRequest request = GetMetaGroupRequest.newBuilder()
                .setServiceId(serviceId)
                .setMetaGroupId(metaGroupId)
                .build();

        GetMetaGroupResponse response = internalReadRpcCall((x) -> x.withDeadlineAfter(clientReadTimeoutSec, TimeUnit.SECONDS)
                                                                    .getMetaGroup(request));
        return response.getMetaGroupInfo();
    }

    public List<MetaGroupInfo> listMetaGroup(String serviceId) throws StarClientException {
        ListMetaGroupRequest request = ListMetaGroupRequest.newBuilder()
                .setServiceId(serviceId)
                .build();

        ListMetaGroupResponse response = internalReadRpcCall((x) -> x.withDeadlineAfter(clientListTimeoutSec, TimeUnit.SECONDS)
                                                                     .listMetaGroup(request));
        return response.getMetaGroupInfosList();
    }

    public boolean queryMetaGroupStable(String serviceId, long metaGroupId) throws StarClientException {
        return queryMetaGroupStable(serviceId, metaGroupId, DEFAULT_ID);
    }

    public boolean queryMetaGroupStable(String serviceId, long metaGroupId, long workerGroupId)
            throws StarClientException {
        QueryMetaGroupStableRequest.Builder reqBuilder = QueryMetaGroupStableRequest.newBuilder()
                .setServiceId(serviceId)
                .setMetaGroupId(metaGroupId);
        if (workerGroupId != DEFAULT_ID) {
            reqBuilder.setWorkerGroupId(workerGroupId);
        }
        QueryMetaGroupStableResponse response =
                internalReadRpcCall((x) -> x.withDeadlineAfter(clientReadTimeoutSec, TimeUnit.SECONDS)
                                            .queryMetaGroupStable(reqBuilder.build()));
        return response.getIsStable();
    }

    public FilePathInfo allocateFilePath(String serviceId, FileStoreType fsType, String suffix)
            throws StarClientException {
        return allocateFilePath(serviceId, fsType, suffix, "", "");
    }

    public FilePathInfo allocateFilePath(String serviceId, String fsKey, String suffix, String customRootDir)
            throws StarClientException {
        return allocateFilePath(serviceId, FS_NOT_SET, suffix, fsKey, customRootDir);
    }

    public FilePathInfo allocateFilePath(String serviceId, String fsKey, String suffix) throws StarClientException {
        return allocateFilePath(serviceId, FS_NOT_SET, suffix, fsKey, "");
    }

    private FilePathInfo allocateFilePath(String serviceId, FileStoreType fsType,
                                          String suffix, String fsKey, String customRootDir) throws StarClientException {
        AllocateFilePathRequest request =
                AllocateFilePathRequest.newBuilder()
                        .setServiceId(serviceId)
                        .setSuffix(suffix)
                        .setFsType(fsType)
                        .setFsKey(fsKey)
                        .setCustomRootDir(customRootDir)
                        .build();

        AllocateFilePathResponse response = internalWriteRpcCall(
                (x) -> x.withDeadlineAfter(clientReadTimeoutSec, TimeUnit.SECONDS)
                        .allocateFilePath(request));
        return response.getPathInfo();
    }

    public String addFileStore(FileStoreInfo info, String serviceId) throws StarClientException {
        if (info.getFsName().isEmpty()) {
            throw new StarClientException(StatusCode.INVALID_ARGUMENT, "Fs name can not be empty");
        }

        AddFileStoreRequest request = AddFileStoreRequest.newBuilder().setFsInfo(info).setServiceId(serviceId).build();
        AddFileStoreResponse response = internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS)
                                                                     .addFileStore(request));
        return response.getFsKey();
    }

    public void removeFileStore(String fsKey, String serviceId) throws StarClientException {
        removeFileStoreInternal(fsKey, "", serviceId);
    }

    public void removeFileStoreByName(String fsName, String serviceId) throws StarClientException {
        removeFileStoreInternal("", fsName, serviceId);
    }

    private void removeFileStoreInternal(String fsKey, String fsName, String serviceId) throws StarClientException {
        RemoveFileStoreRequest request = RemoveFileStoreRequest.newBuilder().setFsName(fsName).setFsKey(fsKey)
                .setServiceId(serviceId).build();
        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS).removeFileStore(request));
    }

    public void updateFileStore(FileStoreInfo info, String serviceId) throws StarClientException {
        UpdateFileStoreRequest request = UpdateFileStoreRequest.newBuilder().setFsInfo(info)
                .setServiceId(serviceId).build();
        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS).updateFileStore(request));
    }

    public void replaceFileStore(FileStoreInfo info, String serviceId) throws StarClientException {
        ReplaceFileStoreRequest request = ReplaceFileStoreRequest.newBuilder().setFsInfo(info)
                .setServiceId(serviceId).build();
        internalWriteRpcCall((x) -> x.withDeadlineAfter(clientWriteTimeoutSec, TimeUnit.SECONDS).replaceFileStore(request));
    }

    public List<FileStoreInfo> listFileStore(String serviceId) throws StarClientException {
        return listFileStore(serviceId, FS_NOT_SET);
    }

    public List<FileStoreInfo> listFileStore(String serviceId, FileStoreType fsType) throws StarClientException {
        ListFileStoreRequest request = ListFileStoreRequest.newBuilder().setServiceId(serviceId)
                .setFsType(fsType).build();
        ListFileStoreResponse response = internalReadRpcCall((x) -> x.withDeadlineAfter(clientListTimeoutSec, TimeUnit.SECONDS)
                                                                     .listFileStore(request));
        return response.getFsInfosList();
    }

    public FileStoreInfo getFileStore(String fsKey, String serviceId) throws StarClientException {
        return getFileStoreInternal("", fsKey, serviceId);
    }

    public FileStoreInfo getFileStoreByName(String fsName, String serviceId) throws StarClientException {
        return getFileStoreInternal(fsName, "", serviceId);
    }

    private FileStoreInfo getFileStoreInternal(String fsName, String fsKey, String serviceId)
            throws StarClientException {
        GetFileStoreRequest request = GetFileStoreRequest.newBuilder()
                .setServiceId(serviceId)
                .setFsName(fsName).setFsKey(fsKey).build();
        GetFileStoreResponse response = internalReadRpcCall((x) -> x.withDeadlineAfter(clientReadTimeoutSec, TimeUnit.SECONDS)
                                                                    .getFileStore(request));
        return response.getFsInfo();
    }

    public String dump() throws StarClientException {
        DumpRequest request = DumpRequest.newBuilder().build();
        DumpResponse response = internalReadRpcCall((x) -> x.dump(request));
        return response.getLocation();
    }

    private <V extends Message> V internalWriteRpcCall(RpcCallable<V> callable) throws StarClientException {
        return internalRpcCall(callable, true /* isWrite */, false /* useLeader */);
    }

    private <V extends Message> V internalReadRpcCall(RpcCallable<V> callable) throws StarClientException {
        return internalRpcCall(callable, false /* isWrite */, false /* useLeader */);
    }

    private <V extends Message> V internalReadRpcCallWithLeader(RpcCallable<V> callable, boolean useLeader)
            throws StarClientException {
        return internalRpcCall(callable, false /* isWrite */, useLeader);
    }

    private <V extends Message> V internalRpcCall(RpcCallable<V> callable, boolean isWrite, boolean useLeader)
            throws StarClientException {
        V result;
        try {
            if (useLeader && ((isWrite && leaderWriteStub != null) || (!isWrite && leaderReadStub != null))) {
                result = callable.call(isWrite ? leaderWriteStub : leaderReadStub);
            } else {
                result = callable.call(isWrite ? writeStub : readStub);
            }
        } catch (Exception exception) {
            throw new StarClientException(StatusCode.GRPC, exception.getMessage());
        }
        try {
            handleStatusError(result);
            return result;
        } catch (StarClientException exception) {
            // write option can not be sent to remote leader
            if (!useLeader && !isWrite && exception.getCode() == StatusCode.NOT_LEADER) { // handle NOT_LEADER error
                byte[] extraInfo = exception.getExtraInfo();
                if (extraInfo != null && extraInfo.length > 0) {
                    updateLeaderInfo(extraInfo);
                }
                if (leaderReadStub != null) {
                    // retry with the leader channel
                    return internalRpcCall(callable, false /* isWrite */, true /* useLeader */);
                }
            }
            // the exception should be thrown anyway unless retried.
            throw exception;
        }
    }

    private <V> V internalIpcCall(IpcCallable<V> callable) throws StarClientException {
        try {
            return callable.call();
        } catch (StarException exception) {
            StarClientException clientException = new StarClientException(exception.toStatus());
            if (clientException.getCode() == StatusCode.NOT_LEADER) {
                byte[] extraInfo = clientException.getExtraInfo();
                if (extraInfo != null && extraInfo.length > 0) {
                    updateLeaderInfo(extraInfo);
                }
            }
            throw clientException;
        }
    }

    private void updateLeaderInfo(byte[] info) {
        LeaderInfo leader;
        try {
            leader = LeaderInfo.parseFrom(info);
        } catch (InvalidProtocolBufferException exception) {
            // ignore invalid protobuf string
            return;
        }
        String newLeader = String.format("%s:%d", leader.getHost(), leader.getPort());
        try (LockCloseable ignored = new LockCloseable(leaderLock)) {
            if (!newLeader.equals(leaderAddress)) {
                String olderLeaderAddress = leaderAddress;
                leaderAddress = newLeader;
                resetStub(leaderAddress, false /* readOnly */, true /* isLeader */);
                LOG.info("Leader switched from {} to {}", olderLeaderAddress, leaderAddress);
            }
        }
    }

    private static StarStatus extractStatusFromProtobufMessage(Message msg) {
        // Assume all the responses protobuf message has a "StarStatus status" field.
        Descriptors.FieldDescriptor descriptor = msg.getDescriptorForType().findFieldByName("status");
        if (descriptor == null) { // can't find the "Status" field.
            return null;
        }
        Object status = msg.getField(descriptor);
        if (!(status instanceof StarStatus)) {
            return null;
        }
        return (StarStatus) status;
    }

    private void handleStatusError(Message msg) throws StarClientException {
        StarStatus status = extractStatusFromProtobufMessage(msg);
        if (status == null) {
            return;
        }
        if (status.getStatusCode() == StatusCode.OK) {
            return;
        }
        throw new StarClientException(status);
    }

    /**
     * Convenient help function to ease the client to generate the filepath with partitioned prefix feature enabled.
     * @param fromInfo the original FilePathInfo allocated by StarManager
     * @param seed the seed (hashcode) to calculate the prefix for the path
     * @return the full path from @fromInfo
     *  - return the @fromInfo.getFullPath() if the partitioned prefix is not enabled
     *  - return the full path with prefix inserted who is generated from @seed when the partitioned prefix is enabled.
     */
    public static String allocateFilePath(FilePathInfo fromInfo, int seed) {
        String fullPath = fromInfo.getFullPath();
        if (fromInfo.getFsInfo().getFsType() == FileStoreType.S3) {
            // support PartitionedPrefix feature for S3 FileStore
            S3FileStoreInfo s3info = fromInfo.getFsInfo().getS3FsInfo();
            if (s3info.getPartitionedPrefixEnabled()) {
                // update the full path with prefixes
                int modulus = s3info.getNumPartitionedPrefix();
                Preconditions.checkState(s3info.getPathPrefix().isEmpty());
                Preconditions.checkState(modulus > 0);

                String commonPrefix = String.format("s3://%s", s3info.getBucket());
                // FullPath: s3://<bucket>/<other_suffixes>
                // Update to: s3://<bucket>/<prefix>/<other_suffixes>
                Preconditions.checkState(fullPath.startsWith(commonPrefix));
                String relativeSuffix = fullPath.substring(commonPrefix.length());

                // convert integer to reverted hex string
                StringBuilder stringBuilder = new StringBuilder(Integer.toHexString(seed % modulus));
                stringBuilder.reverse();
                fullPath = commonPrefix + "/" + stringBuilder + relativeSuffix;
            }
        }
        return fullPath;
    }

    public void setClientReadTimeoutSec(int sec) {
        clientReadTimeoutSec = sec;
    }

    public void setClientListTimeoutSec(int sec) {
        clientListTimeoutSec = sec;
    }

    public void setClientWriteTimeoutSec(int sec) {
        clientWriteTimeoutSec = sec;
    }

    public void setClientReadMaxRetryCount(double cnt) {
        clientReadMaxRetryCount = cnt;
        if (defaultServerAddress != null) {
            resetStub(defaultServerAddress, true /* readOnly */, false /* isLeader */);
        }
        if (leaderAddress != null) {
            resetStub(leaderAddress, true /* readOnly */, true /* isLeader */);
        }
    }
}
