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


package com.staros.manager;

import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.StarException;
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
import com.staros.proto.DeleteMetaGroupRequest;
import com.staros.proto.DeleteMetaGroupResponse;
import com.staros.proto.DeleteShardGroupRequest;
import com.staros.proto.DeleteShardGroupResponse;
import com.staros.proto.DeleteShardRequest;
import com.staros.proto.DeleteShardResponse;
import com.staros.proto.DeleteWorkerGroupRequest;
import com.staros.proto.DeleteWorkerGroupResponse;
import com.staros.proto.DeregisterServiceRequest;
import com.staros.proto.DeregisterServiceResponse;
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
import com.staros.proto.RegisterServiceResponse;
import com.staros.proto.RemoveFileStoreRequest;
import com.staros.proto.RemoveFileStoreResponse;
import com.staros.proto.RemoveWorkerRequest;
import com.staros.proto.RemoveWorkerResponse;
import com.staros.proto.ReplaceFileStoreRequest;
import com.staros.proto.ReplaceFileStoreResponse;
import com.staros.proto.ReplicaUpdateInfo;
import com.staros.proto.ServiceComponentInfo;
import com.staros.proto.ServiceInfo;
import com.staros.proto.ServiceTemplateInfo;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.ShardInfoList;
import com.staros.proto.ShardReportInfo;
import com.staros.proto.ShutdownServiceRequest;
import com.staros.proto.ShutdownServiceResponse;
import com.staros.proto.StarManagerGrpc;
import com.staros.proto.StarStatus;
import com.staros.proto.StatusCode;
import com.staros.proto.UpdateFileStoreRequest;
import com.staros.proto.UpdateFileStoreResponse;
import com.staros.proto.UpdateMetaGroupInfo;
import com.staros.proto.UpdateMetaGroupRequest;
import com.staros.proto.UpdateMetaGroupResponse;
import com.staros.proto.UpdateShardGroupInfo;
import com.staros.proto.UpdateShardGroupRequest;
import com.staros.proto.UpdateShardGroupResponse;
import com.staros.proto.UpdateShardInfo;
import com.staros.proto.UpdateShardReplicaRequest;
import com.staros.proto.UpdateShardReplicaResponse;
import com.staros.proto.UpdateShardRequest;
import com.staros.proto.UpdateShardResponse;
import com.staros.proto.UpdateWorkerGroupRequest;
import com.staros.proto.UpdateWorkerGroupResponse;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerHeartbeatRequest;
import com.staros.proto.WorkerHeartbeatResponse;
import com.staros.proto.WorkerInfo;
import com.staros.util.Config;
import com.staros.util.Constant;
import com.staros.util.StatusFactory;
import com.staros.worker.WorkerGroup;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.staros.util.Constant.EMPTY_SERVICE_ID;

public class StarManagerService extends StarManagerGrpc.StarManagerImplBase {
    private static final Logger LOG = LogManager.getLogger(StarManagerService.class);

    private StarManager starManager;

    public StarManagerService(StarManager manager) {
        starManager = manager;
    }

    @Override
    public void registerService(RegisterServiceRequest request, StreamObserver<RegisterServiceResponse> responseObserver) {
        StarStatus status = StatusFactory.getStatus();
        RegisterServiceResponse.Builder response = RegisterServiceResponse.newBuilder();

        ServiceTemplateInfo template = request.getServiceTemplateInfo();
        String serviceTemplateName = template.getServiceTemplateName();
        // do not allow empty template name
        if (serviceTemplateName.isEmpty()) {
            status = StatusFactory.getStatus(StatusCode.INVALID_ARGUMENT, "empty service template name");
            response.setStatus(status);
        } else {
            List<String> serviceComponents = new ArrayList<>();
            for (ServiceComponentInfo info : template.getServiceComponentInfoList()) {
                serviceComponents.add(info.getServiceComponentName());
            }

            try {
                starManager.registerService(serviceTemplateName, serviceComponents);
            } catch (Throwable e) {
                response.setStatus(handleException(e));
            }
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deregisterService(DeregisterServiceRequest request, StreamObserver<DeregisterServiceResponse> responseObserver) {
        StarStatus status = StatusFactory.getStatus();
        DeregisterServiceResponse.Builder response = DeregisterServiceResponse.newBuilder();

        String serviceTemplateName = request.getServiceTemplateName();
        // do not allow empty template name
        if (serviceTemplateName.isEmpty()) {
            status = StatusFactory.getStatus(StatusCode.INVALID_ARGUMENT, "empty service template name");
            response.setStatus(status);
        } else {
            try {
                starManager.deregisterService(serviceTemplateName);
            } catch (Throwable e) {
                response.setStatus(handleException(e));
            }
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void bootstrapService(BootstrapServiceRequest request, StreamObserver<BootstrapServiceResponse> responseObserver) {
        StarStatus status = StatusFactory.getStatus();
        BootstrapServiceResponse.Builder response = BootstrapServiceResponse.newBuilder();

        String serviceTemplateName = request.getServiceTemplateName();
        String serviceName = request.getServiceName();
        // do not allow empty name
        if (serviceTemplateName.isEmpty() || serviceName.isEmpty()) {
            status =
                    StatusFactory.getStatus(StatusCode.INVALID_ARGUMENT, "empty service template name or service name");
            response.setStatus(status);
        } else {
            try {
                String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
                response.setServiceId(serviceId);
            } catch (Throwable e) {
                response.setStatus(handleException(e));
            }
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void shutdownService(ShutdownServiceRequest request, StreamObserver<ShutdownServiceResponse> responseObserver) {
        StarStatus status = StatusFactory.getStatus();
        ShutdownServiceResponse.Builder response = ShutdownServiceResponse.newBuilder();

        String serviceId = request.getServiceId();
        if (serviceId.equals(EMPTY_SERVICE_ID)) {
            status = StatusFactory.getStatus(StatusCode.INVALID_ARGUMENT, "invalid service id");
            response.setStatus(status);
        } else {
            try {
                starManager.shutdownService(serviceId);
            } catch (Throwable e) {
                response.setStatus(handleException(e));
            }
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getService(GetServiceRequest request, StreamObserver<GetServiceResponse> responseObserver) {
        StarStatus status = StatusFactory.getStatus();
        GetServiceResponse.Builder response = GetServiceResponse.newBuilder();

        GetServiceRequest.IdentifierCase icase = request.getIdentifierCase();
        switch (icase) {
            case SERVICE_ID: {
                String serviceId = request.getServiceId();
                if (serviceId.isEmpty()) {
                    status = StatusFactory.getStatus(StatusCode.INVALID_ARGUMENT, "invalid service id.");
                    response.setStatus(status);
                } else {
                    try {
                        ServiceInfo serviceInfo = starManager.getServiceInfoById(serviceId);
                        response.setServiceInfo(serviceInfo);
                    } catch (Throwable e) {
                        response.setStatus(handleException(e));
                    }
                }
            }
            break;
            case SERVICE_NAME: {
                String serviceName = request.getServiceName();
                if (serviceName.isEmpty()) {
                    status = StatusFactory.getStatus(StatusCode.INVALID_ARGUMENT, "invalid service name.");
                    response.setStatus(status);
                } else {
                    try {
                        ServiceInfo serviceInfo = starManager.getServiceInfoByName(serviceName);
                        response.setServiceInfo(serviceInfo);
                    } catch (Throwable e) {
                        response.setStatus(handleException(e));
                    }
                }
            }
            break;
            case IDENTIFIER_NOT_SET: {
                status = StatusFactory.getStatus(StatusCode.INVALID_ARGUMENT, "service id or service name not set.");
                response.setStatus(status);
            }
            break;
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void createWorkerGroup(CreateWorkerGroupRequest request,
                                  StreamObserver<CreateWorkerGroupResponse> responseObserver) {
        CreateWorkerGroupResponse.Builder response = CreateWorkerGroupResponse.newBuilder();
        try {
            long groupId = starManager.createWorkerGroup(request.getServiceId(), request.getOwner(),
                    request.getSpec(), request.getLabelsMap(), request.getPropertiesMap(), request.getReplicaNumber(),
                    request.getReplicationType(), request.getWarmupLevel());
            WorkerGroup group = starManager.getWorkerManager().getWorkerGroup(request.getServiceId(), groupId);
            response.setStatus(StatusFactory.getStatus())
                    .setGroupInfo(group.toProtobuf());
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void listWorkerGroup(ListWorkerGroupRequest request,
                                StreamObserver<ListWorkerGroupResponse> responseObserver) {
        ListWorkerGroupResponse.Builder response = ListWorkerGroupResponse.newBuilder();
        try {
            List<WorkerGroupDetailInfo> groups = starManager.listWorkerGroups(request.getServiceId(), request.getGroupIdsList(),
                    request.getFilterLabelsMap(), request.getIncludeWorkersInfo());
            response.setStatus(StatusFactory.getStatus())
                    .addAllGroupsInfo(groups);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateWorkerGroup(UpdateWorkerGroupRequest request,
                                  StreamObserver<UpdateWorkerGroupResponse> responseObserver) {
        UpdateWorkerGroupResponse.Builder respBuilder = UpdateWorkerGroupResponse.newBuilder();
        try {
            WorkerGroupSpec spec = null;
            if (request.hasSpec()) {
                spec = request.getSpec();
            }
            long groupId = request.getGroupId();
            starManager.updateWorkerGroup(request.getServiceId(), request.getGroupId(), spec, request.getLabelsMap(),
                    request.getPropertiesMap(), request.getReplicaNumber(), request.getReplicationType(),
                    request.getWarmupLevel());
            WorkerGroup group = starManager.getWorkerManager().getWorkerGroup(request.getServiceId(), groupId);
            respBuilder.setStatus(StatusFactory.getStatus())
                    .setGroupInfo(group.toProtobuf());
        } catch (Throwable e) {
            respBuilder.setStatus(handleException(e));
        }
        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteWorkerGroup(DeleteWorkerGroupRequest request,
                                  StreamObserver<DeleteWorkerGroupResponse> responseObserver) {
        StarStatus status = StatusFactory.getStatus();
        try {
            starManager.deleteWorkerGroup(request.getServiceId(), request.getGroupId());
        } catch (Throwable e) {
            status = handleException(e);
        }
        responseObserver.onNext(DeleteWorkerGroupResponse.newBuilder()
                .setStatus(status)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void addWorker(AddWorkerRequest request, StreamObserver<AddWorkerResponse> responseObserver) {
        AddWorkerResponse.Builder response = AddWorkerResponse.newBuilder();
        String serviceId = request.getServiceId();
        long groupId = request.getGroupId();
        String workerAddress = request.getIpPort();

        StarStatus status = validateWorkerAddress(workerAddress);
        if (status.getStatusCode() == StatusCode.OK) {
            try {
                checkWorkerGroupId(groupId);
                long workerId = starManager.addWorker(serviceId, groupId, workerAddress);
                response.setWorkerId(workerId);
            } catch (Throwable e) {
                response.setStatus(handleException(e));
            }
        } else {
            LOG.warn("Invalid worker address: {}", workerAddress);
            response.setStatus(status);
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void removeWorker(RemoveWorkerRequest request, StreamObserver<RemoveWorkerResponse> responseObserver) {
        RemoveWorkerResponse.Builder response = RemoveWorkerResponse.newBuilder();
        String serviceId = request.getServiceId();
        long groupId = request.getGroupId();
        long workerId = request.getWorkerId();

        try {
            checkWorkerGroupId(groupId);
            starManager.removeWorker(serviceId, groupId, workerId);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getWorker(GetWorkerRequest request, StreamObserver<GetWorkerResponse> responseObserver) {
        StarStatus status = StatusFactory.getStatus();
        GetWorkerResponse.Builder response = GetWorkerResponse.newBuilder();
        String serviceId = request.getServiceId();

        GetWorkerRequest.IdentifierCase icase = request.getIdentifierCase();
        switch (icase) {
            case WORKER_ID: {
                long workerId = request.getWorkerId();
                if (workerId == 0L) {
                    status = StatusFactory.getStatus(StatusCode.INVALID_ARGUMENT, "invalid worker id.");
                    response.setStatus(status);
                } else {
                    try {
                        WorkerInfo workerInfo = starManager.getWorkerInfo(serviceId, workerId);
                        response.setWorkerInfo(workerInfo);
                    } catch (Throwable e) {
                        response.setStatus(handleException(e));
                    }
                }
            }
            break;
            case IP_PORT: {
                String ipPort = request.getIpPort();
                if (ipPort.isEmpty()) {
                    status = StatusFactory.getStatus(StatusCode.INVALID_ARGUMENT, "invalid worker ip port.");
                    response.setStatus(status);
                } else {
                    try {
                        WorkerInfo workerInfo = starManager.getWorkerInfo(serviceId, ipPort);
                        response.setWorkerInfo(workerInfo);
                    } catch (Throwable e) {
                        response.setStatus(handleException(e));
                    }
                }
            }
            break;
            case IDENTIFIER_NOT_SET: {
                status = StatusFactory.getStatus(StatusCode.INVALID_ARGUMENT, "worker id or worker ip port not set.");
                response.setStatus(status);
            }
            break;
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void createShard(CreateShardRequest request, StreamObserver<CreateShardResponse> responseObserver) {
        LOG.debug("receive create shard request, {}", request);
        CreateShardResponse.Builder response = CreateShardResponse.newBuilder();
        String serviceId = request.getServiceId();
        List<CreateShardInfo> createShardInfos = request.getCreateShardInfosList();

        try {
            List<ShardInfo> shardInfos = starManager.createShard(serviceId, createShardInfos);
            response.addAllShardInfo(shardInfos);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteShard(DeleteShardRequest request, StreamObserver<DeleteShardResponse> responseObserver) {
        LOG.debug("receive delete shard request, {}", request);
        DeleteShardResponse.Builder response = DeleteShardResponse.newBuilder();
        String serviceId = request.getServiceId();
        List<Long> shardIds = request.getShardIdList();

        try {
            starManager.deleteShard(serviceId, shardIds);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateShard(UpdateShardRequest request, StreamObserver<UpdateShardResponse> responseObserver) {
        UpdateShardResponse.Builder response = UpdateShardResponse.newBuilder();
        String serviceId = request.getServiceId();
        List<UpdateShardInfo> updateShardInfos = request.getUpdateShardInfosList();

        try {
            starManager.updateShard(serviceId, updateShardInfos);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getShard(GetShardRequest request, StreamObserver<GetShardResponse> responseObserver) {
        LOG.debug("receive get shard request, {}", request);
        GetShardResponse.Builder response = GetShardResponse.newBuilder();
        String serviceId = request.getServiceId();
        List<Long> shardIds = request.getShardIdList();

        try {
            List<ShardInfo> shardInfos = starManager.getShardInfo(serviceId, shardIds, request.getWorkerGroupId());
            response.addAllShardInfo(shardInfos);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void listShard(ListShardRequest request, StreamObserver<ListShardResponse> responseObserver) {
        LOG.debug("receive list shard request, {}", request);
        ListShardResponse.Builder response = ListShardResponse.newBuilder();
        String serviceId = request.getServiceId();
        List<Long> groupIds = request.getGroupIdsList();

        try {
            List<ShardInfoList> shardInfoLists = starManager.listShardInfo(serviceId, groupIds, request.getWorkerGroupId(),
                    request.getWithoutReplicaInfo());
            response.addAllShardInfoLists(shardInfoLists);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void createShardGroup(CreateShardGroupRequest request, StreamObserver<CreateShardGroupResponse> responseObserver) {
        LOG.debug("receive create shard group request, {}", request);
        CreateShardGroupResponse.Builder response = CreateShardGroupResponse.newBuilder();
        String serviceId = request.getServiceId();
        List<CreateShardGroupInfo> createShardGroupInfos = request.getCreateShardGroupInfosList();

        try {
            List<ShardGroupInfo> shardGroupInfos = starManager.createShardGroup(serviceId, createShardGroupInfos);
            response.addAllShardGroupInfos(shardGroupInfos);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteShardGroup(DeleteShardGroupRequest request, StreamObserver<DeleteShardGroupResponse> responseObserver) {
        LOG.debug("receive delete shard group request, {}", request);
        DeleteShardGroupResponse.Builder response = DeleteShardGroupResponse.newBuilder();

        try {
            starManager.deleteShardGroup(request.getServiceId(), request.getDeleteInfo().getGroupIdsList(),
                    request.getDeleteInfo().getCascadeDeleteShard());
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateShardGroup(UpdateShardGroupRequest request, StreamObserver<UpdateShardGroupResponse> responseObserver) {
        UpdateShardGroupResponse.Builder response = UpdateShardGroupResponse.newBuilder();
        String serviceId = request.getServiceId();
        List<UpdateShardGroupInfo> updateShardGroupInfos = request.getUpdateShardGroupInfosList();

        try {
            starManager.updateShardGroup(serviceId, updateShardGroupInfos);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void listShardGroup(ListShardGroupRequest request, StreamObserver<ListShardGroupResponse> responseObserver) {
        LOG.debug("receive list shard group request, {}", request);
        ListShardGroupResponse.Builder response = ListShardGroupResponse.newBuilder();
        String serviceId = request.getServiceId();
        boolean includeAnonymousGroup = request.getIncludeAnonymousGroup();
        long startGroupId = request.getStartGroupId();

        try {
            Pair<List<ShardGroupInfo>, Long> pair = starManager.listShardGroupInfo(serviceId, includeAnonymousGroup,
                    startGroupId);
            response.addAllShardGroupInfos(pair.getKey());
            response.setNextGroupId(pair.getValue());
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getShardGroup(GetShardGroupRequest request, StreamObserver<GetShardGroupResponse> responseObserver) {
        LOG.debug("receive get shard group request, {}", request);
        GetShardGroupResponse.Builder response = GetShardGroupResponse.newBuilder();
        String serviceId = request.getServiceId();
        List<Long> shardGroupIds = request.getShardGroupIdList();

        try {
            List<ShardGroupInfo> shardGroupInfos = starManager.getShardGroupInfo(serviceId, shardGroupIds);
            response.addAllShardGroupInfo(shardGroupInfos);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void createMetaGroup(CreateMetaGroupRequest request, StreamObserver<CreateMetaGroupResponse> responseObserver) {
        LOG.debug("receive create meta group request, {}", request);
        CreateMetaGroupResponse.Builder response = CreateMetaGroupResponse.newBuilder();
        String serviceId = request.getServiceId();
        CreateMetaGroupInfo createMetaGroupInfo = request.getCreateMetaGroupInfo();

        try {
            response.setMetaGroupInfo(starManager.createMetaGroup(serviceId, createMetaGroupInfo));
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteMetaGroup(DeleteMetaGroupRequest request, StreamObserver<DeleteMetaGroupResponse> responseObserver) {
        LOG.debug("receive delete meta group request, {}", request);
        DeleteMetaGroupResponse.Builder response = DeleteMetaGroupResponse.newBuilder();
        String serviceId = request.getServiceId();
        long metaGroupId = request.getDeleteMetaGroupInfo().getMetaGroupId();

        try {
            starManager.deleteMetaGroup(serviceId, metaGroupId);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateMetaGroup(UpdateMetaGroupRequest request, StreamObserver<UpdateMetaGroupResponse> responseObserver) {
        LOG.debug("receive update meta group request, {}", request);
        UpdateMetaGroupResponse.Builder response = UpdateMetaGroupResponse.newBuilder();
        String serviceId = request.getServiceId();
        UpdateMetaGroupInfo updateMetaGroupInfo = request.getUpdateMetaGroupInfo();

        try {
            starManager.updateMetaGroup(serviceId, updateMetaGroupInfo);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getMetaGroup(GetMetaGroupRequest request, StreamObserver<GetMetaGroupResponse> responseObserver) {
        LOG.debug("receive get meta group request, {}", request);
        GetMetaGroupResponse.Builder response = GetMetaGroupResponse.newBuilder();
        String serviceId = request.getServiceId();
        long metaGroupId = request.getMetaGroupId();

        try {
            response.setMetaGroupInfo(starManager.getMetaGroupInfo(serviceId, metaGroupId));
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void listMetaGroup(ListMetaGroupRequest request, StreamObserver<ListMetaGroupResponse> responseObserver) {
        LOG.debug("receive list meta group request, {}", request);
        ListMetaGroupResponse.Builder response = ListMetaGroupResponse.newBuilder();
        String serviceId = request.getServiceId();

        try {
            List<MetaGroupInfo> metaGroupInfos = starManager.listMetaGroupInfo(serviceId);
            response.addAllMetaGroupInfos(metaGroupInfos);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryMetaGroupStable(QueryMetaGroupStableRequest request,
                                     StreamObserver<QueryMetaGroupStableResponse> responseObserver) {
        QueryMetaGroupStableResponse.Builder response = QueryMetaGroupStableResponse.newBuilder();
        try {
            boolean isStable = starManager.queryMetaGroupStable(
                    request.getServiceId(), request.getMetaGroupId(), request.getWorkerGroupId());
            response.setIsStable(isStable);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void workerHeartbeat(WorkerHeartbeatRequest request, StreamObserver<WorkerHeartbeatResponse> responseObserver) {
        LOG.debug("receive worker heartbeat request, {}", request);
        WorkerHeartbeatResponse.Builder response = WorkerHeartbeatResponse.newBuilder();

        String serviceId = request.getServiceId();
        long workerId = request.getWorkerId();
        long startTime = request.getStartTime();
        Map<String, String> workerProperties = request.getWorkerPropertiesMap();
        List<Long> shardIds = request.getShardIdsList();
        List<ShardReportInfo> shardReportInfos = request.getShardReportInfosList();

        try {
            starManager.processWorkerHeartbeat(serviceId, workerId, startTime,
                    workerProperties, shardIds, shardReportInfos);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void allocateFilePath(AllocateFilePathRequest request, StreamObserver<AllocateFilePathResponse> responseObserver) {
        LOG.debug("receive allocate file path request {}", request);
        AllocateFilePathResponse.Builder response = AllocateFilePathResponse.newBuilder();

        String serviceId = request.getServiceId();
        FileStoreType fsType = request.getFsType();
        String suffix = request.getSuffix();
        String fsKey = request.getFsKey();
        String rootDir = request.getCustomRootDir();
        try {
            FilePathInfo pathInfo = starManager.allocateFilePath(serviceId, fsType, suffix, fsKey, rootDir);
            response.setPathInfo(pathInfo);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void addFileStore(AddFileStoreRequest request, StreamObserver<AddFileStoreResponse> responseObserver) {
        LOG.debug("receive add file store request, {}", request);
        AddFileStoreResponse.Builder response = AddFileStoreResponse.newBuilder();

        FileStoreInfo fsInfo = request.getFsInfo();
        String serviceId = request.getServiceId();
        try {
            String fsKey = starManager.addFileStore(serviceId, fsInfo);
            response.setFsKey(fsKey);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void removeFileStore(RemoveFileStoreRequest request, StreamObserver<RemoveFileStoreResponse> responseObserver) {
        LOG.debug("receive remove file store request {}", request);
        RemoveFileStoreResponse.Builder response = RemoveFileStoreResponse.newBuilder();

        String fsName = request.getFsName();
        String fsKey = request.getFsKey();
        String serviceId = request.getServiceId();
        try {
            starManager.removeFileStore(serviceId, fsKey, fsName);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void listFileStore(ListFileStoreRequest request, StreamObserver<ListFileStoreResponse> responseObserver) {
        LOG.debug("receive list file store request {}", request);
        ListFileStoreResponse.Builder response = ListFileStoreResponse.newBuilder();
        try {
            List<FileStoreInfo> fsInfos = starManager.listFileStore(request.getServiceId(), request.getFsType());
            response.addAllFsInfos(fsInfos);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateFileStore(UpdateFileStoreRequest request, StreamObserver<UpdateFileStoreResponse> responseObserver) {
        LOG.debug("receive update storage request, {}", request);
        UpdateFileStoreResponse.Builder response = UpdateFileStoreResponse.newBuilder();
        FileStoreInfo fsInfo = request.getFsInfo();
        String serviceId = request.getServiceId();
        try {
            starManager.updateFileStore(serviceId, fsInfo);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void replaceFileStore(ReplaceFileStoreRequest request, StreamObserver<ReplaceFileStoreResponse> responseObserver) {
        LOG.debug("receive replace storage request, {}", request);
        ReplaceFileStoreResponse.Builder response = ReplaceFileStoreResponse.newBuilder();
        FileStoreInfo fsInfo = request.getFsInfo();
        String serviceId = request.getServiceId();
        try {
            starManager.replaceFileStore(serviceId, fsInfo);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getFileStore(GetFileStoreRequest request, StreamObserver<GetFileStoreResponse> responseObserver) {
        GetFileStoreResponse.Builder response = GetFileStoreResponse.newBuilder();
        String serviceId = request.getServiceId();
        String fsName = request.getFsName();
        String fsKey = request.getFsKey();
        try {
            FileStoreInfo fsInfo = starManager.getFileStore(serviceId, fsName, fsKey);
            response.setFsInfo(fsInfo);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateShardReplicaInfo(UpdateShardReplicaRequest request,
                                       StreamObserver<UpdateShardReplicaResponse> responseObserver) {
        UpdateShardReplicaResponse.Builder response = UpdateShardReplicaResponse.newBuilder();
        try {
            String serviceId = request.getServiceId();
            long workerId = request.getWorkerId();
            List<ReplicaUpdateInfo> updateReplicaInfo = request.getReplicaUpdateInfosList();
            starManager.updateShardReplicaInfo(serviceId, workerId, updateReplicaInfo);
            response.setStatus(StatusFactory.getStatus());
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void dump(DumpRequest request, StreamObserver<DumpResponse> responseObserver) {
        StarStatus status = StatusFactory.getStatus();
        DumpResponse.Builder response = DumpResponse.newBuilder();

        try {
            String location = starManager.dump();
            response.setLocation(location);
        } catch (Throwable e) {
            response.setStatus(handleException(e));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    // valid worker host, either ip:port or domainName:port, example: "127.0.0.1:1234" or "a.b.com:5678"
    private StarStatus validateWorkerAddress(String workerAddress) {
        StarStatus status = StatusFactory.getStatus(StatusCode.INVALID_ARGUMENT, "invalid host port");
        String[] array = workerAddress.split(":");
        if (array.length != 2 || array[0].isEmpty() || array[1].isEmpty()) {
            return status;
        }
        // check port
        int port = Integer.parseInt(array[1]);
        InetSocketAddress address = new InetSocketAddress(array[0], port);
        if (port <= 0 || address.isUnresolved()) {
            return status;
        }
        return StatusFactory.getStatus();
    }

    private void checkWorkerGroupId(long groupId) {
        if (groupId == Constant.DEFAULT_PROTOBUF_INTEGER && !Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY) {
            throw new InvalidArgumentStarException("Worker group id not set!");
        }
    }

    private StarStatus handleException(Throwable throwable) {
        if (throwable instanceof StarException) {
            return ((StarException) throwable).toStatus();
        } else {
            return StatusFactory.getStatus(StatusCode.INTERNAL, throwable.getMessage());
        }
    }
}
