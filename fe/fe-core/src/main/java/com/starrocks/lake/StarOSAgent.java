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


package com.starrocks.lake;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.staros.client.StarClient;
import com.staros.client.StarClientException;
import com.staros.manager.StarManagerServer;
import com.staros.proto.CreateMetaGroupInfo;
import com.staros.proto.CreateShardGroupInfo;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.JoinMetaGroupInfo;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.PlacementPreference;
import com.staros.proto.PlacementRelationship;
import com.staros.proto.QuitMetaGroupInfo;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaRole;
import com.staros.proto.ServiceInfo;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.StatusCode;
import com.staros.proto.UpdateMetaGroupInfo;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerInfo;
import com.staros.proto.WorkerState;
import com.staros.util.LockCloseable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.warehouse.Cluster;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * StarOSAgent is responsible for
 * 1. Encapsulation of StarClient api.
 * 2. Maintenance of StarOS worker to StarRocks backend map.
 */
public class StarOSAgent {
    private static final Logger LOG = LogManager.getLogger(StarOSAgent.class);

    public static final String SERVICE_NAME = "starrocks";

    private StarClient client;
    private String serviceId;
    private ReentrantReadWriteLock rwLock;

    public StarOSAgent() {
        serviceId = "";
        rwLock = new ReentrantReadWriteLock();
    }

    public boolean init(StarManagerServer server) {
        if (Config.integrate_starmgr) {
            if (!Config.use_staros) {
                LOG.error("integrate_starmgr is true but use_staros is false!");
                return false;
            }
            // check if Config.starmanager_address == FE address
            String[] starMgrAddr = Config.starmgr_address.split(":");
            if (!starMgrAddr[0].equals("127.0.0.1")) {
                LOG.error("Config.starmgr_address not equal 127.0.0.1, it is {}", starMgrAddr[0]);
                return false;
            }
        }

        client = new StarClient(server);
        client.connectServer(Config.starmgr_address);
        return true;
    }

    private void prepare() {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (serviceId.equals("")) {
                getServiceId();
            }
        }
    }

    public void getServiceId() {
        try {
            ServiceInfo serviceInfo = client.getServiceInfoByName(SERVICE_NAME);
            serviceId = serviceInfo.getServiceId();
        } catch (StarClientException e) {
            LOG.warn("Failed to get serviceId from starMgr. Error: {}", e);
            return;
        }
        LOG.info("get serviceId {} from starMgr", serviceId);
    }

    public FilePathInfo allocateFilePath(long tableId) throws DdlException {
        try {
            EnumDescriptor enumDescriptor = FileStoreType.getDescriptor();
            FileStoreType fsType = FileStoreType.valueOf(enumDescriptor.findValueByName(Config.default_fs_type).getNumber());
            FilePathInfo pathInfo = client.allocateFilePath(serviceId, fsType, Long.toString(tableId));
            LOG.debug("Allocate file path from starmgr: {}", pathInfo);
            return pathInfo;
        } catch (StarClientException e) {
            throw new DdlException("Failed to allocate file path from StarMgr", e);
        }
    }

    public boolean registerAndBootstrapService() {
        try {
            client.registerService("starrocks");
        } catch (StarClientException e) {
            if (e.getCode() != StatusCode.ALREADY_EXIST) {
                LOG.error("Failed to register service from starMgr. Error: {}", e);
                return false;
            }
        }

        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            try {
                serviceId = client.bootstrapService("starrocks", SERVICE_NAME);
                LOG.info("get serviceId: {} by bootstrapService to starMgr", serviceId);
            } catch (StarClientException e) {
                if (e.getCode() != StatusCode.ALREADY_EXIST) {
                    LOG.error("Failed to bootstrap service from starMgr. Error: {}", e);
                    return false;
                } else {
                    getServiceId();
                }
            }
        }
        return true;
    }

    public long createShardGroup(long dbId, long tableId, long partitionId) throws DdlException {
        prepare();
        List<ShardGroupInfo> shardGroupInfos = null;
        try {
            List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
            createShardGroupInfos.add(CreateShardGroupInfo.newBuilder()
                    .setPolicy(PlacementPolicy.SPREAD)
                    .putLabels("dbId", String.valueOf(dbId))
                    .putLabels("tableId", String.valueOf(tableId))
                    .putLabels("partitionId", String.valueOf(partitionId))
                    .putProperties("createTime", String.valueOf(System.currentTimeMillis()))
                    .build());
            shardGroupInfos = client.createShardGroup(serviceId, createShardGroupInfos);
            LOG.debug("Create shard group success. shard group infos: {}", shardGroupInfos);
            Preconditions.checkState(shardGroupInfos.size() == 1);
        } catch (StarClientException e) {
            throw new DdlException("Failed to create shard group. error: " + e.getMessage());
        }
        return shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList()).get(0);
    }

    public void deleteShardGroup(List<Long> groupIds) {
        prepare();
        try {
            client.deleteShardGroup(serviceId, groupIds, true);
        } catch (StarClientException e) {
            LOG.warn("Failed to delete shard group. error: {}", e.getMessage());
        }
    }

    public List<ShardGroupInfo> listShardGroup() {
        prepare();
        try {
            return client.listShardGroup(serviceId);
        } catch (StarClientException e) {
            LOG.info("list shard group failed. Error: {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    public List<Long> createShards(int numShards, int replicaNum, FilePathInfo pathInfo, FileCacheInfo cacheInfo, long groupId)
        throws DdlException {
        return createShards(numShards, replicaNum, pathInfo, cacheInfo, groupId, null);
    }

    public List<Long> createShards(int numShards, int replicaNum, FilePathInfo pathInfo, FileCacheInfo cacheInfo, long groupId,
            List<Long> matchShardIds)
        throws DdlException {
        if (matchShardIds != null) {
            Preconditions.checkState(numShards == matchShardIds.size());
        }
        prepare();
        List<ShardInfo> shardInfos = null;
        try {
            List<CreateShardInfo> createShardInfoList = new ArrayList<>(numShards);

            CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
            builder.setReplicaCount(replicaNum)
                    .addGroupIds(groupId)
                    .setPathInfo(pathInfo)
                    .setCacheInfo(cacheInfo);

            for (int i = 0; i < numShards; ++i) {
                builder.setShardId(GlobalStateMgr.getCurrentState().getNextId());
                if (matchShardIds != null) {
                    builder.clearPlacementPreferences();
                    PlacementPreference preference = PlacementPreference.newBuilder()
                            .setPlacementPolicy(PlacementPolicy.PACK)
                            .setPlacementRelationship(PlacementRelationship.WITH_SHARD)
                            .setRelationshipTargetId(matchShardIds.get(i))
                            .build();
                    builder.addPlacementPreferences(preference);
                }
                createShardInfoList.add(builder.build());
            }
            shardInfos = client.createShard(serviceId, createShardInfoList);
            LOG.debug("Create shards success. shard infos: {}", shardInfos);
        } catch (Exception e) {
            throw new DdlException("Failed to create shards. error: " + e.getMessage());
        }

        Preconditions.checkState(shardInfos.size() == numShards);
        return shardInfos.stream().map(ShardInfo::getShardId).collect(Collectors.toList());
    }

    public List<Long> listShard(long groupId) throws DdlException {
        prepare();

        List<List<ShardInfo>> shardInfo;
        try {
            shardInfo = client.listShard(serviceId, Arrays.asList(groupId));
        } catch (StarClientException e) {
            throw new DdlException("Failed to list shards. error: " + e.getMessage());
        }
        return shardInfo.get(0).stream().map(ShardInfo::getShardId).collect(Collectors.toList());
    }

    public void deleteShards(Set<Long> shardIds) throws DdlException {
        prepare();
        try {
            client.deleteShard(serviceId, shardIds);
        } catch (StarClientException e) {
            LOG.warn("Failed to delete shards. error: {}", e.getMessage());
            throw new DdlException("Failed to delete shards. error: " + e.getMessage());
        }
    }

    // for debug
    private List<ReplicaInfo> getShardReplicas(long shardId, long workerGroupId) throws UserException {
        prepare();
        try {
            List<ShardInfo> shardInfos = client.getShardInfo(serviceId, Lists.newArrayList(shardId), workerGroupId);
            Preconditions.checkState(shardInfos.size() == 1);
            return shardInfos.get(0).getReplicaInfoList();
        } catch (StarClientException e) {
            throw new UserException("Failed to get shard info. error: " + e.getMessage());
        }
    }

    private List<ReplicaInfo> getShardReplicas(long shardId) throws UserException {
        prepare();
        try {
            String currentWarehouse = ConnectContext.get().getCurrentWarehouse();
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(currentWarehouse);

            Cluster cluster = warehouse.getClusters().values().stream().findFirst().orElseThrow(
                    () -> new UserException("no cluster exists in this warehouse")
            );

            long workerGroupId = cluster.getWorkerGroupId();
            List<ShardInfo> shardInfos = client.getShardInfo(serviceId, Lists.newArrayList(shardId), workerGroupId);
            Preconditions.checkState(shardInfos.size() == 1);
            return shardInfos.get(0).getReplicaInfoList();
        } catch (StarClientException e) {
            throw new UserException("Failed to get shard info. error: " + e.getMessage());
        }
    }

    // for debug
    public long getPrimaryBackendIdByShard(long shardId, long workerGroupId) throws UserException {
        // for debug
        LOG.info("enter getPrimaryBackendIdByShard, workerGroupId is {}", workerGroupId);
        List<ReplicaInfo> replicas = getShardReplicas(shardId, workerGroupId);
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            for (ReplicaInfo replicaInfo : replicas) {
                if (replicaInfo.getReplicaRole() == ReplicaRole.PRIMARY) {
                    WorkerInfo workerInfo = replicaInfo.getWorkerInfo();
                    long workerId = workerInfo.getWorkerId();
                    return workerId;
                }
            }
        }
        throw new UserException("Failed to get primary backend. shard id: " + shardId);
    }

    public long getPrimaryBackendIdByShard(long shardId) throws UserException {
        List<ReplicaInfo> replicas = getShardReplicas(shardId);

        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            for (ReplicaInfo replicaInfo : replicas) {
                if (replicaInfo.getReplicaRole() == ReplicaRole.PRIMARY) {
                    WorkerInfo workerInfo = replicaInfo.getWorkerInfo();
                    long workerId = workerInfo.getWorkerId();

                    // add backend to SystemInfoService
                    Backend backend = workerToBackend(workerInfo);
                    GlobalStateMgr.getCurrentSystemInfo().addBackend(backend);
                    return workerId;
                }
            }
        }
        throw new UserException("Failed to get primary backend. shard id: " + shardId);
    }

    public Set<Long> getBackendIdsByShard(long shardId) throws UserException {
        List<ReplicaInfo> replicas = getShardReplicas(shardId);
        Set<Long> workerIds = Sets.newHashSet();
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            for (ReplicaInfo replicaInfo : replicas) {
                // TODO: check worker state
                WorkerInfo workerInfo = replicaInfo.getWorkerInfo();
                long workerId = workerInfo.getWorkerId();
                workerIds.add(workerId);
            }
        }
        return workerIds;
    }

    public void createMetaGroup(long metaGroupId, List<Long> shardGroupIds) throws DdlException {
        prepare();

        try {
            CreateMetaGroupInfo createInfo = CreateMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupId)
                    .setPlacementPolicy(PlacementPolicy.PACK)
                    .addAllShardGroupIds(shardGroupIds)
                    .build();
            client.createMetaGroup(serviceId, createInfo);
        } catch (StarClientException e) {
            throw new DdlException("Failed to create meta group. error: " + e.getMessage());
        }
    }

    public void updateMetaGroup(long metaGroupId, List<Long> shardGroupIds, boolean isJoin) throws DdlException {
        prepare();

        try {
            UpdateMetaGroupInfo.Builder builder = UpdateMetaGroupInfo.newBuilder();

            if (isJoin) {
                JoinMetaGroupInfo joinInfo = JoinMetaGroupInfo.newBuilder()
                        .setMetaGroupId(metaGroupId)
                        .build();
                builder.setJoinInfo(joinInfo);
            } else {
                QuitMetaGroupInfo quitInfo = QuitMetaGroupInfo.newBuilder()
                        .setMetaGroupId(metaGroupId)
                        .build();
                builder.setQuitInfo(quitInfo);
            }

            builder.addAllShardGroupIds(shardGroupIds);

            client.updateMetaGroup(serviceId, builder.build());
        } catch (StarClientException e) {
            throw new DdlException("Failed to update meta group. error: " + e.getMessage());
        }
    }

    public long createWorkerGroup(String size) throws DdlException {
        // size should be S, M, L, XL...
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize(size).build();
        // owner means tenant, now there is only one tenant, so pass "Starrocks" to starMgr
        String owner = "Starrocks";
        WorkerGroupDetailInfo result = null;
        try {
            result = client.createWorkerGroup(serviceId, owner, spec, Collections.emptyMap(),
                    Collections.emptyMap());
        } catch (StarClientException e) {
            LOG.warn("Failed to create worker group. error: {}", e.getMessage());
            throw new DdlException("Failed to create worker group. error: " + e.getMessage());
        }
        return result.getGroupId();
    }

    public void deleteWorkerGroup(long groupId) throws DdlException {
        try {
            client.deleteWorkerGroup(serviceId, groupId);
        } catch (StarClientException e) {
            LOG.warn("Failed to delete worker group {}. error: {}", groupId, e.getMessage());
            throw new DdlException("Failed to delete worker group. error: " + e.getMessage());
        }
    }

    public void modifyWorkerGroup(long groupId, String size) throws DdlException {
        WorkerGroupDetailInfo updatedInfo = null;
        WorkerGroupSpec newSpec = WorkerGroupSpec.newBuilder().setSize(size).build();
        try {
            updatedInfo = client.alterWorkerGroupSpec(serviceId, groupId, newSpec);
        } catch (StarClientException e) {
            LOG.warn("Failed to update worker group size. error: {}", e.getMessage());
            throw new DdlException("Failed to update worker group size. error: " + e.getMessage());
        }
    }

    private Backend workerToBackend(WorkerInfo workerInfo) {
        String workerAddr = workerInfo.getIpPort();
        String[] pair = workerAddr.split(":");
        int heartbeatPort = Integer.parseInt(workerInfo.getWorkerPropertiesMap().get("be_heartbeat_port"));
        int bePort = Integer.parseInt(workerInfo.getWorkerPropertiesMap().get("be_port"));
        int beHttpPort = Integer.parseInt(workerInfo.getWorkerPropertiesMap().get("be_http_port"));
        int beBrpcPort = Integer.parseInt(workerInfo.getWorkerPropertiesMap().get("be_brpc_port"));

        Backend backend = new Backend(workerInfo.getWorkerId(), pair[0], heartbeatPort);
        backend.setIsAlive(true);
        backend.setBePort(bePort);
        backend.setHttpPort(beHttpPort);
        backend.setBrpcPort(beBrpcPort);
        return backend;
    }

    public Backend getWorkerById(long workerId) throws UserException {
        prepare();
        try {
            WorkerInfo workerInfo = client.getWorkerInfo(serviceId, workerId);
            return workerToBackend(workerInfo);
        } catch (StarClientException e) {
            throw new UserException("Failed to get worker by id. error: " + e.getMessage());
        }
    }

    public boolean checkWorkerHealthy(long workerId) throws UserException {
        prepare();
        try {
            WorkerInfo workerInfo = client.getWorkerInfo(serviceId, workerId);
            WorkerState state = workerInfo.getWorkerState();
            return state != WorkerState.DOWN;
        } catch (StarClientException e) {
            throw new UserException("Failed to get worker by id. error: " + e.getMessage());
        }
    }

    public List<Backend> getWorkersByWorkerGroup(List<Long> workerGroupIds) throws UserException {
        prepare();
        try {
            List<WorkerGroupDetailInfo> workerGroupDetailInfos = client.listWorkerGroup(serviceId, workerGroupIds, true);
            List<Backend> backends = Lists.newArrayList();
            for (WorkerGroupDetailInfo detailInfo : workerGroupDetailInfos) {
                List<WorkerInfo> workerInfos = detailInfo.getWorkersInfoList();
                for (WorkerInfo workerInfo : workerInfos) {
                    backends.add(workerToBackend(workerInfo));
                }
            }
            return backends;
        } catch (StarClientException e) {
            throw new UserException("Failed to get workers by group id. error: " + e.getMessage());
        }
    }

    public List<Backend> getWorkers() throws UserException {
        prepare();
        try {
            List<WorkerGroupDetailInfo> workerGroupDetailInfos = client.listWorkerGroup(serviceId, Lists.newArrayList(), true);
            List<Backend> backends = Lists.newArrayList();
            for (WorkerGroupDetailInfo detailInfo : workerGroupDetailInfos) {
                List<WorkerInfo> workerInfos = detailInfo.getWorkersInfoList();
                for (WorkerInfo workerInfo : workerInfos) {
                    backends.add(workerToBackend(workerInfo));
                }
            }

            return backends;
        } catch (StarClientException e) {
            throw new UserException("Failed to get workers. error: " + e.getMessage());
        }
    }
}
