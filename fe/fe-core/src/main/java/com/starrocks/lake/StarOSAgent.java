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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.staros.client.StarClient;
import com.staros.client.StarClientException;
import com.staros.manager.StarManagerServer;
import com.staros.proto.CreateMetaGroupInfo;
import com.staros.proto.CreateShardGroupInfo;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
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
import com.staros.proto.WorkerInfo;
import com.staros.util.LockCloseable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/**
 * StarOSAgent is responsible for
 * 1. Encapsulation of StarClient api.
 * 2. Maintenance of StarOS worker to StarRocks backend map.
 */
public class StarOSAgent {
    private static final Logger LOG = LogManager.getLogger(StarOSAgent.class);

    public static final String SERVICE_NAME = "starrocks";

    public static final long DEFAULT_WORKER_GROUP_ID = 0L;

    private StarClient client;
    private String serviceId;
    private Map<String, Long> workerToId;
    private Map<Long, Long> workerToBackend;
    private ReentrantReadWriteLock rwLock;

    public StarOSAgent() {
        serviceId = "";
        workerToId = Maps.newHashMap();
        workerToBackend = Maps.newHashMap();
        rwLock = new ReentrantReadWriteLock();
    }

    public boolean init(StarManagerServer server) {
        client = new StarClient(server);
        client.connectServer(String.format("127.0.0.1:%d", Config.cloud_native_meta_port));
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
            LOG.warn("Failed to get serviceId from starMgr. Error:", e);
            return;
        }
        LOG.info("get serviceId {} from starMgr", serviceId);
    }

    public String addFileStore(FileStoreInfo fsInfo) throws DdlException {
        try {
            return client.addFileStore(fsInfo, serviceId);
        } catch (StarClientException e) {
            throw new DdlException("Failed to add file store", e);
        }
    }

    public void removeFileStoreByName(String fsName) throws DdlException {
        try {
            client.removeFileStoreByName(fsName, serviceId);
        } catch (StarClientException e) {
            throw new DdlException("Failed to remove file store", e);
        }
    }

    public void updateFileStore(FileStoreInfo fsInfo) throws DdlException {
        try {
            client.updateFileStore(fsInfo, serviceId);
        } catch (StarClientException e) {
            throw new DdlException("Failed to update file store", e);
        }
    }

    public FileStoreInfo getFileStoreByName(String fsName) throws DdlException {
        try {
            return client.getFileStoreByName(fsName, serviceId);
        } catch (StarClientException e) {
            if (e.getCode() == StatusCode.NOT_EXIST) {
                return null;
            }
            throw new DdlException("Failed to get file store", e);
        }
    }

    public FileStoreInfo getFileStore(String fsKey) throws DdlException {
        try {
            return client.getFileStore(fsKey, serviceId);
        } catch (StarClientException e) {
            if (e.getCode() == StatusCode.NOT_EXIST) {
                return null;
            }
            throw new DdlException("Failed to get file store", e);
        }
    }

    public List<FileStoreInfo> listFileStore() throws DdlException {
        try {
            return client.listFileStore(serviceId);
        } catch (StarClientException e) {
            throw new DdlException("Failed to list file store", e);
        }
    }

    private FileStoreType getFileStoreType(String storageType) {
        if (storageType == null) {
            return null;
        }
        for (FileStoreType type : FileStoreType.values()) {
            if (type.name().equalsIgnoreCase(storageType)) {
                return type;
            }
        }
        return null;
    }

    public FilePathInfo allocateFilePath(long tableId) throws DdlException {
        try {
            FileStoreType fsType = getFileStoreType(Config.cloud_native_storage_type);
            if (fsType == null || fsType == FileStoreType.INVALID) {
                throw new DdlException("Invalid cloud native storage type: " + Config.cloud_native_storage_type);
            }
            FilePathInfo pathInfo = client.allocateFilePath(serviceId, fsType, Long.toString(tableId));
            LOG.debug("Allocate file path from starmgr: {}", pathInfo);
            return pathInfo;
        } catch (StarClientException e) {
            throw new DdlException("Failed to allocate file path from StarMgr", e);
        }
    }

    public FilePathInfo allocateFilePath(String storageVolumeId, long tableId) throws DdlException {
        try {
            FilePathInfo pathInfo = client.allocateFilePath(serviceId,
                     storageVolumeId, Long.toString(tableId));
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

    // for ut only
    public long getWorkerId(String workerIpPort) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return workerToId.get(workerIpPort);
        }
    }

    private long getWorker(String workerIpPort) throws DdlException {
        long workerId = -1;
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            if (workerToId.containsKey(workerIpPort)) {
                workerId = workerToId.get(workerIpPort);

            } else {
                // When FE && staros restart, workerToId is Empty, but staros already persisted
                // worker infos, so we need to get workerId from starMgr
                try {
                    WorkerInfo workerInfo = client.getWorkerInfo(serviceId, workerIpPort);
                    workerId = workerInfo.getWorkerId();
                } catch (StarClientException e) {
                    if (e.getCode() != StatusCode.NOT_EXIST) {
                        throw new DdlException("Failed to get worker id from starMgr. error: "
                                + e.getMessage());
                    }

                    LOG.info("worker {} not exist.", workerIpPort);
                }
            }
        }

        return workerId;
    }

    public void addWorker(long nodeId, String workerIpPort, long workerGroupId) {
        prepare();
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (serviceId.equals("")) {
                LOG.warn("When addWorker serviceId is empty");
                return;
            }

            if (workerToId.containsKey(workerIpPort)) {
                return;
            }

            long workerId = -1;
            try {
                workerId = client.addWorker(serviceId, workerIpPort, workerGroupId);
            } catch (StarClientException e) {
                if (e.getCode() != StatusCode.ALREADY_EXIST) {
                    LOG.warn("Failed to addWorker. Error: {}", e);
                    return;
                } else {
                    // get workerId from starMgr
                    try {
                        WorkerInfo workerInfo = client.getWorkerInfo(serviceId, workerIpPort);
                        workerId = workerInfo.getWorkerId();
                    } catch (StarClientException e2) {
                        LOG.warn("Failed to get getWorkerInfo. Error: {}", e2);
                        return;
                    }
                    LOG.info("worker {} already added in starMgr", workerId);
                }
            }
            tryRemovePreviousWorker(nodeId);
            workerToId.put(workerIpPort, workerId);
            workerToBackend.put(workerId, nodeId);
            LOG.info("add worker {} success, backendId is {}", workerId, nodeId);
        }
    }

    // remove previous worker with same backend id
    private void tryRemovePreviousWorker(long backendId) {
        long prevWorkerId = getWorkerIdByBackendIdInternal(backendId);
        if (prevWorkerId < 0) {
            return;
        }
        try {
            client.removeWorker(serviceId, prevWorkerId);
        } catch (StarClientException e) {
            // TODO: fix this corner case later in star mgr
            LOG.error("Failed to remove worker {} with backend id {}. error: {}", prevWorkerId, backendId, e.getMessage());
        }
        workerToBackend.remove(prevWorkerId);
        workerToId.entrySet().removeIf(e -> e.getValue() == prevWorkerId);
    }

    public void removeWorker(String workerIpPort) throws DdlException {
        prepare();

        long workerId = getWorker(workerIpPort);

        try {
            client.removeWorker(serviceId, workerId);
        } catch (StarClientException e) {
            // when multi threads remove this worker, maybe we would get "NOT_EXIST"
            // but it is right, so only need to throw exception
            // if code is not StarClientException.ExceptionCode.NOT_EXIST
            if (e.getCode() != StatusCode.NOT_EXIST) {
                throw new DdlException("Failed to remove worker. error: " + e.getMessage());
            }
        }

        removeWorkerFromMap(workerId, workerIpPort);
    }

    public void removeWorkerFromMap(long workerId, String workerIpPort) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            workerToBackend.remove(workerId);
            workerToId.remove(workerIpPort);
        }

        LOG.info("remove worker {} success from StarMgr", workerIpPort);
    }

    public void removeWorkerFromMap(String workerIpPort) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Long workerId = workerToId.remove(workerIpPort);
            if (workerId != null) {
                workerToBackend.remove(workerId);
            }
        }
        LOG.info("remove worker {} success from StarMgr", workerIpPort);
    }

    public long getWorkerIdByBackendId(long backendId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return getWorkerIdByBackendIdInternal(backendId);
        }
    }

    private long getWorkerIdByBackendIdInternal(long backendId) {
        long workerId = -1;
        for (Map.Entry<Long, Long> entry : workerToBackend.entrySet()) {
            if (entry.getValue() == backendId) {
                workerId = entry.getKey();
                break;
            }
        }
        return workerId;
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

    public List<Long> createShards(int numShards, FilePathInfo pathInfo, FileCacheInfo cacheInfo, long groupId)
        throws DdlException {
        return createShards(numShards, pathInfo, cacheInfo, groupId, null, Collections.EMPTY_MAP);
    }

    public List<Long> createShards(int numShards, FilePathInfo pathInfo, FileCacheInfo cacheInfo, long groupId,
                                   @NotNull Map<String, String> properties)
            throws DdlException {
        return createShards(numShards, pathInfo, cacheInfo, groupId, null, properties);
    }

    public List<Long> createShards(int numShards, FilePathInfo pathInfo, FileCacheInfo cacheInfo, long groupId,
                                   @Nullable List<Long> matchShardIds, @NotNull Map<String, String> properties)
        throws DdlException {
        if (matchShardIds != null) {
            Preconditions.checkState(numShards == matchShardIds.size());
        }
        prepare();
        List<ShardInfo> shardInfos = null;
        try {
            List<CreateShardInfo> createShardInfoList = new ArrayList<>(numShards);

            CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
            builder.setReplicaCount(1)
                    .addGroupIds(groupId)
                    .setPathInfo(pathInfo)
                    .setCacheInfo(cacheInfo)
                    .putAllShardProperties(properties);

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
        if (shardIds.isEmpty()) {
            return;
        }
        prepare();
        try {
            client.deleteShard(serviceId, shardIds);
        } catch (StarClientException e) {
            LOG.warn("Failed to delete shards. error: {}", e.getMessage());
            throw new DdlException("Failed to delete shards. error: " + e.getMessage());
        }
    }

    private List<ReplicaInfo> getShardReplicas(long shardId) throws UserException {
        return getShardReplicas(shardId, DEFAULT_WORKER_GROUP_ID);
    }

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

    private long getAvailableBackendId(String host, int starletPort) {
        long backendId = GlobalStateMgr.getCurrentSystemInfo()
                .getBackendIdWithStarletPort(host, starletPort);
        if (backendId == -1L) {
            backendId = GlobalStateMgr.getCurrentSystemInfo().
                    getComputeNodeIdWithStarletPort(host, starletPort);
        }
        return backendId;
    }

    public long getPrimaryComputeNodeIdByShard(long shardId) throws UserException {
        return getPrimaryComputeNodeIdByShard(shardId, DEFAULT_WORKER_GROUP_ID);
    }

    public long getPrimaryComputeNodeIdByShard(long shardId, long workerGroupId) throws UserException {
        List<ReplicaInfo> replicas = getShardReplicas(shardId, workerGroupId);

        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            for (ReplicaInfo replicaInfo : replicas) {
                if (replicaInfo.getReplicaRole() == ReplicaRole.PRIMARY) {
                    WorkerInfo workerInfo = replicaInfo.getWorkerInfo();
                    long workerId = workerInfo.getWorkerId();
                    if (!workerToBackend.containsKey(workerId)) {
                        // get backendId from system info by host & starletPort
                        String workerAddr = workerInfo.getIpPort();
                        String[] pair = workerAddr.split(":");

                        long backendId = getAvailableBackendId(pair[0], Integer.parseInt(pair[1]));
                        if (backendId == -1L) {
                            throw new UserException("Failed to get backend by worker. worker id: " + workerId);
                        }
                        // put it into map
                        workerToId.put(workerAddr, workerId);
                        workerToBackend.put(workerId, backendId);
                        return backendId;
                    }
                    return workerToBackend.get(workerId);
                }
            }
        }
        throw new UserException("Failed to get primary backend. shard id: " + shardId);
    }

    public Set<Long> getBackendIdsByShard(long shardId, long workerGroupId) throws UserException {
        List<ReplicaInfo> replicas = getShardReplicas(shardId, workerGroupId);

        Set<Long> backendIds = Sets.newHashSet();
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            for (ReplicaInfo replicaInfo : replicas) {
                // TODO: check worker state
                WorkerInfo workerInfo = replicaInfo.getWorkerInfo();
                long workerId = workerInfo.getWorkerId();
                if (!workerToBackend.containsKey(workerId)) {
                    // get backendId from system info
                    String workerAddr = workerInfo.getIpPort();
                    String[] pair = workerAddr.split(":");
                    long backendId = getAvailableBackendId(pair[0], Integer.parseInt(pair[1]));

                    if (backendId == -1L) {
                        LOG.info("can't find backendId with starletPort for {}.", workerAddr);
                        // FIXME: workaround fix of missing starletPort due to Backend::write() missing the field during
                        //  saveImage(). Refer to: https://starrocks.atlassian.net/browse/SR-16340
                        if (workerInfo.getWorkerPropertiesMap().containsKey("be_port")) {
                            int bePort = Integer.parseInt(workerInfo.getWorkerPropertiesMap().get("be_port"));
                            ComputeNode cn = GlobalStateMgr.getCurrentSystemInfo()
                                    .getBackendWithBePort(pair[0], bePort);
                            if (cn == null) {
                                cn = GlobalStateMgr.getCurrentSystemInfo()
                                        .getComputeNodeWithBePort(pair[0], bePort);
                                if (cn == null) {
                                    LOG.warn("can't find backendId with bePort:{} for {}.", bePort, workerAddr);
                                } else {
                                    backendId = cn.getId();
                                }
                            } else {
                                backendId = cn.getId();
                            }
                        }
                        // Can't find the backendId, give up
                        if (backendId == -1L) {
                            continue;
                        }
                    }

                    // put it into map
                    workerToId.put(workerAddr, workerId);
                    workerToBackend.put(workerId, backendId);
                    backendIds.add(backendId);
                } else {
                    backendIds.add(workerToBackend.get(workerId));
                }
            }
        }
        return backendIds;
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
                        .setDeleteMetaGroupIfEmpty(true)
                        .build();
                builder.setQuitInfo(quitInfo);
            }

            builder.addAllShardGroupIds(shardGroupIds);

            client.updateMetaGroup(serviceId, builder.build());
        } catch (StarClientException e) {
            throw new DdlException("Failed to update meta group. error: " + e.getMessage());
        }
    }

    public boolean queryMetaGroupStable(long metaGroupId) {
        prepare();

        try {
            return client.queryMetaGroupStable(serviceId, metaGroupId);
        } catch (StarClientException e) {
            LOG.warn("Failed to query meta group {} whether stable. error:{}", metaGroupId, e.getMessage());
        }
        return false; // return false if any error happens
    }

    public List<Long> getWorkersByWorkerGroup(long workerGroupId) throws UserException {
        List<Long> nodeIds = new ArrayList<>();
        prepare();
        try {
            List<WorkerGroupDetailInfo> workerGroupDetailInfos = client.
                    listWorkerGroup(serviceId, Collections.singletonList(workerGroupId), true);
            for (WorkerGroupDetailInfo detailInfo : workerGroupDetailInfos) {
                List<WorkerInfo> workerInfos = detailInfo.getWorkersInfoList();
                for (WorkerInfo workerInfo : workerInfos) {
                    if (workerToBackend.containsKey(workerInfo.getWorkerId())) {
                        nodeIds.add(workerToBackend.get(workerInfo.getWorkerId()));
                    } else {
                        // workerToBackend may not container this worker, so need to get it from systemInfoSerivce
                        // and fill it
                        long workerId = workerInfo.getWorkerId();
                        String workerAddr = workerInfo.getIpPort();
                        String[] pair = workerAddr.split(":");
                        long nodeId = getAvailableBackendId(pair[0], Integer.parseInt(pair[1]));
                        if (nodeId != -1L) {
                            nodeIds.add(nodeId);

                            // put it into map
                            workerToId.put(workerAddr, workerId);
                            workerToBackend.put(workerId, nodeId);
                        }
                    }
                }
            }
            return nodeIds;
        } catch (StarClientException e) {
            throw new UserException("Failed to get workers by group id. error: " + e.getMessage());
        }
    }

    // dump all starmgr meta, for DEBUG purpose
    public String dump() {
        prepare();

        try {
            return client.dump();
        } catch (StarClientException e) {
            String str = "Fail to dump starmgr meta, " + e.getMessage();
            return str;
        }
    }
}
