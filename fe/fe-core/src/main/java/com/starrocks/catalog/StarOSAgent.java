// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.staros.client.StarClient;
import com.staros.client.StarClientException;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaRole;
import com.staros.proto.ServiceInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.WorkerInfo;
import com.staros.util.LockCloseable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
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
    private AtomicLong serviceId;
    private Map<String, Long> workerToId;
    private Map<Long, Long> workerToBackend;
    private ReentrantReadWriteLock rwLock;

    public StarOSAgent() {
        serviceId = new AtomicLong(-1L);

        // check if Config.starmanager_address == FE address
        if (Config.integrate_starmgr) {
            String[] starMgrAddr = Config.starmgr_address.split(":");
            if (!starMgrAddr[0].equals("127.0.0.1")) {
                LOG.warn("Config.starmgr_address not equal 127.0.0.1, it is {}", starMgrAddr[0]);
                System.exit(-1);
            }
        }
        client = new StarClient();
        client.connectServer(Config.starmgr_address);

        workerToId = Maps.newHashMap();
        workerToBackend = Maps.newHashMap();
        rwLock = new ReentrantReadWriteLock();
    }

    private void prepare() {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (serviceId.equals(-1L)) {
                getServiceId();
            }
        }
    }

    // for ut only
    public long getServiceIdForTest() {
        return this.serviceId.get();
    }

    // for ut only
    public void setServiceId(long id) {
        this.serviceId.set(id);
    }

    public void registerAndBootstrapService() {
        try {
            client.registerService("starrocks");
        } catch (StarClientException e) {
            if (e.getCode() != StarClientException.ExceptionCode.ALREADY_EXIST) {
                LOG.warn(e);
                System.exit(-1);
            }
        }

        try {
            serviceId.set(client.bootstrapService("starrocks", SERVICE_NAME));
            LOG.info("get serviceId: {} by bootstrapService to starMgr", serviceId);
        } catch (StarClientException e) {
            if (e.getCode() != StarClientException.ExceptionCode.ALREADY_EXIST) {
                LOG.warn(e);
                System.exit(-1);
            } else {
                getServiceId();
            }
        }
    }

    public void getServiceId() {
        try {
            ServiceInfo serviceInfo = client.getServiceInfo(SERVICE_NAME);
            serviceId.set(serviceInfo.getServiceId());
        } catch (StarClientException e) {
            LOG.warn(e);
            return;
        }
        LOG.info("get serviceId {} from starMgr", serviceId);
    }

    public String getServiceStorageUri() {
        // TODO: get from StarMgr
        return String.format("s3://bucket/%d/", serviceId);
    }

    // for ut only
    public long getWorkerId(String workerIpPort) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return workerToId.get(workerIpPort);
        }
    }

    public void addWorker(long backendId, String workerIpPort) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (serviceId.equals(-1L)) {
                LOG.warn("When addWorker serviceId is -1");
                return;
            }

            if (workerToId.containsKey(workerIpPort)) {
                return;
            }

            long workerId = -1;
            try {
                workerId = client.addWorker(serviceId.get(), workerIpPort);
            } catch (StarClientException e) {
                if (e.getCode() != StarClientException.ExceptionCode.ALREADY_EXIST) {
                    LOG.warn(e);
                    return;
                } else {
                    // get workerId from starMgr
                    try {
                        WorkerInfo workerInfo = client.getWorkerInfo(serviceId.get(), workerIpPort);
                        workerId = workerInfo.getWorkerId();
                    } catch (StarClientException e2) {
                        LOG.warn(e2);
                        return;
                    }
                    LOG.info("worker {} already added in starMgr", workerId);
                }
            }
            workerToId.put(workerIpPort, workerId);
            workerToBackend.put(workerId, backendId);
            LOG.info("add worker {} success, backendId is {}", workerId, backendId);
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
                    WorkerInfo workerInfo = client.getWorkerInfo(serviceId.get(), workerIpPort);
                    workerId = workerInfo.getWorkerId();
                } catch (StarClientException e) {
                    if (e.getCode() != StarClientException.ExceptionCode.NOT_EXIST) {
                        throw new DdlException("Failed to get worker id from starMgr. error: "
                                + e.getMessage());
                    }

                    LOG.info("worker {} not exist.", workerIpPort);
                }
            }
        }

        return workerId;
    }

    public void removeWorker(String workerIpPort) throws DdlException {

        long workerId = getWorker(workerIpPort);

        try {
            client.removeWorker(serviceId.get(), workerId);
        } catch (StarClientException e) {
            // when multi threads remove this worker, maybe we would get "NOT_EXIST"
            // but it is right, so only need to throw exception
            // if code is not StarClientException.ExceptionCode.NOT_EXIST
            if (e.getCode() != StarClientException.ExceptionCode.NOT_EXIST) {
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

    public long getWorkerIdByBackendId(long backendId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            long workerId = -1;
            for (Map.Entry<Long, Long> entry : workerToBackend.entrySet()) {
                if (entry.getValue() == backendId) {
                    workerId = entry.getKey();
                    break;
                }
            }
            return workerId;
        }
    }

    public List<Long> createShards(int numShards, Map<String, String> properties) throws DdlException {
        prepare();
        List<ShardInfo> shardInfos = null;
        try {
            // TODO: support properties
            shardInfos = client.createShard(serviceId.get(), numShards);
        } catch (StarClientException e) {
            throw new DdlException("Failed to create shards. error: " + e.getMessage());
        }

        Preconditions.checkState(shardInfos.size() == numShards);
        return shardInfos.stream().map(ShardInfo::getShardId).collect(Collectors.toList());
    }

    private List<ReplicaInfo> getShardReplicas(long shardId) throws UserException {
        prepare();
        try {
            List<ShardInfo> shardInfos = client.getShardInfo(serviceId.get(), Lists.newArrayList(shardId));
            Preconditions.checkState(shardInfos.size() == 1);
            return shardInfos.get(0).getReplicaInfoList();
        } catch (StarClientException e) {
            throw new UserException("Failed to get shard info. error: " + e.getMessage());
        }
    }

    public long getPrimaryBackendIdByShard(long shardId) throws UserException {
        List<ReplicaInfo> replicas = getShardReplicas(shardId);

        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            for (ReplicaInfo replicaInfo : replicas) {
                if (replicaInfo.getReplicaRole() == ReplicaRole.PRIMARY) {
                    WorkerInfo workerInfo = replicaInfo.getWorkerInfo();
                    long workerId = workerInfo.getWorkerId();
                    if (!workerToBackend.containsKey(workerId)) {
                        // get backendId from system info by host & starletPort
                        String workerAddr = workerInfo.getIpPort();
                        String[] pair = workerAddr.split(":");
                        long backendId = GlobalStateMgr.getCurrentSystemInfo()
                                .getBackendIdWithStarletPort(pair[0], Integer.parseInt(pair[1]));

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

    public Set<Long> getBackendIdsByShard(long shardId) throws UserException {
        List<ReplicaInfo> replicas = getShardReplicas(shardId);

        Set<Long> backendIds = Sets.newHashSet();
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            for (ReplicaInfo replicaInfo : replicas) {
                // TODO: check worker state
                WorkerInfo workerInfo = replicaInfo.getWorkerInfo();
                long workerId = workerInfo.getWorkerId();
                if (!workerToBackend.containsKey(workerId)) {
                    // get backendId from system info
                    String workerAddr = workerInfo.getIpPort();
                    String[] pair = workerAddr.split(":");
                    long backendId = GlobalStateMgr.getCurrentSystemInfo()
                            .getBackendIdWithStarletPort(pair[0], Integer.parseInt(pair[1]));

                    if (backendId == -1L) {
                        throw new UserException("Failed to get backend by worker. worker id: " + workerId);
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
}
