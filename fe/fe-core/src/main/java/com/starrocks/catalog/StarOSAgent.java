// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.staros.client.StarClient;
import com.staros.client.StarClientException;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ServiceInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.WorkerInfo;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * StarOSAgent is responsible for
 * 1. Encapsulation of StarClient api.
 * 2. Maintenance of StarOS worker to StarRocks backend map.
 */
public class StarOSAgent {
    private static final Logger LOG = LogManager.getLogger(StarOSAgent.class);

    private StarClient client;
    private long serviceId;
    private Map<String, Long> workerToId;
    private Map<Long, Long> workerToBackend;

    public StarOSAgent() {
        serviceId = -1;
        // check if Config.starmanager_address == FE address
        if (Config.integrate_staros) {
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
    }

    public List<Long> createShards(int numShards) {
        List<ShardInfo> shardInfos = Lists.newArrayList();
        try {
            shardInfos = client.createShard(serviceId, numShards);
        } catch (StarClientException e) {
            LOG.warn(e);
            return Lists.newArrayList();
        }

        Preconditions.checkState(shardInfos.size() == numShards);
        return shardInfos.stream().map(shardInfo -> shardInfo.getShardId()).collect(Collectors.toList());
    }

    public long getPrimaryBackendIdByShard(long shardId) {
        Set<Long> backendIds = getBackendIdsByShard(shardId);
        Preconditions.checkState(backendIds.size() == 1);
        return backendIds.iterator().next();
    }

    public Set<Long> getBackendIdsByShard(long shardId) {
        Set<Long> backendIds = Sets.newHashSet();
        List<ShardInfo> shardInfos = Lists.newArrayList();
        try {
            shardInfos = client.getShardInfo(serviceId, Lists.newArrayList(shardId));
        } catch (StarClientException e) {
            LOG.warn(e);
            return Sets.newHashSet();
        }

        Preconditions.checkState(shardInfos.size() == 1);
        List<ReplicaInfo> replicaInfos = shardInfos.get(0).getReplicaInfoList();
        for (ReplicaInfo replicaInfo : replicaInfos) {
            WorkerInfo workerInfo = replicaInfo.getWorkerInfo();
            String ipPort = workerInfo.getIpPort();
            String host = ipPort.split(":")[0];
            long workerId = workerToId.get(ipPort);
            if (!workerToBackend.containsKey(workerId)) {
                LOG.warn("Backend does not exists. host: {}", host);
                continue;
            }
            long backendId = workerToBackend.get(workerId);
            backendIds.add(backendId);
        }
        return backendIds;
    }

    public void registerAndBootstrapService(String serviceName) {
        if (serviceId != -1) {
            return;
        }

        try {
            client.registerService("starrocks");
        } catch (StarClientException e) {
            if (e.getCode() != StarClientException.ExceptionCode.ALREADY_EXIST) {
                LOG.warn(e);
                System.exit(-1);
            }
        }

        try {
            serviceId = client.bootstrapService("starrocks", serviceName);
            LOG.info("get serviceId: {} by bootstrapService to starMgr", serviceId);
        } catch (StarClientException e) {
            if (e.getCode() != StarClientException.ExceptionCode.ALREADY_EXIST) {
                LOG.warn(e);
                System.exit(-1);
            } else {
                getServiceId(serviceName);
            }
        }
    }

    public void getServiceId(String serviceName) {
        if (serviceId != -1) {
            return;
        }
        try {
            ServiceInfo serviceInfo = client.getServiceInfo(serviceName);
            serviceId = serviceInfo.getServiceId();
        } catch (StarClientException e) {
            LOG.warn(e);
            System.exit(-1);
        }
        LOG.info("get serviceId {} from starMgr", serviceId);
    }

    public void addWorker(long backendId, String workerIpPort) {
        if (serviceId == -1) {
            LOG.warn("When addWorker serviceId is -1");
            return;
        }
        if (workerToId.containsKey(workerIpPort)) {
            return;
        }
        long workerId = -1;
        try {
            workerId = client.addWorker(serviceId, workerIpPort);
        } catch (StarClientException e) {
            if (e.getCode() != StarClientException.ExceptionCode.ALREADY_EXIST) {
                LOG.warn(e);
                return;
            } else {
                // get workerId from staros
                try {
                    WorkerInfo workerInfo = client.getWorkerInfo(serviceId, workerIpPort);
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
        LOG.info("add worker {} succ, backendId is {}", workerId, backendId);
    }
}
