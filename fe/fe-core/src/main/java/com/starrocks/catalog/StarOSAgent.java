// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.staros.client.StarClient;
import com.staros.client.StarClientException;
import com.staros.proto.ServiceInfo;
import com.staros.proto.StatusCode;
import com.staros.proto.WorkerInfo;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
    }

    // for ut only
    public long getServiceId() {
        return serviceId;
    }

    // for ut only
    public void setServiceId(long id) {
        this.serviceId = id;
    }

    public List<Long> createShards(int numShards) {
        return Lists.newArrayList();
    }

    public long getPrimaryBackendIdByShard(long shardId) {
        return 0;
    }

    public Set<Long> getBackendIdsByShard(long shardId) {
        return Sets.newHashSet();
    }

    public void registerAndBootstrapService(String serviceName) {
        if (serviceId != -1) {
            return;
        }

        try {
            client.registerService("starrocks");
        } catch (StarClientException e) {
            if (e.getCode() != StatusCode.ALREADY_EXIST) {
                LOG.warn(e);
                System.exit(-1);
            }
        }

        try {
            serviceId = client.bootstrapService("starrocks", serviceName);
            LOG.info("get serviceId: {} by bootstrapService to starMgr", serviceId);
        } catch (StarClientException e) {
            if (e.getCode() != StatusCode.ALREADY_EXIST) {
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

    // for ut only
    public long getWorkerId(String workerIpPort) {
        return workerToId.get(workerIpPort);
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
            if (e.getCode() != StatusCode.ALREADY_EXIST) {
                LOG.warn(e);
                return;
            } else {
                // get workerId from starMgr
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
        LOG.info("add worker {} success, backendId is {}", workerId, backendId);
    }

    public void removeWorker(String workerIpPort) throws DdlException {
        long workerId = -1;
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
                return;
            }
        }

        try {
            client.removeWorker(serviceId, workerId);
        } catch (StarClientException e) {
            // when multi threads remove this worker, maybe we would get "NOT_EXIST"
            // but it is right, so only need to throw exception
            // if code is not StatusCode.NOT_EXIST
            if (e.getCode() != StatusCode.NOT_EXIST) {
                throw new DdlException("Failed to remove worker. error: " + e.getMessage());
            }
        }

        workerToBackend.remove(workerId);
        workerToId.remove(workerIpPort);
        LOG.info("remove worker {} success from StarMgr", workerIpPort);
    }

    public long getWorkerIdByBackendId(long backendId) {
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
