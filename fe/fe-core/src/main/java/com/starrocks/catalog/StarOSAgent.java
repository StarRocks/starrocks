// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
//import com.staros.client.StarClient;
//import com.staros.client.StarClientException;
//import com.staros.proto.ReplicaInfo;
//import com.staros.proto.ShardInfo;
//import com.staros.proto.WorkerInfo;
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
    // private Map<Long, Long> workerIdToBeId;

    public StarOSAgent() {
        client = new StarClient();
        serviceId = -1;
        // check if Config.starmanager_address == FE address
        if (Config.integrate_staros) {
            String[] starMgrAddr = Config.starmgr_address.split(":");
            if (!starMgrAddr[0].equals("127.0.0.1")) {
                LOG.warn("Config.starmgr_address not equal 127.0.0.1, it is {}", starMgrAddr[0]);
                System.exit(-1);
            }
        }
        // client.connectServer(Config.starmanager_address);
    }

    public List<Long> createShards(int numShards) {
        // TODO: support shardGroup, numReplicasPerShard and shardStorageType
        return client.createShards(numShards);
    }

    public long getPrimaryBackendIdByShard(long shardId) {
        long workerId = client.getPrimaryWorkerIdByShard(shardId);
        Worker worker = client.getWorker(workerId);
        return GlobalStateMgr.getCurrentSystemInfo().getBackendIdByHost(worker.getHost());
    }

    public Set<Long> getBackendIdsByShard(long shardId) {
        Set<Long> backendIds = Sets.newHashSet();
        for (long workerId : client.getWorkerIdsByShard(shardId)) {
            Worker worker = client.getWorker(workerId);
            long backendId = GlobalStateMgr.getCurrentSystemInfo().getBackendIdByHost(worker.getHost());
            backendIds.add(backendId);
        }
        return backendIds;
    }

    public void registerAndBootstrapService(String serviceName) {
        if (serviceId == -1) {
            client.registerService("starrocks");
            serviceId = client.bootstrapService("starrocks", serviceName);
        }
        LOG.info("serviceId from starClient is {} ", serviceId);
    }

    public void getServiceId(String serviceName) {
        if (serviceId != -1) {
            return;
        }
        serviceId = client.getServiceInfo(serviceName);
    }

    /*public List<Long> createShards(int numShards) {
         List<ShardInfo> shardInfos = Lists.newArrayList();
         try {
             shardInfos = client.createShard(1, numShards);
         } catch (StarClientException e) {
             LOG.warn(e);
             return Lists.newArrayList();
         }

         Preconditions.checkState(shardInfos.size() == numShards);
         return shardInfos.stream().map(shardInfo -> shardInfo.getShardId()).collect(Collectors.toList());
     }*/

    /*
     public long getPrimaryBackendIdByShard(long shardId) {
         Set<Long> backendIds = getBackendIdsByShard(shardId);
         Preconditions.checkState(backendIds.size() == 1);
         return backendIds.iterator().next();
     }*/

    /*
     public Set<Long> getBackendIdsByShard(long shardId) {
         Set<Long> backendIds = Sets.newHashSet();
         List<ShardInfo> shardInfos = Lists.newArrayList();
         try {
             shardInfos = client.getShardInfo(1, Lists.newArrayList(shardId));
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
             long backendId = Catalog.getCurrentSystemInfo().getBackendIdByHost(host);
             if (backendId == -1) {
                 LOG.warn("Backend does not exists. host: {}", host);
                 continue;
             }
             backendIds.add(backendId);
         }
         return backendIds;
     }*/

    /*
     public void registerAndBootStrapService(String serviceName) {
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
             LOG.info("get serviceId: {} by bootstrapService to strMgr", serviceId);
         } catch (StarClientException e) {
             if (e.getCode() != StarClientException.ExceptionCode.ALREADY_EXIST) {
                 LOG.warn(e);
                 System.exit(-1);
             } else {
                 getServiceId(String serviceName);
             }
         }
     }*/

    /*
     public void getServiceId(String serviceName) {
         if (serviceId != -1) {
             return;
         }
         try {
             ServiceInfo serviceInfo = client.getServiceInfo(serviceName);
             serviceId = serviceInfo.getServiceId();
         } catch (StarClientException e) {
             Log.warn(e);
             System.exit(-1);
         }
         LOG.info("get serviceId: {} by getServiceInfo from strMgr", serviceId);
    }*/

    // Mock StarClient
    private class StarClient {
        // private Map<Long, List<Replica>> shardIdToWorkerIds;
        private Map<Long, Worker> idToWorker;

        private long id = System.currentTimeMillis();

        public StarClient() {
            idToWorker = Maps.newHashMap();
        }

        public synchronized List<Long> createShards(int numShards) {
            // Get shard and workers from StarOS and update shardIdToWorkerIds
            List<Long> shards = Lists.newArrayList();
            for (int i = 0; i < numShards; ++i) {
                shards.add(id++);
            }
            return shards;
        }

        public synchronized long getPrimaryWorkerIdByShard(long shardId) {
            // Use primary replica of shard from StarOS
            return getWorkerIdsByShard(shardId).get(0);
        }

        public synchronized List<Long> getWorkerIdsByShard(long shardId) {
            // Use workers from StarOS
            idToWorker.clear();
            ImmutableMap<Long, Backend> idToBackend = GlobalStateMgr.getCurrentSystemInfo().getIdToBackend();
            for (Map.Entry<Long, Backend> entry : idToBackend.entrySet()) {
                idToWorker.put(entry.getKey(), new Worker(entry.getKey(), entry.getValue().getHost()));
            }
            List<Long> workerIds = idToWorker.keySet().stream().sorted().collect(Collectors.toList());
            return Lists.newArrayList(workerIds.get((int) (shardId % workerIds.size())));
        }

        public synchronized Worker getWorker(long id) {
            return idToWorker.get(id);
        }

        // register service
        public synchronized void registerService(String serviceTemplateName) {
            LOG.info("service {} registered.", serviceTemplateName);
        }

        // bootstrap service
        public synchronized long bootstrapService(String serviceTemplateName, String serviceName) {
            long serviceId = 1;
            LOG.info("service {} bootstrapped.", serviceName);
            return serviceId;
        }

        public long getServiceInfo(String serviceName) {
            long serviceId = 1;
            // get serviceId by client
            return serviceId;
        }
    }

    // Mock StarOS Worker
    private class Worker {
        private long id;
        private String host;

        public Worker(long id, String host) {
            this.id = id;
            this.host = host;
        }

        public String getHost() {
            return host;
        }
    }
}
