// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.staros.client.StarClient;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.WorkerInfo;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
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
    // private Map<Long, Long> workerIdToBeId;

    public StarOSAgent() {
        client = new StarClient();
        client.connectServer(Config.starmanager_address);
    }

    public List<Long> createShards(int numShards) {
        List<ShardInfo> shardInfos = client.createShard(1, numShards);
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
        List<ShardInfo> shardInfos = client.getShardInfo(1, Lists.newArrayList(shardId));
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
    }

    /*
    public List<Long> createShards(int numShards) {
        // TODO: support shardGroup, numReplicasPerShard and shardStorageType
        return client.createShards(numShards);
    }

    public long getPrimaryBackendIdByShard(long shardId) {
        long workerId = client.getPrimaryWorkerIdByShard(shardId);
        Worker worker = client.getWorker(workerId);
        return Catalog.getCurrentSystemInfo().getBackendIdByHost(worker.getHost());
    }

    public Set<Long> getBackendIdsByShard(long shardId) {
        Set<Long> backendIds = Sets.newHashSet();
        for (long workerId : client.getWorkerIdsByShard(shardId)) {
            Worker worker = client.getWorker(workerId);
            long backendId = Catalog.getCurrentSystemInfo().getBackendIdByHost(worker.getHost());
            backendIds.add(backendId);
        }
        return backendIds;
    }

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
            ImmutableMap<Long, Backend> idToBackend = Catalog.getCurrentSystemInfo().getIdToBackend();
            for (Map.Entry<Long, Backend> entry : idToBackend.entrySet()) {
                idToWorker.put(entry.getKey(), new Worker(entry.getKey(), entry.getValue().getHost()));
            }
            List<Long> workerIds = idToWorker.keySet().stream().sorted().collect(Collectors.toList());
            return Lists.newArrayList(workerIds.get((int) (shardId % workerIds.size())));
        }

        public synchronized Worker getWorker(long id) {
            return idToWorker.get(id);
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
     */
}
