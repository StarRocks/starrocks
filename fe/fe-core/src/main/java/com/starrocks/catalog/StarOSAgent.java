// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;

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
    private StarClient client;
    // private Map<Long, Long> workerIdToBeId;

    public StarOSAgent() {
        client = new StarClient();
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
