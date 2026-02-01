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


package com.staros.schedule;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.staros.proto.ReplicaState;
import com.staros.replica.Replica;
import com.staros.shard.Shard;
import com.staros.shard.ShardGroup;
import com.staros.shard.ShardManager;
import com.staros.worker.Worker;
import com.staros.worker.WorkerManager;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Helper class to get worker -> shard index and possibly group by worker groups
 */
public class ReplicaWorkerInvertIndex {
    private final Multimap<Long, Long> workerIndex;
    private final Set<Long> workerGroupIds;

    public ReplicaWorkerInvertIndex() {
        this.workerIndex = ArrayListMultimap.create();
        this.workerGroupIds = new HashSet<>();
    }

    private void addReplica(long shardId, Worker worker) {
        if (worker == null) {
            return;
        }
        workerGroupIds.add(worker.getGroupId());
        workerIndex.put(worker.getWorkerId(), shardId);
    }

    public void buildFrom(WorkerManager workerManager, ShardManager shardManager, ShardGroup group) {
        for (long id : group.getShardIds()) {
            Shard shard = shardManager.getShard(id);
            if (shard == null) {
                continue;
            }
            addReplicas(workerManager, shard);
        }
    }

    public void addReplicas(WorkerManager workerManager, Shard shard) {
        shard.getReplica().stream().filter(x -> x.getState() != ReplicaState.REPLICA_SCALE_IN).map(Replica::getWorkerId)
                .forEach(x -> addReplica(shard.getShardId(), workerManager.getWorker(x)));
    }

    public void addReplica(long shardId, long workerId, long workerGroupId) {
        workerGroupIds.add(workerGroupId);
        workerIndex.put(workerId, shardId);
    }

    public void removeReplica(long shardId, long workerId) {
        // leave the workerGroupIds untouched for now
        workerIndex.remove(workerId, shardId);
    }

    public Collection<Long> getReplicaShardList(long workerId) {
        if (workerIndex.containsKey(workerId)) {
            return workerIndex.get(workerId);
        } else {
            return Collections.emptyList();
        }
    }

    public Collection<Long> getAllWorkerGroupIds() {
        return workerGroupIds;
    }
}
