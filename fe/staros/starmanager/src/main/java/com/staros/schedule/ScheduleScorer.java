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

import com.google.common.collect.ImmutableMap;
import com.staros.proto.PlacementPolicy;
import com.staros.schedule.select.Selector;
import com.staros.worker.Worker;
import com.staros.worker.WorkerManager;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScheduleScorer {
    /**
     * WEIGHT defines that the higher weight, the more preferable the worker will be chosen.
     * TODO: make these numbers configurable.
     */
    // A REVERSE weight, the larger the weight given, the less preferred the worker will be selected.
    private static final long WORKER_SHARD_NUMBER_WEIGHT = 1;
    // worker with existing pack replica are greatly preferred.
    private static final long SHARD_GROUP_PACK_WEIGHT = 10000;
    // worker with existing spread replica are not preferred
    private static final long SHARD_GROUP_SPREAD_WEIGHT = -100;

    private static final Map<PlacementPolicy, Long> SHARD_GROUP_WEIGHT =
            ImmutableMap.<PlacementPolicy, Long>builder()
                    .put(PlacementPolicy.PACK, SHARD_GROUP_PACK_WEIGHT)
                    .put(PlacementPolicy.SPREAD, SHARD_GROUP_SPREAD_WEIGHT)
                    .build();

    private final Map<Long, Double> scores;

    public ScheduleScorer(Collection<Long> targetWorkers) {
        this.scores = new HashMap<>(targetWorkers.size());
        targetWorkers.forEach(x -> this.scores.put(x, 0.0));
    }

    /**
     * Allow query worker manager about the worker info and change the score accordingly.
     *
     * @param manager WorkerManager to retrieve the worker status
     */
    public void apply(WorkerManager manager) {
        // TODO: take more metrics into consideration
        this.scores.replaceAll((id, val) -> {
            Worker worker = manager.getWorker(id);
            return worker == null ? val : val - Math.log(worker.getNumOfShards() * WORKER_SHARD_NUMBER_WEIGHT + 1);
        });
    }

    public void apply(PlacementPolicy policy, Collection<Long> workerIds) {
        if (!SHARD_GROUP_WEIGHT.containsKey(policy)) {
            return;
        }
        long weight = SHARD_GROUP_WEIGHT.get(policy);
        workerIds.forEach(key -> scores.computeIfPresent(key, (k, val) -> val + weight));
    }

    public boolean isEmpty() {
        return scores.isEmpty();
    }

    public void remove(long workerId) {
        scores.remove(workerId);
    }

    public Map<Long, Double> getScores() {
        return Collections.unmodifiableMap(scores);
    }

    public List<Long> selectHighEnd(Selector selector, int nSelect) {
        return selector.selectHighEnd(scores, nSelect);
    }

    public List<Long> selectLowEnd(Selector selector, int nSelect) {
        return selector.selectLowEnd(scores, nSelect);
    }
}
