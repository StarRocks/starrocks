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

package com.starrocks.connector.lance;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class LanceWorkerSelector {
    // Bound retained state when warehouses or eligible worker sets change.
    private static final LoadingCache<WorkerPool, AtomicInteger> NEXT_WORKER_INDEX = CacheBuilder.newBuilder()
            .maximumSize(1024)
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(CacheLoader.from(() -> new AtomicInteger()));

    private record WorkerPool(long warehouseId, long workerGroupId, List<Long> workerIds) {
    }

    private LanceWorkerSelector() {
    }

    public static ComputeNode selectWorker(WorkerProvider provider) throws StarRocksException {
        List<ComputeNode> workers = new ArrayList<>(provider.getAllWorkers());
        if (workers.isEmpty()) {
            throw new StarRocksException("Failed to find backend to execute");
        }
        workers.sort(Comparator.comparingLong(ComputeNode::getId));
        ComputeResource resource = provider.getComputeResource();
        WorkerPool pool = new WorkerPool(resource.getWarehouseId(), resource.getWorkerGroupId(),
                workers.stream().map(ComputeNode::getId).toList());
        int index = Math.floorMod(NEXT_WORKER_INDEX.getUnchecked(pool).getAndIncrement(), workers.size());
        return workers.get(index);
    }
}
