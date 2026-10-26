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

package com.starrocks.qe;

import com.google.common.collect.ImmutableMap;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;

public class WorkerProviderHelper {
    public interface NextWorkerIndexSupplier {
        int getAsInt(ComputeResource computeResource);
    }

    public static <C extends ComputeNode> C getNextWorker(ImmutableMap<Long, C> workers,
                                                          NextWorkerIndexSupplier getNextWorkerNodeIndex,
                                                          ComputeResource computeResource) {
        if (workers.isEmpty()) {
            return null;
        }
        int index = getNextWorkerNodeIndex.getAsInt(computeResource) % workers.size();
        if (index < 0) {
            index = -index;
        }
        return workers.values().asList().get(index);
    }

    /**
     * The splitmix64 finalizer: a cheap bijective 64-bit mix in which every output bit depends on every input bit.
     */
    public static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /**
     * Folds two ids into one backup affinity key, e.g. (colocate group id, bucket seq).
     */
    public static long combineAffinityKey(long first, long second) {
        return mix64(mix64(first) ^ second);
    }

    /**
     * Rendezvous hashing weight of {@code nodeId} for {@code affinityKey}: the node with the highest weight among the
     * candidates owns the key.
     */
    public static long rendezvousWeight(long affinityKey, long nodeId) {
        return mix64(affinityKey ^ mix64(nodeId));
    }
}
