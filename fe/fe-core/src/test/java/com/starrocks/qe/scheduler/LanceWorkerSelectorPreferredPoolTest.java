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

package com.starrocks.qe.scheduler;

import com.google.common.collect.ImmutableMap;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LanceWorkerSelectorPreferredPoolTest {

    @Test
    void testSelectWorkerUsesPreferredPool() throws Exception {
        Map<Long, ComputeNode> map = new HashMap<>();
        for (long i = 0; i < 3; i++) {
            ComputeNode node = new ComputeNode(i, "192.168.1." + i, 9030);
            node.setAlive(true);
            map.put(i, node);
        }
        ImmutableMap<Long, ComputeNode> nodes = ImmutableMap.copyOf(map);
        DefaultWorkerProvider base = new DefaultWorkerProvider(
                nodes, ImmutableMap.of(), nodes, ImmutableMap.of(), false, WarehouseManager.DEFAULT_RESOURCE);
        WorkerProvider restricted = new LocationRestrictedWorkerProvider(base, Set.of(0L, 1L, 2L), Set.of(0L, 1L));

        Set<Long> seen = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            ComputeNode worker = LanceWorkerSelector.selectWorker(restricted);
            seen.add(worker.getId());
            Assertions.assertTrue(worker.getId() == 0L || worker.getId() == 1L);
        }
        Assertions.assertFalse(seen.contains(2L));
        Assertions.assertFalse(seen.isEmpty());
    }

    @Test
    void testSelectWorkerExplicitPool() throws Exception {
        ComputeNode a = new ComputeNode(10L, "192.168.1.10", 9030);
        ComputeNode b = new ComputeNode(11L, "192.168.1.11", 9030);
        a.setAlive(true);
        b.setAlive(true);
        ComputeNode picked = LanceWorkerSelector.selectWorker(
                java.util.List.of(a, b), WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertTrue(picked.getId() == 10L || picked.getId() == 11L);
    }
}
