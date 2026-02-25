// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.mv;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class MaterializedViewDependencyGraph {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewDependencyGraph.class);

    private MaterializedViewDependencyGraph() {
        super();
    }

    public static List<MaterializedView> buildTopologicalOrder(List<MaterializedView> allMVs) {
        // Collect all materialized views and process them in topological order,
        // ensuring that dependencies are handled correctly.
        Map<Long, MaterializedView> mvById = new HashMap<>();
        // mv -> its related mvs
        Map<MaterializedView, Set<Long>> dependencyGraph = new HashMap<>();
        // mv <- mvs which depend on this mv
        Map<Long, Set<Long>> reDependencyGraph = new HashMap<>();

        // First, gather all MVs across all databases.
        for (MaterializedView mv : allMVs) {
            long mvId = mv.getId();
            mvById.put(mvId, mv);

            // Build dependency graph: for each MV, collect its direct MV dependencies (by table id).
            Set<Long> deps = new HashSet<>();
            Set<MvId> relatedMVs = mv.getRelatedMaterializedViews();
            if (CollectionUtils.isNotEmpty(relatedMVs)) {
                for (MvId depMv : relatedMVs) {
                    long depId = depMv.getId();
                    deps.add(depId);
                    reDependencyGraph.computeIfAbsent(depId, k -> new HashSet<>()).add(mvId);
                }
            }
            dependencyGraph.put(mv, deps);
        }

        // Topological sort using Kahn's algorithm
        List<MaterializedView> topoOrder = new ArrayList<>();
        Queue<MaterializedView> ready = new ArrayDeque<>();
        Map<Long, Integer> inDegree = new HashMap<>();

        for (MaterializedView mv : allMVs) {
            int depCount = dependencyGraph.get(mv).size();
            inDegree.put(mv.getId(), depCount);
            if (depCount == 0) {
                ready.add(mv);
            }
        }

        while (!ready.isEmpty()) {
            MaterializedView mv = ready.poll();
            topoOrder.add(mv);
            // For all MVs that depend on mv, decrease their in-degree
            Set<Long> deps = reDependencyGraph.get(mv.getId());
            if (CollectionUtils.isNotEmpty(deps)) {
                for (Long depId : deps) {
                    MaterializedView other = mvById.get(depId);
                    if (other != null) {
                        inDegree.put(other.getId(), inDegree.get(other.getId()) - 1);
                        if (inDegree.get(other.getId()) == 0) {
                            ready.add(other);
                        }
                    }
                }
            }
        }

        // If topoOrder does not contain all MVs, there is a cycle
        if (topoOrder.size() != allMVs.size()) {
            LOG.warn("Cycle detected in Materialized View dependencies! Processing in arbitrary order. " +
                    "All MVs: {}, Topo Order: {}", allMVs.size(), topoOrder.size());
            // fallback: Use the original order
            topoOrder = allMVs;
        }
        return topoOrder;
    }
}
