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

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Orders materialized views so that every MV comes after the MVs it is built on.
 *
 * <p>Dependency edges come from each MV's persisted {@code baseTableInfos}: an MV depends on every other MV of the
 * input whose id appears as a base table id. This is a pure id comparison, no table is resolved, so external tables
 * (whose {@code tableId} is -1), plain tables, views and dropped tables simply produce no edge. The runtime
 * {@code relatedMaterializedViews} set is deliberately not used: it is empty right after image load and it points from
 * a base table to the MVs built on it, i.e. the opposite direction.
 *
 * <p>Callers must build the order before any MV is reloaded, because reloading an MV may rewrite its
 * {@code baseTableInfos}.
 */
public class MaterializedViewDependencyGraph {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewDependencyGraph.class);

    private MaterializedViewDependencyGraph() {
        super();
    }

    /**
     * Splits the MVs into topological layers: layer 0 holds the MVs without a predecessor in the input, and layer k+1
     * holds the MVs whose last predecessor is in layer k. Inside a layer, MVs keep their input order.
     *
     * <p>Predecessors of an MV are the ids in its {@code baseTableInfos} that belong to another MV of the input; ids
     * outside the input (dangling ids, non-MV tables) and the MV's own id are ignored. If the dependencies contain a
     * cycle, a single layer holding all MVs in input order is returned.
     *
     * @param allMVs the MVs to order, across all databases
     * @return the layers in dependency order; empty if the input is empty
     */
    public static List<List<MaterializedView>> buildTopologicalLayers(List<MaterializedView> allMVs) {
        List<List<MaterializedView>> layers = new ArrayList<>();
        if (CollectionUtils.isEmpty(allMVs)) {
            return layers;
        }

        int mvCount = allMVs.size();
        Map<Long, Integer> indexById = new HashMap<>();
        for (int i = 0; i < mvCount; i++) {
            indexById.put(allMVs.get(i).getId(), i);
        }

        // successors.get(p) = indexes of the MVs that directly depend on the MV at index p
        List<List<Integer>> successors = new ArrayList<>(mvCount);
        for (int i = 0; i < mvCount; i++) {
            successors.add(new ArrayList<>());
        }
        int[] inDegree = new int[mvCount];
        for (int i = 0; i < mvCount; i++) {
            MaterializedView mv = allMVs.get(i);
            List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
            if (CollectionUtils.isEmpty(baseTableInfos)) {
                continue;
            }
            Set<Integer> predecessors = new LinkedHashSet<>();
            for (BaseTableInfo baseTableInfo : baseTableInfos) {
                if (baseTableInfo == null || baseTableInfo.getTableId() == mv.getId()) {
                    continue;
                }
                Integer predecessor = indexById.get(baseTableInfo.getTableId());
                if (predecessor != null) {
                    predecessors.add(predecessor);
                }
            }
            for (int predecessor : predecessors) {
                successors.get(predecessor).add(i);
            }
            inDegree[i] = predecessors.size();
        }

        // Kahn's algorithm, dequeuing the whole ready set at once as one layer.
        List<Integer> current = new ArrayList<>();
        for (int i = 0; i < mvCount; i++) {
            if (inDegree[i] == 0) {
                current.add(i);
            }
        }
        int placed = 0;
        while (!current.isEmpty()) {
            List<MaterializedView> layer = new ArrayList<>(current.size());
            List<Integer> next = new ArrayList<>();
            for (int index : current) {
                layer.add(allMVs.get(index));
                for (int successor : successors.get(index)) {
                    inDegree[successor]--;
                    if (inDegree[successor] == 0) {
                        next.add(successor);
                    }
                }
            }
            layers.add(layer);
            placed += layer.size();
            // keep the input order inside a layer
            Collections.sort(next);
            current = next;
        }

        // If not every MV was placed, there is a cycle
        if (placed != mvCount) {
            LOG.warn("Cycle detected in Materialized View dependencies! Processing in arbitrary order. " +
                    "All MVs: {}, Topo Order: {}", mvCount, placed);
            // fallback: a single layer in the original order
            List<List<MaterializedView>> fallback = new ArrayList<>();
            fallback.add(new ArrayList<>(allMVs));
            return fallback;
        }
        return layers;
    }

    /**
     * Returns the MVs in topological order, i.e. the layers of {@link #buildTopologicalLayers(List)} flattened.
     */
    public static List<MaterializedView> buildTopologicalOrder(List<MaterializedView> allMVs) {
        List<MaterializedView> topoOrder = new ArrayList<>();
        for (List<MaterializedView> layer : buildTopologicalLayers(allMVs)) {
            topoOrder.addAll(layer);
        }
        return topoOrder;
    }
}
