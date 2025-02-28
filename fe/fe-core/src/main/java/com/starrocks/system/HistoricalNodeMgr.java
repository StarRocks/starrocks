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

package com.starrocks.system;

import com.google.common.collect.ImmutableMap;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HistoricalNodeMgr {
    private static final Logger LOG = LogManager.getLogger(HistoricalNodeMgr.class);
    // TODO: Repace it with a session variable.
    // 1min
    private ConcurrentHashMap<String, HistoricalNodeSet> whToComputeNodeIds;
    //public static long kMinUpdateInterval = 60 * 1000;
    public HistoricalNodeMgr() {
        whToComputeNodeIds = new ConcurrentHashMap<>();
    }

    /*
    public void addHistoricalNodeSet(String warehouse) {
        HistoricalNodeSet nodeSet = new HistoricalNodeSet(kMinUpdateInterval);
        whToComputeNodeIds.put(warehouse, nodeSet);
        LOG.info("[Gavin] add historical node set for warehouse: {}", warehouse);
    }

    public void removeHistoricalNodeSet(String warehouse) {
        whToComputeNodeIds.remove(warehouse);
        LOG.info("[Gavin] remove historical node set for warehouse: {}", warehouse);
    }

    public boolean updateHistoricalNodeIds(List<Long> nodeIds, String warehouse) {
        HistoricalNodeSet nodeSet = whToComputeNodeIds.computeIfAbsent(warehouse, k -> new HistoricalNodeSet());
        int minUpdateInterval = ConnectContext.getSessionVariableOrDefault().getHistoricalNodesMinUpdateInterval();
        return nodeSet.setHistoricalNodeIds(nodeIds, minUpdateInterval * 1000);
    }

    public List<Long> getHistoricalNodeIds(String warehouse) {
        HistoricalNodeSet nodeSet = whToComputeNodeIds.get(warehouse);
        if (nodeSet == null) {
            LOG.error("fail to update historical nodes for warehouse[{}] because the warehouse is not exist",
                    warehouse);
            return null;
        }
        return nodeSet.getNodeIds();
    }
    */

    public boolean updateHistoricalBackends(Map<Long, Backend> idToBackend, String warehouse) {
        HistoricalNodeSet nodeSet = whToComputeNodeIds.computeIfAbsent(warehouse, k -> new HistoricalNodeSet());
        int minUpdateInterval = ConnectContext.getSessionVariableOrDefault().getHistoricalNodesMinUpdateInterval();
        return nodeSet.updateHistoricalBackends(idToBackend, minUpdateInterval * 1000);
    }

    public boolean updateHistoricalComputeNodes(Map<Long, ComputeNode> idToComputeNode, String warehouse) {
        HistoricalNodeSet nodeSet = whToComputeNodeIds.computeIfAbsent(warehouse, k -> new HistoricalNodeSet());
        int minUpdateInterval = ConnectContext.getSessionVariableOrDefault().getHistoricalNodesMinUpdateInterval();
        return nodeSet.updateHistoricalComputeNodes(idToComputeNode, minUpdateInterval * 1000);
    }

    public ImmutableMap<Long, Backend> getHistoricalBackends(String warehouse) {
        HistoricalNodeSet nodeSet = whToComputeNodeIds.get(warehouse);
        if (nodeSet == null) {
            LOG.error("fail to get historical backends for warehouse[{}] because the warehouse is not exist",
                    warehouse);
            return ImmutableMap.of();
        }
        return nodeSet.getHistoricalBackends();
    }

    public ImmutableMap<Long, ComputeNode> getHistoricalComputeNodes(String warehouse) {
        HistoricalNodeSet nodeSet = whToComputeNodeIds.get(warehouse);
        if (nodeSet == null) {
            LOG.error("fail to get historical compute nodes for warehouse[{}] because the warehouse is not exist",
                    warehouse);
            return ImmutableMap.of();
        }
        return nodeSet.getHistoricalComputeNodes();
    }

    public long getLastUpdateTime(String warehouse) {
        HistoricalNodeSet nodeSet = whToComputeNodeIds.get(warehouse);
        if (nodeSet == null) {
            LOG.error("fail to get last update time for warehouse[{}] because the warehouse is not exist",
                    warehouse);
            return 0;
        }
        return nodeSet.getLastUpdateTime();
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
    }

    public void load(SRMetaBlockReader reader) throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
    }
}
