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

import com.google.common.collect.ImmutableList;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class HistoricalNodeMgr {
    private static final Logger LOG = LogManager.getLogger(HistoricalNodeMgr.class);
    private ConcurrentHashMap<String, HistoricalNodeSet> whToComputeNodeIds;

    public HistoricalNodeMgr() {
        whToComputeNodeIds = new ConcurrentHashMap<>();
    }

    public void updateHistoricalBackendIds(List<Long> backendIds, long currentTime, String warehouse) {
        HistoricalNodeSet nodeSet = whToComputeNodeIds.computeIfAbsent(warehouse, k -> new HistoricalNodeSet());
        nodeSet.updateHistoricalBackendIds(backendIds, currentTime);
    }

    public void updateHistoricalComputeNodeIds(List<Long> computeNodeIds, long currentTime, String warehouse) {
        HistoricalNodeSet nodeSet = whToComputeNodeIds.computeIfAbsent(warehouse, k -> new HistoricalNodeSet());
        nodeSet.updateHistoricalComputeNodeIds(computeNodeIds, currentTime);
    }

    public ImmutableList<Long> getHistoricalBackendIds(String warehouse) {
        HistoricalNodeSet nodeSet = whToComputeNodeIds.get(warehouse);
        if (nodeSet == null) {
            return ImmutableList.of();
        }
        return nodeSet.getHistoricalBackendIds();
    }

    public ImmutableList<Long> getHistoricalComputeNodeIds(String warehouse) {
        HistoricalNodeSet nodeSet = whToComputeNodeIds.get(warehouse);
        if (nodeSet == null) {
            return ImmutableList.of();
        }
        return nodeSet.getHistoricalComputeNodeIds();
    }

    public long getLastUpdateTime(String warehouse) {
        HistoricalNodeSet nodeSet = whToComputeNodeIds.get(warehouse);
        if (nodeSet == null) {
            return 0;
        }
        return nodeSet.getLastUpdateTime();
    }

    public ConcurrentHashMap<String, HistoricalNodeSet> getAllHistoricalNodeSet() {
        return whToComputeNodeIds;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Map<String, HistoricalNodeSet> serializedHistoricalNodes = whToComputeNodeIds.entrySet().stream()
                .filter(entry -> warehouseManager.warehouseExists(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        int numJson = 1 + serializedHistoricalNodes.size() * 2;
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.HISTORICAL_NODE_MGR, numJson);

        writer.writeInt(serializedHistoricalNodes.size());
        for (Map.Entry<String, HistoricalNodeSet> nodeSetEntry : serializedHistoricalNodes.entrySet()) {
            writer.writeString(nodeSetEntry.getKey());
            writer.writeJson(nodeSetEntry.getValue());
        }
        writer.close();
        LOG.info("save image for historical node manager, serializedHistoricalNodes: {}", serializedHistoricalNodes);
    }

    public void load(SRMetaBlockReader reader) throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        reader.readMap(String.class, HistoricalNodeSet.class, whToComputeNodeIds::put);
        LOG.info("load image for historical node manager, whToComputeNodeIds: {}", whToComputeNodeIds);
    }
}
