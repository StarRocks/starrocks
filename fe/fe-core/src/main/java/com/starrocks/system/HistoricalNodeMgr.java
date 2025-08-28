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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.Pair;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.MapEntryConsumer;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.ComputeResourceProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class HistoricalNodeMgr {
    private static final Logger LOG = LogManager.getLogger(HistoricalNodeMgr.class);
    // Use a string as the map key to be compatible with the old image data format.
    // This key consists of warehouseId and workerGroupId, such as: "warehouseId_workerGroupId".
    private ConcurrentHashMap<String, HistoricalNodeSet> computeResourceToNodeSet;

    public HistoricalNodeMgr() {
        computeResourceToNodeSet = new ConcurrentHashMap<>();
    }

    public void updateHistoricalBackendIds(ComputeResource computeResource, List<Long> backendIds, long currentTime) {
        String resourceKey = computeResourceToStr(computeResource);
        HistoricalNodeSet nodeSet = computeResourceToNodeSet.computeIfAbsent(resourceKey, k -> new HistoricalNodeSet());
        nodeSet.updateHistoricalBackendIds(backendIds, currentTime);
    }

    public void updateHistoricalComputeNodeIds(ComputeResource computeResource, List<Long> computeNodeIds, long currentTime) {
        String resourceKey = computeResourceToStr(computeResource);
        HistoricalNodeSet nodeSet = computeResourceToNodeSet.computeIfAbsent(resourceKey, k -> new HistoricalNodeSet());
        nodeSet.updateHistoricalComputeNodeIds(computeNodeIds, currentTime);
    }

    public ImmutableList<Long> getHistoricalBackendIds(ComputeResource computeResource) {
        String resourceKey = computeResourceToStr(computeResource);
        HistoricalNodeSet nodeSet = computeResourceToNodeSet.get(resourceKey);
        if (nodeSet == null) {
            return ImmutableList.of();
        }
        return nodeSet.getHistoricalBackendIds();
    }

    public ImmutableList<Long> getHistoricalComputeNodeIds(ComputeResource computeResource) {
        String resourceKey = computeResourceToStr(computeResource);
        HistoricalNodeSet nodeSet = computeResourceToNodeSet.get(resourceKey);
        if (nodeSet == null) {
            return ImmutableList.of();
        }
        return nodeSet.getHistoricalComputeNodeIds();
    }

    public long getLastUpdateTime(ComputeResource computeResource) {
        String resourceKey = computeResourceToStr(computeResource);
        HistoricalNodeSet nodeSet = computeResourceToNodeSet.get(resourceKey);
        if (nodeSet == null) {
            return 0;
        }
        return nodeSet.getLastUpdateTime();
    }

    public ConcurrentHashMap<ComputeResource, HistoricalNodeSet> getAllHistoricalNodeSet() {
        ConcurrentHashMap<ComputeResource, HistoricalNodeSet> allHistoricalNodeSet = new ConcurrentHashMap<>();
        for (Map.Entry<String, HistoricalNodeSet> entry : computeResourceToNodeSet.entrySet()) {
            ComputeResource computeResource = strToComputeResource(entry.getKey());
            HistoricalNodeSet nodeSet = entry.getValue();
            if (computeResource != null && nodeSet != null) {
                allHistoricalNodeSet.put(computeResource, nodeSet);
            }
        }
        return allHistoricalNodeSet;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        Map<String, HistoricalNodeSet> serializedHistoricalNodes = computeResourceToNodeSet.entrySet().stream()
                .filter(entry -> isResourceAvailable(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        int numJson = 1 + serializedHistoricalNodes.size() * 2;
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.HISTORICAL_NODE_MGR, numJson);

        writer.writeInt(serializedHistoricalNodes.size());
        for (Map.Entry<String, HistoricalNodeSet> nodeSetEntry : serializedHistoricalNodes.entrySet()) {
            writer.writeString(nodeSetEntry.getKey());
            writer.writeJson(nodeSetEntry.getValue());
        }
        writer.close();
        LOG.info("save image for historical node manager, nodeSetSize: {}", serializedHistoricalNodes.size());
    }

    public void load(SRMetaBlockReader reader) throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        reader.readMap(String.class, HistoricalNodeSet.class,
                (MapEntryConsumer<String, HistoricalNodeSet>) (resourceKey, nodeSet) -> {
                    // Check and convert the resource key.
                    // Mainly for compatibility with old image data, the early warehouse name will be converted into
                    // the new resource key format.
                    // Loading historical node metadata failed usually is not critical because it only affects the
                    // cache sharing performance in a short time, and will be corrected once the cluster nodes changed.
                    // So we ignore the exception when loading historical node metadata to avoid affecting the overall process.
                    try {
                        Pair<Long, Long> resourceId = strToComputeResourceId(resourceKey);
                        if (resourceId != null) {
                            computeResourceToNodeSet.put(computeResourceIdToStr(resourceId), nodeSet);
                        }
                    } catch (Exception e) {
                        LOG.error("fail to load image for historical node manager, resouceKey: {}", resourceKey);
                    }
                });
        LOG.info("load image for historical node manager, nodeSetSize: {}", computeResourceToNodeSet.size());
    }

    public void clear() {
        computeResourceToNodeSet.clear();
    }

    private String computeResourceToStr(ComputeResource computeResource) {
        return computeResource.getWarehouseId() + "-" + computeResource.getWorkerGroupId();
    }

    private ComputeResource strToComputeResource(String computeResourceKey) {
        ComputeResource computeResource = null;
        Pair<Long, Long> resourceId = strToComputeResourceId(computeResourceKey);
        if (resourceId != null) {
            final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            ComputeResourceProvider computeResourceProvider = warehouseManager.getComputeResourceProvider();
            computeResource = computeResourceProvider.ofComputeResource(resourceId.first, resourceId.second);
        }
        return computeResource;
    }

    private String computeResourceIdToStr(Pair<Long, Long> resouceId) {
        return resouceId.first + "-" + resouceId.second;
    }

    private Pair<Long, Long> strToComputeResourceId(String computeResourceKey) {
        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        String[] parts = computeResourceKey.split("-");
        Pair<Long, Long> resourceId = null;
        if (parts.length >= 2) {
            try {
                long warehouseId = Long.parseLong(parts[0]);
                long workerGroupId = Long.parseLong(parts[1]);
                resourceId = new Pair<>(warehouseId, workerGroupId);
            } catch (NumberFormatException e) {
                LOG.warn("fail to convert {} to valid compute resource id", computeResourceKey);
            }
        } else if (parts.length == 1) {
            // To be compatible with old image data, which only contains warehouse name in the resource key.
            Warehouse wh = warehouseManager.getWarehouseAllowNull(parts[0]);
            if (wh != null) {
                resourceId = new Pair<>(wh.getId(), StarOSAgent.DEFAULT_WORKER_GROUP_ID);
            }
        }
        return resourceId;
    }

    @VisibleForTesting
    public boolean isResourceAvailable(String computeResourceKey) {
        try {
            Pair<Long, Long> computeResourceId = strToComputeResourceId(computeResourceKey);
            if (computeResourceId == null) {
                return false;
            }
            final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            ComputeResourceProvider computeResourceProvider = warehouseManager.getComputeResourceProvider();
            ComputeResource computeResource = computeResourceProvider.ofComputeResource(computeResourceId.first,
                    computeResourceId.second);
            return computeResourceProvider.isResourceAvailable(computeResource);
        } catch (Exception e) {
            LOG.warn("fail to check compute resource {} availability", computeResourceKey);
            return false;
        }
    }
}
