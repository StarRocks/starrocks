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


package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.UserException;
import com.starrocks.proto.PublishLogVersionRequest;
import com.starrocks.proto.PublishLogVersionResponse;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import javax.validation.constraints.NotNull;

public class Utils {
    private static final Logger LOG = LogManager.getLogger(Utils.class);

    private Utils() {
    }

    // Returns null if no backend available.
    public static Long chooseBackend(LakeTablet tablet) {
        try {
            return tablet.getPrimaryBackendId();
        } catch (UserException ex) {
            LOG.info("Ignored error {}", ex.getMessage());
        }
        List<Long> backendIds = GlobalStateMgr.getCurrentSystemInfo().seqChooseBackendIds(1, true, false);
        if (CollectionUtils.isEmpty(backendIds)) {
            return null;
        }
        return backendIds.get(0);
    }

    // Preconditions: Has required the database's reader lock.
    // Returns a map from backend ID to a list of tablet IDs.
    public static Map<Long, List<Long>> groupTabletID(OlapTable table) throws NoAliveBackendException {
        return groupTabletID(table.getPartitions(), MaterializedIndex.IndexExtState.ALL);
    }

    public static Map<Long, List<Long>> groupTabletID(Collection<Partition> partitions,
                                                      MaterializedIndex.IndexExtState indexState)
            throws NoAliveBackendException {
        Map<Long, List<Long>> groupMap = new HashMap<>();
        for (Partition partition : partitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(indexState)) {
                for (Tablet tablet : index.getTablets()) {
                    Long beId = chooseBackend((LakeTablet) tablet);
                    if (beId == null) {
                        throw new NoAliveBackendException("no alive backend");
                    }
                    groupMap.computeIfAbsent(beId, k -> Lists.newArrayList()).add(tablet.getId());
                }
            }
        }
        return groupMap;
    }

    public static void publishVersion(@NotNull List<Tablet> tablets, long txnId, long baseVersion, long newVersion)
            throws NoAliveBackendException, RpcException {
        publishVersion(tablets, txnId, baseVersion, newVersion, null);
    }

    public static void publishVersion(@NotNull List<Tablet> tablets, long txnId, long baseVersion, long newVersion, Map<Long,
            Double> compactionScores)
            throws NoAliveBackendException, RpcException {
        Map<Long, List<Long>> beToTablets = new HashMap<>();
        for (Tablet tablet : tablets) {
            Long beId = Utils.chooseBackend((LakeTablet) tablet);
            if (beId == null) {
                throw new NoAliveBackendException("No alive backend for handle publish version request");
            }
            beToTablets.computeIfAbsent(beId, k -> Lists.newArrayList()).add(tablet.getId());
        }
        List<Long> txnIds = Lists.newArrayList(txnId);
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        List<Future<PublishVersionResponse>> responseList = Lists.newArrayListWithCapacity(beToTablets.size());
        List<Backend> backendList = Lists.newArrayListWithCapacity(beToTablets.size());
        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            Backend backend = systemInfoService.getBackend(entry.getKey());
            if (backend == null) {
                throw new NoAliveBackendException("Backend been dropped while building publish version request");
            }
            PublishVersionRequest request = new PublishVersionRequest();
            request.baseVersion = baseVersion;
            request.newVersion = newVersion;
            request.tabletIds = entry.getValue();
            request.txnIds = txnIds;

            LakeService lakeService = BrpcProxy.getLakeService(backend.getHost(), backend.getBrpcPort());
            Future<PublishVersionResponse> future = lakeService.publishVersion(request);
            responseList.add(future);
            backendList.add(backend);
        }

        for (int i = 0; i < responseList.size(); i++) {
            try {
                PublishVersionResponse response = responseList.get(i).get();
                if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                    throw new RpcException("Fail to publish version for tablets " + response.failedTablets);
                }
                if (compactionScores != null && response != null && response.compactionScores != null) {
                    compactionScores.putAll(response.compactionScores);
                }
            } catch (Exception e) {
                throw new RpcException(backendList.get(i).getHost(), e.getMessage());
            }
        }
    }

    public static void publishLogVersion(@NotNull List<Tablet> tablets, long txnId, long version)
            throws NoAliveBackendException, RpcException {
        Map<Long, List<Long>> beToTablets = new HashMap<>();
        for (Tablet tablet : tablets) {
            Long beId = Utils.chooseBackend((LakeTablet) tablet);
            if (beId == null) {
                throw new NoAliveBackendException("No alive backend for handle publish version request");
            }
            beToTablets.computeIfAbsent(beId, k -> Lists.newArrayList()).add(tablet.getId());
        }
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        List<Future<PublishLogVersionResponse>> responseList = Lists.newArrayListWithCapacity(beToTablets.size());
        List<Backend> backendList = Lists.newArrayListWithCapacity(beToTablets.size());
        for (Map.Entry<Long, List<Long>> entry : beToTablets.entrySet()) {
            Backend backend = systemInfoService.getBackend(entry.getKey());
            if (backend == null) {
                throw new NoAliveBackendException("Backend been dropped while building publish version request");
            }
            PublishLogVersionRequest request = new PublishLogVersionRequest();
            request.tabletIds = entry.getValue();
            request.txnId = txnId;
            request.version = version;

            LakeService lakeService = BrpcProxy.getLakeService(backend.getHost(), backend.getBrpcPort());
            Future<PublishLogVersionResponse> future = lakeService.publishLogVersion(request);
            responseList.add(future);
            backendList.add(backend);
        }

        for (int i = 0; i < responseList.size(); i++) {
            try {
                PublishLogVersionResponse response = responseList.get(i).get();
                if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                    throw new RpcException(backendList.get(i).getHost(),
                            "Fail to publish log version for tablets {}" + response.failedTablets);
                }
            } catch (Exception e) {
                throw new RpcException(backendList.get(i).getHost(), e.getMessage());
            }
        }
    }
}
