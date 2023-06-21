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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/TabletStatMgr.java

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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.proto.TabletStatRequest;
import com.starrocks.proto.TabletStatRequest.TabletInfo;
import com.starrocks.proto.TabletStatResponse;
import com.starrocks.proto.TabletStatResponse.TabletStat;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TTabletStat;
import com.starrocks.thrift.TTabletStatResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/*
 * TabletStatMgr is for collecting tablet(replica) statistics from backends.
 * Each FE will collect by itself.
 */
public class TabletStatMgr extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletStatMgr.class);

    // for lake table
    private final Map<Long, Long> partitionToUpdatedVersion;

    private LocalDateTime lastWorkTimestamp = LocalDateTime.MIN;

    public TabletStatMgr() {
        super("tablet stat mgr", Config.tablet_stat_update_interval_second * 1000L);
        partitionToUpdatedVersion = Maps.newHashMap();
    }

    public LocalDateTime getLastWorkTimestamp() {
        return lastWorkTimestamp;
    }

    @Override
    protected void runAfterCatalogReady() {
        updateLocalTabletStat();
        updateLakeTabletStat();

        // after update replica in all backends, update index row num
        long start = System.currentTimeMillis();
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                continue;
            }
            db.writeLock();
            try {
                for (Table table : db.getTables()) {
                    if (!table.isNativeTableOrMaterializedView()) {
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) table;
                    for (Partition partition : olapTable.getAllPartitions()) {
                        long version = partition.getVisibleVersion();
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            long indexRowCount = 0L;
                            for (Tablet tablet : index.getTablets()) {
                                indexRowCount += tablet.getRowCount(version);
                            } // end for tablets
                            index.setRowCount(indexRowCount);
                        } // end for indices
                    } // end for partitions
                    LOG.debug("finished to set row num for table: {} in database: {}",
                            table.getName(), db.getFullName());
                }
            } finally {
                db.writeUnlock();
            }
        }
        LOG.info("finished to update index row num of all databases. cost: {} ms",
                (System.currentTimeMillis() - start));
        lastWorkTimestamp = LocalDateTime.now();
    }

    private void updateLocalTabletStat() {
        ImmutableMap<Long, Backend> backends = GlobalStateMgr.getCurrentSystemInfo().getIdToBackend();

        long start = System.currentTimeMillis();
        for (Backend backend : backends.values()) {
            BackendService.Client client = null;
            TNetworkAddress address = null;
            boolean ok = false;
            try {
                address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                client = ClientPool.backendPool.borrowObject(address);
                TTabletStatResult result = client.get_tablet_stat();

                LOG.debug("get tablet stat from backend: {}, num: {}", backend.getId(), result.getTablets_statsSize());
                updateLocalTabletStat(backend.getId(), result);

                ok = true;
            } catch (Exception e) {
                LOG.warn("task exec error. backend[{}]", backend.getId(), e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        }
        LOG.info("finished to get local tablet stat of all backends. cost: {} ms",
                (System.currentTimeMillis() - start));
    }

    private void updateLocalTabletStat(Long beId, TTabletStatResult result) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (Map.Entry<Long, TTabletStat> entry : result.getTablets_stats().entrySet()) {
            if (invertedIndex.getTabletMeta(entry.getKey()) == null) {
                // the replica is obsolete, ignore it.
                continue;
            }

            // Currently, only local table will update replica.
            Replica replica = invertedIndex.getReplica(entry.getKey(), beId);
            if (replica == null) {
                // replica may be deleted from catalog, ignore it.
                continue;
            }
            // TODO(cmy) no db lock protected. I think it is ok even we get wrong row num
            replica.updateStat(
                    entry.getValue().getData_size(),
                    entry.getValue().getRow_num(),
                    entry.getValue().getVersion_count()
            );
        }
    }

    private void updateLakeTabletStat() {
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                continue;
            }

            List<OlapTable> tables = Lists.newArrayList();
            db.readLock();
            try {
                for (Table table : db.getTables()) {
                    if (table.isCloudNativeTableOrMaterializedView()) {
                        tables.add((OlapTable) table);
                    }
                }
            } finally {
                db.readUnlock();
            }

            for (OlapTable table : tables) {
                updateLakeTableTabletStat(db, table);
            }
        }
    }

    @java.lang.SuppressWarnings("squid:S2142")  // allow catch InterruptedException
    private void updateLakeTableTabletStat(Database db, OlapTable table) {
        // prepare tablet infos
        Map<Long, List<TabletInfo>> beToTabletInfos = Maps.newHashMap();
        Map<Long, Long> partitionToVersion = Maps.newHashMap();
        db.readLock();
        try {
            for (Partition partition : table.getPartitions()) {
                long partitionId = partition.getId();
                long version = partition.getVisibleVersion();
                // partition init version is 1
                if (version <= partitionToUpdatedVersion.getOrDefault(partitionId, 1L)) {
                    continue;
                }

                partitionToVersion.put(partitionId, version);
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    for (Tablet tablet : index.getTablets()) {
                        Long beId = Utils.chooseBackend((LakeTablet) tablet);
                        if (beId == null) {
                            continue;
                        }
                        TabletInfo tabletInfo = new TabletInfo();
                        tabletInfo.tabletId = tablet.getId();
                        tabletInfo.version = version;
                        beToTabletInfos.computeIfAbsent(beId, k -> Lists.newArrayList()).add(tabletInfo);
                    }
                }
            }
        } finally {
            db.readUnlock();
        }

        if (beToTabletInfos.isEmpty()) {
            return;
        }

        // get tablet stats from be
        List<Future<TabletStatResponse>> responseList = Lists.newArrayListWithCapacity(beToTabletInfos.size());
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        Map<Long, TabletStat> idToStat = Maps.newHashMap();
        long start = System.currentTimeMillis();
        try {
            for (Map.Entry<Long, List<TabletInfo>> entry : beToTabletInfos.entrySet()) {
                Backend backend = systemInfoService.getBackend(entry.getKey());
                if (backend == null) {
                    continue;
                }
                TabletStatRequest request = new TabletStatRequest();
                request.tabletInfos = entry.getValue();

                LakeService lakeService = BrpcProxy.getLakeService(backend.getHost(), backend.getBrpcPort());
                Future<TabletStatResponse> responseFuture = lakeService.getTabletStats(request);
                responseList.add(responseFuture);
            }

            for (Future<TabletStatResponse> responseFuture : responseList) {
                TabletStatResponse response = responseFuture.get();
                if (response != null && response.tabletStats != null) {
                    for (TabletStat tabletStat : response.tabletStats) {
                        idToStat.put(tabletStat.tabletId, tabletStat);
                    }
                }
            }
        } catch (Throwable e) {
            LOG.warn("failed to get lake tablet stats. table id: {}", table.getId(), e);
            return;
        }
        LOG.info("finished to get lake tablet stats. db id: {}, table id: {}, cost: {} ms", db.getId(), table.getId(),
                (System.currentTimeMillis() - start));

        if (idToStat.isEmpty()) {
            return;
        }

        // update tablet stats
        db.writeLock();
        try {
            for (Partition partition : table.getPartitions()) {
                long partitionId = partition.getId();
                if (!partitionToVersion.containsKey(partitionId)) {
                    continue;
                }

                boolean allTabletsUpdated = true;
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    for (Tablet tablet : index.getTablets()) {
                        TabletStat stat = idToStat.get(tablet.getId());
                        if (stat == null) {
                            allTabletsUpdated = false;
                            continue;
                        }

                        LakeTablet lakeTablet = (LakeTablet) tablet;
                        lakeTablet.setRowCount(stat.numRows);
                        lakeTablet.setDataSize(stat.dataSize);
                        LOG.debug("update lake tablet info. tablet id: {}, num rows: {}, data size: {}", stat.tabletId,
                                stat.numRows, stat.dataSize);
                    }
                }
                if (allTabletsUpdated) {
                    long version = partitionToVersion.get(partitionId);
                    partitionToUpdatedVersion.put(partitionId, version);
                    LOG.info("update lake tablet stats. db id: {}, table id: {}, partition id: {}, version: {}",
                            db.getId(), table.getId(), partitionId, version);
                }
            }
        } finally {
            db.writeUnlock();
        }
    }
}
