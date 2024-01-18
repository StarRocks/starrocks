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

package com.starrocks.pseudocluster;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.exception.AlreadyExistsException;
import com.starrocks.common.exception.UserException;
import com.starrocks.thrift.TCreateTabletReq;
import com.starrocks.thrift.TTablet;
import com.starrocks.thrift.TTabletStat;
import com.starrocks.thrift.TTabletStatResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class BeTabletManager {
    private static final Logger LOG = LogManager.getLogger(BeTabletManager.class);
    PseudoBackend backend;
    Map<Long, Tablet> tablets;
    Map<Long, Set<Long>> tabletIdsByPartition;

    public BeTabletManager(PseudoBackend backend) {
        this.backend = backend;
        tablets = Maps.newHashMap();
        tabletIdsByPartition = Maps.newHashMap();
    }

    public synchronized Tablet createTablet(TCreateTabletReq request) throws UserException {
        if (tablets.get(request.tablet_id) != null) {
            throw new AlreadyExistsException("Tablet already exists");
        }
        boolean isSchemaChange = false;
        if (request.base_tablet_id > 0) {
            Tablet baseTablet = getTablet(request.base_tablet_id);
            if (baseTablet == null) {
                throw new UserException("Base tablet not found");
            }
            isSchemaChange = true;
        }
        Tablet tablet = new Tablet(request.tablet_id, request.table_id, request.partition_id,
                request.tablet_schema.getSchema_hash(), request.enable_persistent_index);
        tablet.setRunning(!isSchemaChange);
        tablets.put(request.tablet_id, tablet);
        tabletIdsByPartition.computeIfAbsent(tablet.partitionId, k -> Sets.newHashSet()).add(tablet.id);
        LOG.info("created tablet {} {}", tablet.id, isSchemaChange ? "base tablet: " + request.base_tablet_id : "");
        return tablet;
    }

    public synchronized void addClonedTablet(Tablet tablet) {
        tablets.put(tablet.id, tablet);
        tabletIdsByPartition.computeIfAbsent(tablet.partitionId, k -> Sets.newHashSet()).add(tablet.id);
    }

    public synchronized void dropTablet(long tabletId, boolean force) {
        Tablet removed = tablets.remove(tabletId);
        if (removed != null) {
            Set<Long> tabletIds = tabletIdsByPartition.get(removed.partitionId);
            tabletIds.remove(tabletId);
            if (tabletIds.isEmpty()) {
                tabletIdsByPartition.remove(removed.partitionId);
            }
            LOG.info("Dropped tablet {} force:{}", removed.id, force);
            // TODO: if tablet trash feature is simulated, we should consider not updating disk usage.
            if (PseudoBackend.getCurrentBackend() != null) {
                PseudoBackend.getCurrentBackend().updateDiskUsage(
                        0 - removed.numRowsets() * PseudoBackend.DEFAULT_SIZE_ON_DISK_PER_ROWSET_B);
            }
        } else {
            LOG.warn("Drop Tablet {} not found", tabletId);
        }
    }

    public synchronized Tablet getTablet(long tabletId) {
        return tablets.get(tabletId);
    }

    public synchronized List<Tablet> getTabletsByTable(long tableId) {
        return tablets.values().stream().filter(t -> t.tableId == tableId).collect(Collectors.toList());
    }

    public synchronized List<Tablet> getTablets(long partitionId) {
        Set<Long> tabletIds = tabletIdsByPartition.get(partitionId);
        if (tabletIds == null) {
            return Collections.emptyList();
        }
        return tabletIds.stream().map(tablets::get).filter(t -> t != null).collect(Collectors.toList());
    }

    public synchronized int getNumTablet() {
        return tablets.size();
    }

    void getTabletStat(TTabletStatResult result) {
        Map<Long, TTabletStat> statMap = Maps.newHashMap();
        for (Tablet tablet : tablets.values()) {
            statMap.put(tablet.id, tablet.getStats());
        }
        result.tablets_stats = statMap;
    }

    public synchronized Map<Long, TTablet> getAllTabletInfo() {
        Map<Long, TTablet> tabletInfo = Maps.newHashMap();
        for (Tablet tablet : tablets.values()) {
            TTablet tTablet = new TTablet();
            tTablet.addToTablet_infos(tablet.toThrift());
            tabletInfo.put(tablet.id, tTablet);
        }
        return tabletInfo;
    }

    public synchronized void maintenance() {
        for (Tablet tablet : tablets.values()) {
            tablet.doCompaction();
            tablet.versionGC();
        }
    }
}
