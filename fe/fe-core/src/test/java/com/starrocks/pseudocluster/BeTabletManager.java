// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.pseudocluster;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.UserException;
import com.starrocks.thrift.TCreateTabletReq;
import com.starrocks.thrift.TTablet;
import com.starrocks.thrift.TTabletInfo;
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
        Tablet tablet = new Tablet(request.tablet_id, request.table_id, request.partition_id,
                request.tablet_schema.getSchema_hash(), request.enable_persistent_index);
        tablets.put(request.tablet_id, tablet);
        tabletIdsByPartition.computeIfAbsent(tablet.partitionId, k -> Sets.newHashSet()).add(tablet.id);
        LOG.info("created tablet {}", tablet.id);
        return tablet;
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
        } else {
            LOG.warn("Drop Tablet {} not found", tabletId);
        }
    }

    public synchronized Tablet getTablet(long tabletId) {
        return tablets.get(tabletId);
    }

    public synchronized List<Tablet> getTablets(long partitionId) {
        Set<Long> tabletIds = tabletIdsByPartition.get(partitionId);
        if (tabletIds == null) {
            return Collections.emptyList();
        }
        return tabletIds.stream().map(tablets::get).filter(t -> t != null).collect(Collectors.toList());
    }

    void getTabletStat(TTabletStatResult result) {
        Map<Long, TTabletStat> statMap = Maps.newHashMap();
        for (Tablet tablet : tablets.values()) {
            statMap.put(tablet.id, tablet.getStats());
        }
        result.tablets_stats = statMap;
    }

    void getTabletInfo(TTabletInfo info) {

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
            tablet.versionGC();
        }
    }
}
