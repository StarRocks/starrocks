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
package com.starrocks.meta;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TabletInvertedIndexV2 extends TabletInvertedIndex {
    public void readLock() {
    }

    public void readUnlock() {
    }

    public void writeLock() {
    }

    public void writeUnlock() {
    }

    // tablet id -> tablet meta
    // tabletMetaMap/tabletId -> tabletMeta

    // backendId id -> replica list
    // backingReplicaMetaTable/backendId/tabletId -> replicaId

    // tablet id -> replica list
    // replicaMetaTable/tabletId/replicaId -> Replica

    // replica id -> tablet id
    // replicaToTabletMap/replicaId -> tabletId

    @Override
    public Long getTabletIdByReplica(long replicaId) {
        byte[] key = ByteCoder.encode(Lists.newArrayList("replicaToTabletMap", String.valueOf(replicaId)));
        return MetadataHandler.getInstance().get(null, key, Long.class);
    }

    @Override
    public TabletMeta getTabletMeta(long tabletId) {
        byte[] key = ByteCoder.encode(Lists.newArrayList("tabletMetaMap", String.valueOf(tabletId)));
        String tabletMetaJson = MetadataHandler.getInstance().get(null, key, String.class);
        return GsonUtils.GSON.fromJson(tabletMetaJson, TabletMeta.class);
    }

    @Override
    public List<TabletMeta> getTabletMetaList(List<Long> tabletIdList) {
        List<TabletMeta> tabletMetaList = new ArrayList<>(tabletIdList.size());
        for (Long tabletId : tabletIdList) {
            TabletMeta tabletMeta = getTabletMeta(tabletId);
            tabletMetaList.add(Objects.requireNonNullElse(tabletMeta, NOT_EXIST_TABLET_META));
        }
        return tabletMetaList;
    }

    @Override
    public Map<Long, Replica> getReplicaMetaWithBackend(Long backendId) {
        byte[] key = ByteCoder.encode(Lists.newArrayList("backingReplicaMetaTable", String.valueOf(backendId)));
        List<byte[]> replicaList = MetadataHandler.getInstance().getPrefix(null, key);

        Map<Long, Replica> replicaMap = new HashMap<>();
        for (byte[] bytes : replicaList) {
            List<String> values = ByteCoder.decode(bytes);
            Long tabletId = Long.parseLong(values.get(3));
            Long replicaId = Long.parseLong(values.get(4));
            TabletMeta tabletMeta = getTabletMeta(tabletId);

            Replica replica = getReplicaByTabletMeta(tabletMeta, replicaId);
            replicaMap.put(tabletId, replica);
        }

        return replicaMap;
    }

    @Override
    public void addTablet(long tabletId, TabletMeta tabletMeta) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }

        byte[] key = ByteCoder.encode(Lists.newArrayList("tabletMetaMap", String.valueOf(tabletId)));
        MetadataHandler.getInstance().put(null, new String(key),
                GsonUtils.GSON.toJson(tabletMeta, TabletMeta.class));
    }


    @Override
    public void deleteTablet(long tabletId) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }

        TabletMeta tabletMeta = getTabletMeta(tabletId);
        List<Replica> replicas = getReplicaByTabletMeta(tabletMeta);
        for (Replica replica : replicas) {
            //TODO
        }

        byte[] k1 = ByteCoder.encode(Lists.newArrayList("tabletMetaMap", String.valueOf(tabletId)));
        MetadataHandler.getInstance().delete(null, k1);
    }

    @Override
    public void addReplica(long tabletId, Replica replica) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }

        byte[] k1 = ByteCoder.encode(Lists.newArrayList("backingReplicaMetaTable",
                String.valueOf(replica.getBackendId()), String.valueOf(tabletId)));
        MetadataHandler.getInstance().put(null, new String(k1), String.valueOf(replica.getId()));

        byte[] k3 = ByteCoder.encode(Lists.newArrayList("replicaToTabletMap", String.valueOf(replica.getId())));
        MetadataHandler.getInstance().put(null, new String(k3), String.valueOf(tabletId));
    }

    @Override
    public void deleteReplica(long tabletId, long backendId) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }

        Long replica = getReplicaId(tabletId, backendId);

        byte[] k1 = ByteCoder.encode(Lists.newArrayList("backingReplicaMetaTable",
                String.valueOf(backendId), String.valueOf(tabletId), String.valueOf(replica)));
        MetadataHandler.getInstance().delete(null, k1);

        byte[] k3 = ByteCoder.encode(Lists.newArrayList("replicaToTabletMap",
                String.valueOf(replica), String.valueOf(tabletId)));
        MetadataHandler.getInstance().delete(null, k3);
    }

    @Override
    public Replica getReplica(long tabletId, long backendId) {
        byte[] key = ByteCoder.encode(Lists.newArrayList("backingReplicaMetaTable",
                String.valueOf(backendId),
                String.valueOf(tabletId)));
        List<byte[]> replicaList = MetadataHandler.getInstance().getPrefix(null, key);

        for (byte[] bytes : replicaList) {
            List<String> values = ByteCoder.decode(bytes);
            Long replicaId = Long.parseLong(values.get(4));
            TabletMeta tabletMeta = getTabletMeta(tabletId);

            Replica replica = getReplicaByTabletMeta(tabletMeta, replicaId);
            return replica;
        }

        return null;
    }

    public Long getReplicaId(long tabletId, long backendId) {
        byte[] key = ByteCoder.encode(Lists.newArrayList("backingReplicaMetaTable",
                String.valueOf(backendId),
                String.valueOf(tabletId)));
        List<byte[]> replicaList = MetadataHandler.getInstance().getPrefix(null, key);

        for (byte[] bytes : replicaList) {
            List<String> values = ByteCoder.decode(bytes);
            Long replicaId = Long.parseLong(values.get(4));
            return replicaId;
        }

        return null;
    }

    private Replica getReplicaByTabletMeta(TabletMeta tabletMeta, long replicaId) {
        return null;
    }

    private List<Replica> getReplicaByTabletMeta(TabletMeta tabletMeta) {
        return null;
    }

    @Override
    public List<Replica> getReplicasByTabletId(long tabletId) {
        TabletMeta tabletMeta = getTabletMeta(tabletId);
        return getReplicaByTabletMeta(tabletMeta);
    }

    @Override
    public List<Replica> getReplicasOnBackendByTabletIds(List<Long> tabletIds, long backendId) {

        List<Replica> replicas = new ArrayList<>();
        for (Long tabletId : tabletIds) {
            Replica replica = getReplica(tabletId,  backendId);
            replicas.add(replica);
        }

        return replicas;
    }

    @Override
    public List<Long> getTabletIdsByBackendId(long backendId) {
        byte[] key = ByteCoder.encode(Lists.newArrayList("backingReplicaMetaTable", String.valueOf(backendId)));
        List<byte[]> bytesList = MetadataHandler.getInstance().getPrefix(null, key);

        Set<Long> tabletIds = new HashSet<>();
        for (byte[] bytes : bytesList) {
            List<String> values = ByteCoder.decode(bytes);
            Long tabletId = Long.valueOf(values.get(3));
            tabletIds.add(tabletId);
        }

        return new ArrayList<>(tabletIds);
    }

    @Override
    public List<Long> getTabletIdsByBackendIdAndStorageMedium(long backendId, TStorageMedium storageMedium) {
        List<Long> tabletIds = getTabletIdsByBackendId(backendId);

        List<Long> t = new ArrayList<>();
        for (Long tabletId : tabletIds) {
            TabletMeta tabletMeta = getTabletMeta(tabletId);
            if (tabletMeta.getStorageMedium() == storageMedium) {
                t.add(tabletId);
            }
        }

        return t;
    }

    @Override
    public long getTabletNumByBackendId(long backendId) {
        return getTabletIdsByBackendId(backendId).size();
    }

    @Override
    public long getTabletNumByBackendIdAndPathHash(long backendId, long pathHash) {
        Collection<Replica> replicas = getReplicaMetaWithBackend(backendId).values();
        int count = 0;
        for (Replica replica : replicas) {
            if (replica.getPathHash() == pathHash) {
                count++;
            }
        }

        return count;
    }

    @Override
    public Map<TStorageMedium, Long> getReplicaNumByBeIdAndStorageMedium(long backendId) {
        Map<TStorageMedium, Long> replicaNumMap = Maps.newHashMap();
        long hddNum = 0;
        long ssdNum = 0;

        List<Long> tabletIds = getTabletIdsByBackendId(backendId);
        for (Long tabletId : tabletIds) {
            TabletMeta tabletMeta = getTabletMeta(tabletId);
            if (tabletMeta.getStorageMedium() == TStorageMedium.HDD) {
                hddNum++;
            } else {
                ssdNum++;
            }
        }

        replicaNumMap.put(TStorageMedium.HDD, hddNum);
        replicaNumMap.put(TStorageMedium.SSD, ssdNum);
        return replicaNumMap;
    }
}
