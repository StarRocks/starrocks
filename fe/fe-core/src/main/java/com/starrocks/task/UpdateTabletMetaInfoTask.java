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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/UpdateTabletMetaInfoTask.java

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

package com.starrocks.task;

import com.google.common.collect.Lists;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletMetaInfo;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TUpdateTabletMetaInfoReq;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public abstract class UpdateTabletMetaInfoTask extends AgentTask {

    private static final Logger LOG = LogManager.getLogger(UpdateTabletMetaInfoTask.class);

    private MarkedCountDownLatch<Long, Set<Long>> latch;

    private long txnId;

    private UpdateTabletMetaInfoTask(long backendId, long signature) {
        super(null, backendId, TTaskType.UPDATE_TABLET_META_INFO, -1L, -1L, -1L, -1L, -1L, signature);
    }

    public static UpdateTabletMetaInfoTask updateBooleanProperty(long backendId, Set<Long> tablets,
                                                                 Boolean value,
                                                                 TTabletMetaType metaType) {
        if (metaType == TTabletMetaType.INMEMORY) {
            return updateIsInMemory(backendId, tablets, value);
        }
        if (metaType == TTabletMetaType.ENABLE_PERSISTENT_INDEX) {
            return updateEnablePersistentIndex(backendId, tablets, value);
        }
        return null;
    }

    public static UpdateTabletMetaInfoTask updatePartitionId(long backendId, Set<Long> tablets) {
        return new UpdatePartitionIdTask(backendId, requireNonNull(tablets, "tablets is null"));
    }

    public static UpdateTabletMetaInfoTask updateIsInMemory(long backendId, Set<Long> tablets, Boolean value) {
        requireNonNull(tablets, "tablets is null");
        List<Pair<Long, Boolean>> valueList =
                tablets.stream().map(id -> new Pair<>(id, value)).collect(Collectors.toList());
        return updateIsInMemory(backendId, valueList);
    }

    public static UpdateTabletMetaInfoTask updateIsInMemory(long backendId, List<Pair<Long, Boolean>> inMemoryConfigs) {
        return new UpdateIsInMemoryTask(backendId, inMemoryConfigs);
    }

    public static UpdateTabletMetaInfoTask updateEnablePersistentIndex(long backend, Set<Long> tablets,
                                                                       Boolean value) {
        requireNonNull(tablets, "tablets is null");
        List<Pair<Long, Boolean>> valueList =
                tablets.stream().map(id -> new Pair<>(id, value)).collect(Collectors.toList());
        return new UpdateEnablePersistentIndexTask(backend, valueList);
    }

    public static UpdateTabletMetaInfoTask updateEnablePersistentIndex(long backend,
                                                                       List<Pair<Long, Boolean>> valueList) {
        return new UpdateEnablePersistentIndexTask(backend, requireNonNull(valueList, "valueList is null"));
    }

    public static UpdateTabletMetaInfoTask updateBinlogConfig(long backendId,
                                                              Set<Long> tablets,
                                                              BinlogConfig binlogConfig) {
        requireNonNull(tablets, "tablets is null");
        requireNonNull(binlogConfig, "binlogConfig is null");
        List<Pair<Long, BinlogConfig>> configList = tablets.stream().map(id -> new Pair<>(id, binlogConfig)).collect(
                Collectors.toList());
        return updateBinlogConfig(backendId, configList);
    }

    public static UpdateTabletMetaInfoTask updateBinlogConfig(long backendId,
                                                              List<Pair<Long, BinlogConfig>> binlogConfigs) {
        return new UpdateBinlogConfigTask(backendId, requireNonNull(binlogConfigs, "binlogConfigs is null"));
    }

    public static UpdateTabletMetaInfoTask updatePrimaryIndexCacheExpireTime(long backendId,
                                                                             Set<Long> tablets,
                                                                             Integer expireTime) {
        requireNonNull(tablets, "tablets is null");
        List<Pair<Long, Integer>> expireTimeList = tablets.stream().map(id -> new Pair<>(id, expireTime)).collect(
                Collectors.toList());
        return updatePrimaryIndexCacheExpireTime(backendId, expireTimeList);
    }

    public static UpdateTabletMetaInfoTask updatePrimaryIndexCacheExpireTime(long backendId,
                                                                             List<Pair<Long, Integer>> expireTimes) {
        return new UpdatePrimaryIndexCacheExpireTimeTask(backendId, requireNonNull(expireTimes, "expireTimes is null"));
    }

    public void setLatch(MarkedCountDownLatch<Long, Set<Long>> latch) {
        this.latch = requireNonNull(latch, "latch is null");
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public void countDownLatch(long backendId, Set<Long> tablets) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, tablets)) {
                LOG.debug("UpdateTabletMetaInfoTask current latch count: {}, backend: {}, tablets:{}", latch.getCount(),
                        backendId, tablets);
            }
        }
    }

    // call this always means one of tasks is failed. count down to zero to finish entire task
    public void countDownToZero(String errMsg) {
        if (this.latch != null) {
            latch.countDownToZero(new Status(TStatusCode.CANCELLED, errMsg));
            LOG.debug("UpdateTabletMetaInfoTask count down to zero. error msg: {}", errMsg);
        }
    }

    public abstract Set<Long> getTablets();

    public abstract List<TTabletMetaInfo> getTTabletMetaInfoList();

    public TUpdateTabletMetaInfoReq toThrift() {
        TUpdateTabletMetaInfoReq updateTabletMetaInfoReq = new TUpdateTabletMetaInfoReq();
        List<TTabletMetaInfo> metaInfos = getTTabletMetaInfoList();
        updateTabletMetaInfoReq.setTabletMetaInfos(metaInfos);
        if (RunMode.isSharedDataMode()) {
            updateTabletMetaInfoReq.setTablet_type(TTabletType.TABLET_TYPE_LAKE);
        }
        updateTabletMetaInfoReq.setTxn_id(txnId);
        return updateTabletMetaInfoReq;
    }

    private static class UpdatePartitionIdTask extends UpdateTabletMetaInfoTask {
        private final Set<Long> tabletSet;

        private UpdatePartitionIdTask(long backendId, Set<Long> tabletSet) {
            super(backendId, tabletSet.hashCode());
            this.tabletSet = requireNonNull(tabletSet, "tabletSet is null");
        }

        @Override
        public Set<Long> getTablets() {
            return tabletSet;
        }

        @Override
        public List<TTabletMetaInfo> getTTabletMetaInfoList() {
            TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
            List<TTabletMetaInfo> metaInfos = Lists.newArrayList();
            for (Long tabletId : tabletSet) {
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                if (tabletMeta == null) {
                    LOG.warn("could not find tablet [{}] in meta ignore it", tabletId);
                    continue;
                }
                TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                metaInfo.setTablet_id(tabletId);
                metaInfo.setPartition_id(tabletMeta.getPartitionId());
                metaInfo.setMeta_type(TTabletMetaType.PARTITIONID);
                metaInfos.add(metaInfo);
                // add at most 10000 tablet meta during one sync to avoid too large task
                if (metaInfos.size() > 10000) {
                    break;
                }
            }
            return metaInfos;
        }
    }

    private static class UpdateIsInMemoryTask extends UpdateTabletMetaInfoTask {
        private final List<Pair<Long, Boolean>> isInMemoryList;

        private UpdateIsInMemoryTask(long backendId, List<Pair<Long, Boolean>> isInMemoryList) {
            super(backendId, isInMemoryList.hashCode());
            this.isInMemoryList = isInMemoryList;
        }

        @Override
        public Set<Long> getTablets() {
            return isInMemoryList.stream().map(p -> p.first).collect(Collectors.toSet());
        }

        @Override
        public List<TTabletMetaInfo> getTTabletMetaInfoList() {
            List<TTabletMetaInfo> metaInfos = Lists.newArrayList();
            for (Pair<Long, Boolean> pair : isInMemoryList) {
                TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                metaInfo.setTablet_id(pair.first);
                metaInfo.setIs_in_memory(pair.second);
                metaInfo.setMeta_type(TTabletMetaType.INMEMORY);
                metaInfos.add(metaInfo);
            }
            return metaInfos;
        }
    }

    private static class UpdateEnablePersistentIndexTask extends UpdateTabletMetaInfoTask {
        private final List<Pair<Long, Boolean>> enablePersistentIndexList;

        private UpdateEnablePersistentIndexTask(long backendId, List<Pair<Long, Boolean>> enablePersistentIndexList) {
            super(backendId, enablePersistentIndexList.hashCode());
            this.enablePersistentIndexList = enablePersistentIndexList;
        }

        @Override
        public Set<Long> getTablets() {
            return enablePersistentIndexList.stream().map(p -> p.first).collect(Collectors.toSet());
        }

        @Override
        public List<TTabletMetaInfo> getTTabletMetaInfoList() {
            List<TTabletMetaInfo> metaInfos = Lists.newArrayList();
            for (Pair<Long, Boolean> pair : enablePersistentIndexList) {
                TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                metaInfo.setTablet_id(pair.first);
                metaInfo.setEnable_persistent_index(pair.second);
                metaInfo.setMeta_type(TTabletMetaType.ENABLE_PERSISTENT_INDEX);
                metaInfos.add(metaInfo);
            }
            return metaInfos;
        }
    }

    private static class UpdateBinlogConfigTask extends UpdateTabletMetaInfoTask {
        private final List<Pair<Long, BinlogConfig>> binlogConfigList;

        private UpdateBinlogConfigTask(long backendId, List<Pair<Long, BinlogConfig>> binlogConfigList) {
            super(backendId, binlogConfigList.hashCode());
            this.binlogConfigList = binlogConfigList;
        }

        @Override
        public Set<Long> getTablets() {
            return binlogConfigList.stream().map(p -> p.first).collect(Collectors.toSet());
        }

        @Override
        public List<TTabletMetaInfo> getTTabletMetaInfoList() {
            List<TTabletMetaInfo> metaInfos = Lists.newArrayList();
            for (Pair<Long, BinlogConfig> pair : binlogConfigList) {
                TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                metaInfo.setTablet_id(pair.first);
                metaInfo.setBinlog_config(pair.second.toTBinlogConfig());
                metaInfo.setMeta_type(TTabletMetaType.BINLOG_CONFIG);
                metaInfos.add(metaInfo);
            }
            return metaInfos;
        }
    }

    private static class UpdatePrimaryIndexCacheExpireTimeTask extends UpdateTabletMetaInfoTask {
        private final List<Pair<Long, Integer>> expireTimeList;

        private UpdatePrimaryIndexCacheExpireTimeTask(long backendId, List<Pair<Long, Integer>> expireTimeList) {
            super(backendId, expireTimeList.hashCode());
            this.expireTimeList = expireTimeList;
        }

        @Override
        public Set<Long> getTablets() {
            return expireTimeList.stream().map(p -> p.first).collect(Collectors.toSet());
        }

        @Override
        public List<TTabletMetaInfo> getTTabletMetaInfoList() {
            List<TTabletMetaInfo> metaInfos = Lists.newArrayList();
            for (Pair<Long, Integer> pair : expireTimeList) {
                TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                metaInfo.setTablet_id(pair.first);
                metaInfo.setPrimary_index_cache_expire_sec(pair.second);
                metaInfo.setMeta_type(TTabletMetaType.PRIMARY_INDEX_CACHE_EXPIRE_SEC);
                metaInfos.add(metaInfo);
            }
            return metaInfos;
        }
    }

}