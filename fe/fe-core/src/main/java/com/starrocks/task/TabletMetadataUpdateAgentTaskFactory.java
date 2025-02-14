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

package com.starrocks.task;

import com.google.common.collect.Lists;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TPersistentIndexType;
import com.starrocks.thrift.TTabletMetaInfo;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTabletSchema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TabletMetadataUpdateAgentTaskFactory {
    public static TabletMetadataUpdateAgentTask createGenericBooleanPropertyUpdateTask(long backendId,
                                                                                       Set<Long> tablets,
                                                                                       Boolean value,
                                                                                       TTabletMetaType metaType) {
        if (metaType == TTabletMetaType.INMEMORY) {
            return createIsInMemoryUpdateTask(backendId, tablets, value);
        }
        if (metaType == TTabletMetaType.ENABLE_PERSISTENT_INDEX) {
            return createEnablePersistentIndexUpdateTask(backendId, tablets, value);
        }
        return null;
    }

    public static TabletMetadataUpdateAgentTask createPartitionIdUpdateTask(long backendId, Set<Long> tablets) {
        return new UpdatePartitionIdTask(backendId, requireNonNull(tablets, "tablets is null"));
    }

    public static TabletMetadataUpdateAgentTask createIsInMemoryUpdateTask(long backendId, Set<Long> tablets,
                                                                           Boolean value) {
        requireNonNull(tablets, "tablets is null");
        List<Pair<Long, Boolean>> valueList =
                tablets.stream().map(id -> new Pair<>(id, value)).collect(Collectors.toList());
        return createIsInMemoryUpdateTask(backendId, valueList);
    }

    public static TabletMetadataUpdateAgentTask createIsInMemoryUpdateTask(long backendId,
                                                                           List<Pair<Long, Boolean>> inMemoryConfigs) {
        return new UpdateIsInMemoryTask(backendId, inMemoryConfigs);
    }

    public static TabletMetadataUpdateAgentTask createLakePersistentIndexUpdateTask(long backendId, Set<Long> tablets,
                                                                                    boolean enablePersistentIndex,
                                                                                    String persistentIndexType) {
        requireNonNull(tablets, "tablets is null");
        return new UpdateLakePersistentIndexTask(backendId, tablets, enablePersistentIndex, persistentIndexType);
    }

    public static TabletMetadataUpdateAgentTask createEnablePersistentIndexUpdateTask(long backend, Set<Long> tablets,
                                                                                      Boolean value) {
        requireNonNull(tablets, "tablets is null");
        List<Pair<Long, Boolean>> valueList =
                tablets.stream().map(id -> new Pair<>(id, value)).collect(Collectors.toList());
        return createEnablePersistentIndexUpdateTask(backend, valueList);
    }

    public static TabletMetadataUpdateAgentTask createEnablePersistentIndexUpdateTask(long backend,
                                                                                      List<Pair<Long, Boolean>> valueList) {
        return new UpdateEnablePersistentIndexTask(backend, requireNonNull(valueList, "valueList is null"));
    }

    public static TabletMetadataUpdateAgentTask createBinlogConfigUpdateTask(long backendId,
                                                                             Set<Long> tablets,
                                                                             BinlogConfig binlogConfig) {
        requireNonNull(tablets, "tablets is null");
        requireNonNull(binlogConfig, "binlogConfig is null");
        List<Pair<Long, BinlogConfig>> configList = tablets.stream().map(id -> new Pair<>(id, binlogConfig)).collect(
                Collectors.toList());
        return createBinlogConfigUpdateTask(backendId, configList);
    }

    public static TabletMetadataUpdateAgentTask createBinlogConfigUpdateTask(long backendId,
                                                                             List<Pair<Long, BinlogConfig>> configs) {
        return new UpdateBinlogConfigTask(backendId, requireNonNull(configs, "configs is null"));
    }

    public static TabletMetadataUpdateAgentTask createPrimaryIndexCacheExpireTimeUpdateTask(long backendId,
                                                                                            Set<Long> tablets,
                                                                                            Integer expireTime) {
        requireNonNull(tablets, "tablets is null");
        List<Pair<Long, Integer>> expireTimeList = tablets.stream().map(id -> new Pair<>(id, expireTime)).collect(
                Collectors.toList());
        return createPrimaryIndexCacheExpireTimeUpdateTask(backendId, expireTimeList);
    }

    public static TabletMetadataUpdateAgentTask createPrimaryIndexCacheExpireTimeUpdateTask(long backendId,
            List<Pair<Long, Integer>> expireTimes) {
        return new UpdatePrimaryIndexCacheExpireTimeTask(backendId, requireNonNull(expireTimes, "expireTimes is null"));
    }

    public static TabletMetadataUpdateAgentTask createTabletSchemaUpdateTask(long backendId,
                                                                             List<Long> tabletIds,
                                                                             TTabletSchema tabletSchema,
                                                                             boolean createSchemaFile) {
        return new UpdateTabletSchemaTask(backendId, tabletIds, tabletSchema, createSchemaFile);
    }

    private static class UpdatePartitionIdTask extends TabletMetadataUpdateAgentTask {
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
            TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
            List<TTabletMetaInfo> metaInfos = Lists.newArrayList();
            for (Long tabletId : tabletSet) {
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                if (tabletMeta == null) {
                    LOG.warn("could not find tablet [{}] in meta ignore it", tabletId);
                    continue;
                }
                TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                metaInfo.setTablet_id(tabletId);
                metaInfo.setPartition_id(tabletMeta.getPhysicalPartitionId());
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

    private static class UpdateIsInMemoryTask extends TabletMetadataUpdateAgentTask {
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

    private static class UpdateEnablePersistentIndexTask extends TabletMetadataUpdateAgentTask {
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

    private static class UpdateLakePersistentIndexTask extends TabletMetadataUpdateAgentTask {
        private final Set<Long> tablets;
        private boolean enablePersistentIndex;
        private String persistentIndexType;

        private UpdateLakePersistentIndexTask(long backendId, Set<Long> tablets,
                boolean enablePersistentIndex, String persistentIndexType) {
            super(backendId, Objects.hash(tablets, enablePersistentIndex, persistentIndexType));
            this.tablets = tablets;
            this.enablePersistentIndex = enablePersistentIndex;
            this.persistentIndexType = persistentIndexType;
        }

        @Override
        public Set<Long> getTablets() {
            return tablets;
        }

        @Override
        public List<TTabletMetaInfo> getTTabletMetaInfoList() {
            List<TTabletMetaInfo> metaInfos = Lists.newArrayList();
            for (Long tabletId : tablets) {
                TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                metaInfo.setTablet_id(tabletId);
                metaInfo.setEnable_persistent_index(enablePersistentIndex);
                if (persistentIndexType.equalsIgnoreCase("CLOUD_NATIVE")) {
                    metaInfo.setPersistent_index_type(TPersistentIndexType.CLOUD_NATIVE);
                } else if (persistentIndexType.equalsIgnoreCase("LOCAL")) {
                    metaInfo.setPersistent_index_type(TPersistentIndexType.LOCAL);
                } else {
                    throw new IllegalArgumentException("Unknown persistent index type: " + persistentIndexType);
                }
                metaInfo.setMeta_type(TTabletMetaType.ENABLE_PERSISTENT_INDEX);
                metaInfos.add(metaInfo);
            }
            return metaInfos;
        }
    }

    private static class UpdateBinlogConfigTask extends TabletMetadataUpdateAgentTask {
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

    private static class UpdatePrimaryIndexCacheExpireTimeTask extends TabletMetadataUpdateAgentTask {
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

    private static class UpdateTabletSchemaTask extends TabletMetadataUpdateAgentTask {
        private final List<Long> tablets;
        private final TTabletSchema tabletSchema;
        private final boolean createSchemaFile;

        private UpdateTabletSchemaTask(long backendId, List<Long> tablets, TTabletSchema tabletSchema,
                                       boolean createSchemaFile) {
            super(backendId, tablets.hashCode());
            this.tablets = new ArrayList<>(tablets);
            // tabletSchema may be null when the table has multi materialized index
            // and the schema of some materialized indexes are not needed to be updated
            this.tabletSchema = tabletSchema;
            this.createSchemaFile = createSchemaFile;
        }

        @Override
        public Set<Long> getTablets() {
            return new HashSet<>(tablets);
        }

        @Override
        public List<TTabletMetaInfo> getTTabletMetaInfoList() {
            boolean create = createSchemaFile;
            List<TTabletMetaInfo> metaInfos = new ArrayList<>(tablets.size());
            for (Long tabletId : tablets) {
                TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                metaInfo.setTablet_id(tabletId);

                if (tabletSchema != null) {
                    metaInfo.setTablet_schema(tabletSchema);
                }

                metaInfos.add(metaInfo);
                metaInfo.setCreate_schema_file(create);
                create = false;
            }
            return metaInfos;
        }
    }
}
