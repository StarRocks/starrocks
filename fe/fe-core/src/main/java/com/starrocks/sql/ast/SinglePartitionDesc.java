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

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.DataProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.StorageCacheInfo;
import com.starrocks.server.RunMode;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TTabletType;

import java.util.Map;

public abstract class SinglePartitionDesc extends PartitionDesc {
    private String partName;
    private boolean ifNotExists;
    private Map<String, String> properties;
    private Short replicationNum;
    private DataProperty partitionDataProperty;
    private TTabletType tabletType;
    private Long versionInfo;
    private boolean isInMemory;
    private StorageCacheInfo storageCacheInfo;

    protected boolean isAnalyzed;

    public SinglePartitionDesc(boolean ifNotExists, String partName, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.partName = partName;
        this.ifNotExists = ifNotExists;
        this.properties = properties;
        this.replicationNum = RunMode.defaultReplicationNum();
        this.partitionDataProperty = DataProperty.getInferredDefaultDataProperty();
        this.tabletType = TTabletType.TABLET_TYPE_DISK;
        this.versionInfo = null;
        this.isInMemory = false;
        this.storageCacheInfo = null;
        this.isAnalyzed = false;
    }

    @Override
    public String getPartitionName() {
        return partName;
    }

    @Override
    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public short getReplicationNum() {
        return replicationNum;
    }

    @Override
    public DataProperty getPartitionDataProperty() {
        return partitionDataProperty;
    }

    @Override
    public Long getVersionInfo() {
        return versionInfo;
    }

    @Override
    public TTabletType getTabletType() {
        return tabletType;
    }

    @Override
    public boolean isInMemory() {
        return isInMemory;
    }

    @Override
    public StorageCacheInfo getStorageCacheInfo() {
        return storageCacheInfo;
    }

    public boolean isAnalyzed() {
        return isAnalyzed;
    }

    protected void analyzeProperties(Map<String, String> tableProperties) throws AnalysisException {
        Map<String, String> partitionAndTableProperties = Maps.newHashMap();
        // The priority of the partition attribute is higher than that of the table
        if (tableProperties != null) {
            partitionAndTableProperties.putAll(tableProperties);
        }
        if (properties != null) {
            partitionAndTableProperties.putAll(properties);
        }

        // analyze data property
        partitionDataProperty = PropertyAnalyzer.analyzeDataProperty(partitionAndTableProperties,
                DataProperty.getInferredDefaultDataProperty());
        Preconditions.checkNotNull(partitionDataProperty);

        // analyze replication num
        replicationNum = PropertyAnalyzer
                .analyzeReplicationNum(partitionAndTableProperties, RunMode.defaultReplicationNum());
        if (replicationNum == null) {
            throw new AnalysisException("Invalid replication number: " + replicationNum);
        }

        // analyze version info
        versionInfo = PropertyAnalyzer.analyzeVersionInfo(partitionAndTableProperties);

        // analyze in memory
        isInMemory = PropertyAnalyzer
                .analyzeBooleanProp(partitionAndTableProperties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);

        tabletType = PropertyAnalyzer.analyzeTabletType(partitionAndTableProperties);

        // analyze enable storage cache and cache ttl, and whether allow async write back
        boolean enableStorageCache = PropertyAnalyzer.analyzeBooleanProp(
                partitionAndTableProperties, PropertyAnalyzer.PROPERTIES_ENABLE_STORAGE_CACHE, true);
        long storageCacheTtlS = PropertyAnalyzer
                .analyzeLongProp(partitionAndTableProperties, PropertyAnalyzer.PROPERTIES_STORAGE_CACHE_TTL,
                        Config.lake_default_storage_cache_ttl_seconds);
        boolean enableAsyncWriteBack = PropertyAnalyzer.analyzeBooleanProp(
                partitionAndTableProperties, PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK, false);

        if (storageCacheTtlS < -1) {
            throw new AnalysisException("Storage cache ttl should not be less than -1");
        }
        if (!enableStorageCache && storageCacheTtlS != 0 &&
                storageCacheTtlS != Config.lake_default_storage_cache_ttl_seconds) {
            throw new AnalysisException("Storage cache ttl should be 0 when cache is disabled");
        }
        if (enableStorageCache && storageCacheTtlS == 0) {
            throw new AnalysisException("Storage cache ttl should not be 0 when cache is enabled");
        }
        if (!enableStorageCache && enableAsyncWriteBack) {
            throw new AnalysisException("enable_async_write_back can't be turned on when cache is disabled");
        }
        storageCacheInfo = new StorageCacheInfo(enableStorageCache, storageCacheTtlS, enableAsyncWriteBack);

        if (properties != null) {
            // check unknown properties
            Sets.SetView<String> intersection =
                    Sets.intersection(partitionAndTableProperties.keySet(), properties.keySet());
            if (!intersection.isEmpty()) {
                Map<String, String> unknownProperties = Maps.newHashMap();
                intersection.stream().forEach(x -> unknownProperties.put(x, properties.get(x)));
                throw new AnalysisException("Unknown properties: " + unknownProperties);
            }
        }
    }
}
