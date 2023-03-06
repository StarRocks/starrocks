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
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.StorageCacheInfo;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TTabletType;

import java.util.Map;

public class SingleRangePartitionDesc extends PartitionDesc {
    private boolean isAnalyzed;

    private boolean ifNotExists;

    private String partName;
    private PartitionKeyDesc partitionKeyDesc;
    private Map<String, String> properties;

    private DataProperty partitionDataProperty;
    private Short replicationNum;
    private boolean isInMemory = false;
    private TTabletType tabletType = TTabletType.TABLET_TYPE_DISK;
    private Long versionInfo;
    private StorageCacheInfo storageCacheInfo;

    public SingleRangePartitionDesc(boolean ifNotExists, String partName, PartitionKeyDesc partitionKeyDesc,
                                    Map<String, String> properties) {
        this(ifNotExists, partName, partitionKeyDesc, properties, NodePosition.ZERO);
    }

    public SingleRangePartitionDesc(boolean ifNotExists, String partName, PartitionKeyDesc partitionKeyDesc,
                                    Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.isAnalyzed = false;

        this.partName = partName;
        this.partitionKeyDesc = partitionKeyDesc;
        this.properties = properties;

        this.partitionDataProperty = DataProperty.getInferredDefaultDataProperty();
        this.replicationNum = RunMode.defaultReplicationNum();
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public String getPartitionName() {
        return partName;
    }

    public PartitionKeyDesc getPartitionKeyDesc() {
        return partitionKeyDesc;
    }

    public DataProperty getPartitionDataProperty() {
        return partitionDataProperty;
    }

    public short getReplicationNum() {
        return replicationNum;
    }

    public boolean isInMemory() {
        return isInMemory;
    }

    public TTabletType getTabletType() {
        return tabletType;
    }

    public Long getVersionInfo() {
        return versionInfo;
    }

    @Override
    public StorageCacheInfo getStorageCacheInfo() {
        return storageCacheInfo;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public void analyze(int partColNum, Map<String, String> tableProperties) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }

        FeNameFormat.checkPartitionName(partName);

        partitionKeyDesc.analyze(partColNum);

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

        this.isAnalyzed = true;
    }

    public boolean isAnalyzed() {
        return this.isAnalyzed;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION ").append(partName);
        if (partitionKeyDesc.getPartitionType() == PartitionKeyDesc.PartitionRangeType.LESS_THAN) {
            sb.append(" VALUES LESS THEN ");
        } else {
            sb.append(" VALUES ");
        }
        sb.append(partitionKeyDesc.toString());

        if (properties != null && !properties.isEmpty()) {
            sb.append(" (");
            sb.append(new PrintableMap<>(properties, "=", true, false));
            sb.append(")");
        }

        return sb.toString();
    }
}
