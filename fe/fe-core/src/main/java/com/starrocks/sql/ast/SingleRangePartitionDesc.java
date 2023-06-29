// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Joiner;
import com.google.common.base.Joiner.MapJoiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DataProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.StorageCacheInfo;
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
        this.ifNotExists = ifNotExists;

        this.isAnalyzed = false;

        this.partName = partName;
        this.partitionKeyDesc = partitionKeyDesc;
        this.properties = properties;

        this.partitionDataProperty = DataProperty.getInferredDefaultDataProperty();
        this.replicationNum = FeConstants.default_replication_num;
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

    public void analyze(int partColNum, Map<String, String> otherProperties) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }

        FeNameFormat.checkPartitionName(partName);

        partitionKeyDesc.analyze(partColNum);

        if (otherProperties != null) {
            if (properties == null) {
                this.properties = otherProperties;
            } else {
                // The priority of the partition attribute is higher than that of the table
                Map<String, String> partitionProperties = Maps.newHashMap();
                for (String key : otherProperties.keySet()) {
                    partitionProperties.put(key, otherProperties.get(key));
                }
                for (String key : properties.keySet()) {
                    partitionProperties.put(key, properties.get(key));
                }
                this.properties = partitionProperties;
            }
        }

        // analyze data property
        partitionDataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                DataProperty.getInferredDefaultDataProperty(), false);
        Preconditions.checkNotNull(partitionDataProperty);

        // analyze replication num
        replicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, FeConstants.default_replication_num);
        if (replicationNum == null) {
            throw new AnalysisException("Invalid replication number: " + replicationNum);
        }

        // analyze version info
        versionInfo = PropertyAnalyzer.analyzeVersionInfo(properties);

        // analyze in memory
        isInMemory = PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);

        tabletType = PropertyAnalyzer.analyzeTabletType(properties);

        // analyze enable storage cache and cache ttl, and whether allow async write back
        boolean enableStorageCache = PropertyAnalyzer.analyzeBooleanProp(
                properties, PropertyAnalyzer.PROPERTIES_ENABLE_STORAGE_CACHE, false);
        long storageCacheTtlS = PropertyAnalyzer.analyzeLongProp(
                properties, PropertyAnalyzer.PROPERTIES_STORAGE_CACHE_TTL, 0);
        boolean allowAsyncWriteBack = PropertyAnalyzer.analyzeBooleanProp(
                properties, PropertyAnalyzer.PROPERTIES_ALLOW_ASYNC_WRITE_BACK, false);

        if (storageCacheTtlS < -1) {
            throw new AnalysisException("Storage cache ttl should not be less than -1");
        }
        if (!enableStorageCache && storageCacheTtlS != 0) {
            throw new AnalysisException("Storage cache ttl should be 0 when cache is disabled");
        }
        if (enableStorageCache && storageCacheTtlS == 0) {
            storageCacheTtlS = Config.tablet_sched_storage_cooldown_second;
        }
        if (!enableStorageCache && allowAsyncWriteBack) {
            throw new AnalysisException("storage allow_async_write_back can't be enabled when cache is disabled");
        }
        storageCacheInfo = new StorageCacheInfo(enableStorageCache, storageCacheTtlS, allowAsyncWriteBack);

        if (otherProperties == null) {
            // check unknown properties
            if (properties != null && !properties.isEmpty()) {
                MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator(" = ");
                throw new AnalysisException("Unknown properties: " + mapJoiner.join(properties));
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
