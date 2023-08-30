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

import com.google.common.base.Joiner;
import com.google.common.base.Joiner.MapJoiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.StorageCacheInfo;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletType;
import org.apache.logging.log4j.util.Strings;
import org.threeten.extra.PeriodDuration;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

    public void analyze(int partColNum, Map<String, String> otherProperties) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }

        FeNameFormat.checkPartitionName(partName);

        partitionKeyDesc.analyze(partColNum);

        // The priority of the partition attribute is higher than that of the table
        Map<String, String> partitionProperties = Maps.newHashMap();

        if (otherProperties != null) {
            if (properties == null) {
                this.properties = otherProperties;
                partitionProperties.putAll(otherProperties);
            } else {
                for (String key : otherProperties.keySet()) {
                    partitionProperties.put(key, otherProperties.get(key));
                }
                for (String key : properties.keySet()) {
                    partitionProperties.put(key, properties.get(key));
                }
                this.properties = Maps.newHashMap(partitionProperties);
            }
        }

        // analyze data property
        partitionDataProperty = PropertyAnalyzer.analyzeDataProperty(partitionProperties,
                DataProperty.getInferredDefaultDataProperty(), false);

        if (partColNum == 1 && properties != null
                && properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL)) {
            String storageCoolDownTTL = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL);
            if (Strings.isNotBlank(storageCoolDownTTL)) {
                PeriodDuration periodDuration = TimeUtils.parseHumanReadablePeriodOrDuration(storageCoolDownTTL);
                if (partitionKeyDesc.isMax()) {
                    partitionDataProperty = new DataProperty(TStorageMedium.SSD, DataProperty.MAX_COOLDOWN_TIME_MS);
                } else {
                    String stringUpperValue = partitionKeyDesc.getUpperValues().get(0).getStringValue();
                    DateTimeFormatter dateTimeFormatter = DateUtils.probeFormat(stringUpperValue);
                    LocalDateTime upperTime = DateUtils.parseStringWithDefaultHSM(stringUpperValue, dateTimeFormatter);
                    LocalDateTime updatedUpperTime = upperTime.plus(periodDuration);
                    DateLiteral dateLiteral = new DateLiteral(updatedUpperTime, Type.DATETIME);
                    long coolDownTimeStamp = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
                    partitionDataProperty = new DataProperty(TStorageMedium.SSD, coolDownTimeStamp);
                }
            }
        }

        Preconditions.checkNotNull(partitionDataProperty);
        // analyze replication num
        replicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, RunMode.defaultReplicationNum());
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
                properties, PropertyAnalyzer.PROPERTIES_ENABLE_STORAGE_CACHE, true);
        long storageCacheTtlS = PropertyAnalyzer.analyzeLongProp(
                properties, PropertyAnalyzer.PROPERTIES_STORAGE_CACHE_TTL, Config.lake_default_storage_cache_ttl_seconds);
        boolean enableAsyncWriteBack = PropertyAnalyzer.analyzeBooleanProp(
                properties, PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK, false);

        if (storageCacheTtlS < -1) {
            throw new AnalysisException("Storage cache ttl should not be less than -1");
        }
        if (!enableStorageCache && storageCacheTtlS != 0 && storageCacheTtlS != Config.lake_default_storage_cache_ttl_seconds) {
            throw new AnalysisException("Storage cache ttl should be 0 when cache is disabled");
        }
        if (enableStorageCache && storageCacheTtlS == 0) {
            throw new AnalysisException("Storage cache ttl should not be 0 when cache is enabled");
        }
        if (!enableStorageCache && enableAsyncWriteBack) {
            throw new AnalysisException("enable_async_write_back can't be turned on when cache is disabled");
        }
        storageCacheInfo = new StorageCacheInfo(enableStorageCache, storageCacheTtlS, enableAsyncWriteBack);

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
