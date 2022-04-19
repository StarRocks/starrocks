package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.thrift.TTabletType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class PartitionProperties extends PartitionDesc{

    private DataProperty partitionDataProperty;
    private Short replicationNum;
    private Boolean isInMemory;
    private TTabletType tabletType;
    private Long versionInfo;

    public short getReplicationNum() {
        return this.replicationNum;
    }

    public DataProperty getPartitionDataProperty() {
       return this.partitionDataProperty;
    }

    public Long getVersionInfo() {
        return this.versionInfo;
    }

    public TTabletType getTabletType() {
        return this.tabletType;
    }

    public boolean isInMemory() {
        return this.isInMemory;
    }

    public void analyzeProperties(Map<String, String> otherProperties) throws AnalysisException {
        FeNameFormat.checkPartitionName(this.getPartitionName());

        // copy one. because ProperAnalyzer will remove entry after analyze
        Map<String, String> copiedTableProperties = Optional.ofNullable(otherProperties)
                .map(properties -> Maps.newHashMap(properties))
                .orElseGet(() -> new HashMap<>());
        Map<String, String> copiedPartitionProperties = Optional.ofNullable(this.getProperties())
                .map(properties -> Maps.newHashMap(properties))
                .orElseGet(() -> new HashMap<>());

        // The priority of the partition attribute is higher than that of the table
        Map<String, String> allProperties = new HashMap<>();
        copiedTableProperties.forEach((k, v) -> allProperties.put(k, v));
        copiedPartitionProperties.forEach((k, v) -> allProperties.put(k, v));

        // analyze data property
        this.partitionDataProperty = PropertyAnalyzer.analyzeDataProperty(allProperties,
                DataProperty.DEFAULT_DATA_PROPERTY);

        // analyze replication num
        this.replicationNum = PropertyAnalyzer.analyzeReplicationNum(allProperties, FeConstants.default_replication_num);

        // analyze version info
        this.versionInfo = PropertyAnalyzer.analyzeVersionInfo(allProperties);

        // analyze in memory
        this.isInMemory = PropertyAnalyzer.analyzeBooleanProp(allProperties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);

        // analyze tabletType
        this.tabletType = PropertyAnalyzer.analyzeTabletType(allProperties);

        // check unknown properties
        if (copiedTableProperties.isEmpty() && !allProperties.isEmpty()) {
            Joiner.MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator(" = ");
            throw new AnalysisException("Unknown properties: " + mapJoiner.join(allProperties));
        }
    }


}