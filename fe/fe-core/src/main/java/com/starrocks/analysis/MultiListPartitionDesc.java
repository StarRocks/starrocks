// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.PartitionType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.thrift.TTabletType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class MultiListPartitionDesc extends PartitionDesc {

    private final boolean ifNotExists;
    private final String partitionName;
    private final List<List<String>> multiValues;
    private final Map<String, String> partitionProperties;

    private DataProperty partitionDataProperty;
    private Short replicationNum;
    private Boolean isInMemory;
    private TTabletType tabletType;
    private Long versionInfo;

    public MultiListPartitionDesc(boolean ifNotExists, String partitionName, List<List<String>> multiValues,
                                  Map<String, String> partitionProperties) {
        super.type = PartitionType.LIST;
        this.partitionName = partitionName;
        this.ifNotExists = ifNotExists;
        this.multiValues = multiValues;
        this.partitionProperties = partitionProperties;
    }

    public short getReplicationNum() {
        return this.replicationNum;
    }

    public DataProperty getPartitionDataProperty() {
        return this.partitionDataProperty;
    }

    public TTabletType getTabletType() {
        return this.tabletType;
    }

    public boolean isInMemory() {
        return this.isInMemory;
    }

    public List<List<String>> getMultiValues() {
        return this.multiValues;
    }

    public String getPartitionName() {
        return this.partitionName;
    }

    public void analyze(int partitionColSize, Map<String, String> tableProperties) throws AnalysisException {
        for (List<String> values : this.multiValues) {
            if (values.size() != partitionColSize) {
                throw new AnalysisException(
                        "(" + String.join(",", values) + ") size should be equal to partition column size ");
            }
        }
        this.analyzeDuplicateValues(partitionColSize);
        this.analyzeProperties(tableProperties);
    }

    private void analyzeProperties(Map<String, String> tableProperties) throws AnalysisException {
        FeNameFormat.checkPartitionName(this.getPartitionName());

        // copy one. because ProperAnalyzer will remove entry after analyze
        Map<String, String> copiedTableProperties = Optional.ofNullable(tableProperties)
                .map(properties -> Maps.newHashMap(properties))
                .orElseGet(() -> new HashMap<>());
        Map<String, String> copiedPartitionProperties = Optional.ofNullable(this.partitionProperties)
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
        this.replicationNum =
                PropertyAnalyzer.analyzeReplicationNum(allProperties, FeConstants.default_replication_num);

        // analyze version info
        this.versionInfo = PropertyAnalyzer.analyzeVersionInfo(allProperties);

        // analyze in memory
        this.isInMemory =
                PropertyAnalyzer.analyzeBooleanProp(allProperties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);

        // analyze tabletType
        this.tabletType = PropertyAnalyzer.analyzeTabletType(allProperties);

        // check unknown properties
        if (copiedTableProperties.isEmpty() && !allProperties.isEmpty()) {
            Joiner.MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator(" = ");
            throw new AnalysisException("Unknown properties: " + mapJoiner.join(allProperties));
        }
    }

    private void analyzeDuplicateValues(int partitionColSize) throws AnalysisException {
        Set[] container = new TreeSet[partitionColSize];
        for (List<String> values : multiValues) {
            int duplicatedSize = 0;
            for (int i = 0; i < values.size(); i++) {
                if (container[i] == null) {
                    container[i] = new TreeSet();
                }
                if (!container[i].add(values.get(i))) {
                    duplicatedSize++;
                }
            }
            if (duplicatedSize == partitionColSize) {
                throw new AnalysisException(
                        "(" + String.join(",", values) + ") size should be equal to partition column size ");
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION ").append(this.partitionName).append(" VALUES IN (");
        String items = this.multiValues.stream()
                .map(values -> "(" + values.stream().map(value -> "'" + value + "'")
                        .collect(Collectors.joining(",")) + ")")
                .collect(Collectors.joining(","));
        sb.append(items);
        sb.append(")");
        if (this.partitionProperties != null && !this.partitionProperties.isEmpty()) {
            sb.append(" (");
            sb.append(new PrintableMap(this.partitionProperties, "=", true, false));
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return this.toSql();
    }
}
