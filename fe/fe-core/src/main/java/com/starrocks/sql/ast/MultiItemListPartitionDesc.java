// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.StorageCacheInfo;
import com.starrocks.thrift.TTabletType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MultiItemListPartitionDesc extends PartitionDesc {

    private final boolean ifNotExists;
    private final String partitionName;
    private final List<List<String>> multiValues;
    private final Map<String, String> partitionProperties;
    private DataProperty partitionDataProperty;
    private Short replicationNum;
    private Boolean isInMemory;
    private TTabletType tabletType;
    private Long versionInfo;
    private List<ColumnDef> columnDefList;

    public MultiItemListPartitionDesc(boolean ifNotExists, String partitionName, List<List<String>> multiValues,
                                      Map<String, String> partitionProperties) {
        super.type = PartitionType.LIST;
        this.partitionName = partitionName;
        this.ifNotExists = ifNotExists;
        this.multiValues = multiValues;
        this.partitionProperties = partitionProperties;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.partitionProperties;
    }

    @Override
    public short getReplicationNum() {
        return this.replicationNum;
    }

    @Override
    public DataProperty getPartitionDataProperty() {
        return this.partitionDataProperty;
    }

    @Override
    public Long getVersionInfo() {
        return versionInfo;
    }

    @Override
    public TTabletType getTabletType() {
        return this.tabletType;
    }

    @Override
    public boolean isInMemory() {
        return this.isInMemory;
    }

    public List<List<String>> getMultiValues() {
        return this.multiValues;
    }

    @Override
    public String getPartitionName() {
        return this.partitionName;
    }

    @Override
    public boolean isSetIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public StorageCacheInfo getStorageCacheInfo() {
        return null;
    }

    public List<List<LiteralExpr>> getMultiLiteralExprValues() throws AnalysisException {
        List<List<LiteralExpr>> multiPartitionValues = new ArrayList<>(this.multiValues.size());
        for (List<String> values : this.multiValues) {
            List<LiteralExpr> partitionValues = new ArrayList<>(values.size());
            for (int i = 0; i < values.size(); i++) {
                String value = values.get(i);
                Type type = this.columnDefList.get(i).getType();
                LiteralExpr partitionValue = new PartitionValue(value).getValue(type);
                partitionValues.add(partitionValue);
            }
            multiPartitionValues.add(partitionValues);
        }
        return multiPartitionValues;
    }

    public void analyze(List<ColumnDef> columnDefList, Map<String, String> tableProperties) throws AnalysisException {
        FeNameFormat.checkPartitionName(this.getPartitionName());
        this.analyzeValues(columnDefList.size());
        this.analyzeProperties(tableProperties);
        this.columnDefList = columnDefList;
    }

    private void analyzeValues(int partitionColSize) throws AnalysisException {
        for (List<String> values : this.multiValues) {
            if (values.size() != partitionColSize) {
                throw new AnalysisException(
                        "(" + String.join(",", values) + ") size should be equal to partition column size ");
            }
        }
    }

    private void analyzeProperties(Map<String, String> tableProperties) throws AnalysisException {
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
                DataProperty.getInferredDefaultDataProperty(), false);

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

    @Override
    public String toString() {
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
}