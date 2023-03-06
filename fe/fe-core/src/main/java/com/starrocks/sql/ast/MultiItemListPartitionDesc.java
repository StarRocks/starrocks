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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.StorageCacheInfo;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TTabletType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MultiItemListPartitionDesc extends PartitionDesc {

    private final boolean ifNotExists;
    private final String partitionName;
    private final List<List<String>> multiValues;
    private final Map<String, String> properties;
    private DataProperty partitionDataProperty;
    private Short replicationNum;
    private Boolean isInMemory;
    private TTabletType tabletType;
    private Long versionInfo;
    private List<ColumnDef> columnDefList;

    public MultiItemListPartitionDesc(boolean ifNotExists, String partitionName, List<List<String>> multiValues,
                                      Map<String, String> properties) {
        this(ifNotExists, partitionName, multiValues, properties, NodePosition.ZERO);
    }

    public MultiItemListPartitionDesc(boolean ifNotExists, String partitionName, List<List<String>> multiValues,
                                      Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.type = PartitionType.LIST;
        this.partitionName = partitionName;
        this.ifNotExists = ifNotExists;
        this.multiValues = multiValues;
        this.properties = properties;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
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
        Map<String, String> partitionAndTableProperties = Maps.newHashMap();
        // The priority of the partition attribute is higher than that of the table
        if (tableProperties != null) {
            partitionAndTableProperties.putAll(tableProperties);
        }
        if (properties != null) {
            partitionAndTableProperties.putAll(properties);
        }

        // analyze data property
        this.partitionDataProperty = PropertyAnalyzer.analyzeDataProperty(partitionAndTableProperties,
                DataProperty.getInferredDefaultDataProperty());

        // analyze replication num
        this.replicationNum = PropertyAnalyzer
                .analyzeReplicationNum(partitionAndTableProperties, RunMode.defaultReplicationNum());

        // analyze version info
        this.versionInfo = PropertyAnalyzer.analyzeVersionInfo(partitionAndTableProperties);

        // analyze in memory
        this.isInMemory = PropertyAnalyzer
                .analyzeBooleanProp(partitionAndTableProperties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);

        // analyze tabletType
        this.tabletType = PropertyAnalyzer.analyzeTabletType(partitionAndTableProperties);

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
        if (this.properties != null && !this.properties.isEmpty()) {
            sb.append(" (");
            sb.append(new PrintableMap(this.properties, "=", true, false));
            sb.append(")");
        }
        return sb.toString();
    }
}
