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
import com.google.common.collect.Maps;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.StorageCacheInfo;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.thrift.TTabletType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SingleItemListPartitionDesc extends PartitionDesc {

    private final boolean ifNotExists;
    private final String partitionName;
    private final List<String> values;
    private final Map<String, String> partitionProperties;
    private DataProperty partitionDataProperty;
    private Short replicationNum;
    private Boolean isInMemory;
    private TTabletType tabletType;
    private Long versionInfo;
    private List<ColumnDef> columnDefList;

    public SingleItemListPartitionDesc(boolean ifNotExists, String partitionName, List<String> values,
                                       Map<String, String> partitionProperties) {
        super.type = PartitionType.LIST;
        this.ifNotExists = ifNotExists;
        this.partitionName = partitionName;
        this.values = values;
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

    public List<String> getValues() {
        return this.values;
    }

    @Override
    public String getPartitionName() {
        return this.partitionName;
    }

    @Override
    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    @Override
    public StorageCacheInfo getStorageCacheInfo() {
        return null;
    }

    public List<LiteralExpr> getLiteralExprValues() throws AnalysisException {
        List<LiteralExpr> partitionValues = new ArrayList<>(this.values.size());
        for (String value : this.values) {
            //there only one partition column for single partition list
            Type type = this.columnDefList.get(0).getType();
            LiteralExpr partitionValue = new PartitionValue(value).getValue(type);
            partitionValues.add(partitionValue);
        }
        return partitionValues;
    }

    public void analyze(List<ColumnDef> columnDefList, Map<String, String> tableProperties) throws AnalysisException {
        FeNameFormat.checkPartitionName(this.getPartitionName());
        if (columnDefList.size() != 1) {
            throw new AnalysisException("Partition column size should be one when use single list partition ");
        }
        this.analyzeProperties(tableProperties);
        this.columnDefList = columnDefList;
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
                DataProperty.getInferredDefaultDataProperty());

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
        sb.append(this.values.stream().map(value -> "\'" + value + "\'")
                .collect(Collectors.joining(",")));
        sb.append(")");
        if (partitionProperties != null && !partitionProperties.isEmpty()) {
            sb.append(" (");
            sb.append(new PrintableMap(partitionProperties, "=", true, false));
            sb.append(")");
        }
        return sb.toString();
    }
}