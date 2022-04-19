// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.catalog.PartitionProperties;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.PrintableMap;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MultiListPartitionDesc extends PartitionProperties {

    private final boolean ifNotExists;
    private final String partitionName ;
    private final List<List<String>> multiValues;
    private final Map<String, String> partitionProperties;

    public MultiListPartitionDesc(boolean ifNotExists, String partitionName, List<List<String>> multiValues,
                                  Map<String, String> partitionProperties) {
        this.partitionName = partitionName;
        this.ifNotExists = ifNotExists;
        this.multiValues = multiValues;
        this.partitionProperties = partitionProperties;
    }

    public List<List<String>> getMultiValues(){
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
    public Map<String, String> getProperties() {
        return this.partitionProperties;
    }

    @Override
    public void analyze(int partitionColSize, Map<String, String> tableProperties) throws AnalysisException {
        for (List<String> values : this.multiValues){
            if (values.size() != partitionColSize){
                throw new AnalysisException("(" + String.join(",",values) + ") size should be equal to partition column size ");
            }
        }
        super.analyzeProperties(tableProperties);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION ").append(this.partitionName).append(" VALUES IN (");
        String items = this.multiValues.stream()
                .map(values -> "(" + values.stream().map(value -> "\"" + value + "\"")
                                .collect(Collectors.joining(","))+")")
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
