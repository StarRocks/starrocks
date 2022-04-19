// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.collect.Sets;
import com.starrocks.catalog.PartitionProperties;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.PrintableMap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SingleListPartitionDesc extends PartitionProperties {

    private final boolean ifNotExists;
    private final String partitionName;
    private final List<String> values;
    private final Map<String, String> partitionProperties;

    public SingleListPartitionDesc(boolean ifNotExists, String partitionName, List<String> values,
                                   Map<String, String> partitionProperties) {
        this.ifNotExists = ifNotExists;
        this.partitionName = partitionName;
        this.values = values;
        this.partitionProperties = partitionProperties;
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
        return this.ifNotExists;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.partitionProperties;
    }

    @Override
    public void analyze(int partitionColSize, Map<String, String> tableProperties) throws AnalysisException {
        if (partitionColSize != 1) {
            throw new AnalysisException("Partition column size should be one when use single list partition ");
        }
        Set<String> treeSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String value : values) {
            if (!treeSet.add(value)) {
                throw new AnalysisException("Duplicated value" + value);
            }
        }
        super.analyzeProperties(tableProperties);
    }

    @Override
    public String toSql() {
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

    @Override
    public String toString() {
        return toSql();
    }
}
