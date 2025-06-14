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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * It was used to represent single-column list partition, like `PARTITION BY LIST(c)
 * But somehow it's misused in many places so current single-column list partition may also use the MultiItem.
 * This inconsistency can lead to serious bugs like the query result would be incorrect.
 * The plan to fix it:
 * 1. Step one is to trying to convert SingleItem to MultiItem when adding new partitions if it's already a MultiItem
 * 2. Remove the SingleItem and change all metadata when reloading metadata
 */
@Deprecated
public class SingleItemListPartitionDesc extends SinglePartitionDesc {
    private final List<String> values;
    private List<ColumnDef> columnDefList;

    @VisibleForTesting
    public SingleItemListPartitionDesc(boolean ifNotExists, String partitionName, List<String> values,
                                       Map<String, String> properties) {
        this(ifNotExists, partitionName, values, properties, NodePosition.ZERO);
    }

    public SingleItemListPartitionDesc(boolean ifNotExists, String partitionName, List<String> values,
                                       Map<String, String> properties, NodePosition pos) {
        super(ifNotExists, partitionName, properties, pos);
        this.type = PartitionType.LIST;
        this.values = values;
    }

    public List<String> getValues() {
        return this.values;
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
        analyzeProperties(tableProperties, null);

        if (columnDefList.size() != 1) {
            throw new AnalysisException("Partition column size should be one when use single list partition ");
        }
        this.columnDefList = columnDefList;
    }

    /**
     * {@link SingleItemListPartitionDesc}
     */
    public MultiItemListPartitionDesc upgradeToMultiItem() throws AnalysisException {
        List<List<String>> valueList = values.stream().map(Lists::newArrayList).collect(Collectors.toList());
        MultiItemListPartitionDesc desc = new MultiItemListPartitionDesc(isSetIfNotExists(), getPartitionName(),
                valueList, getProperties(), getPos());
        Preconditions.checkState(columnDefList == null, "has not analyzed");
        return desc;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION ");
        if (isSetIfNotExists()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(getPartitionName());

        sb.append(" VALUES IN (");
        sb.append(this.values.stream().map(value -> "\'" + value + "\'")
                .collect(Collectors.joining(",")));
        sb.append(")");

        Map<String, String> properties = getProperties();
        if (properties != null && !properties.isEmpty()) {
            sb.append(" (");
            sb.append(new PrintableMap(properties, "=", true, false));
            sb.append(")");
        }
        return sb.toString();
    }
}
