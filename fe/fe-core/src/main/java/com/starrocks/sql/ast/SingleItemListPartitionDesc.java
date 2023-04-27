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

public class SingleItemListPartitionDesc extends SinglePartitionDesc {
    private final List<String> values;
    private List<ColumnDef> columnDefList;

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
        if (isAnalyzed) {
            return;
        }

        FeNameFormat.checkPartitionName(this.getPartitionName());
        analyzeProperties(tableProperties);

        if (columnDefList.size() != 1) {
            throw new AnalysisException("Partition column size should be one when use single list partition ");
        }
        this.columnDefList = columnDefList;

        isAnalyzed = true;
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
