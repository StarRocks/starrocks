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

import com.starrocks.analysis.ColumnDef;
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

public class MultiItemListPartitionDesc extends SinglePartitionDesc {
    private final List<List<String>> multiValues;
    private List<ColumnDef> columnDefList;

    public MultiItemListPartitionDesc(boolean ifNotExists, String partitionName, List<List<String>> multiValues,
                                      Map<String, String> properties) {
        this(ifNotExists, partitionName, multiValues, properties, NodePosition.ZERO);
    }

    public MultiItemListPartitionDesc(boolean ifNotExists, String partitionName, List<List<String>> multiValues,
                                      Map<String, String> properties, NodePosition pos) {
        super(ifNotExists, partitionName, properties, pos);
        this.type = PartitionType.LIST;
        this.multiValues = multiValues;
    }

    public List<List<String>> getMultiValues() {
        return this.multiValues;
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
        if (isAnalyzed) {
            return;
        }

        FeNameFormat.checkPartitionName(getPartitionName());
        analyzeValues(columnDefList.size());
        analyzeProperties(tableProperties);
        this.columnDefList = columnDefList;

        isAnalyzed = true;
    }

    private void analyzeValues(int partitionColSize) throws AnalysisException {
        for (List<String> values : this.multiValues) {
            if (values.size() != partitionColSize) {
                throw new AnalysisException(
                        "(" + String.join(",", values) + ") size should be equal to partition column size ");
            }
        }
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
        String items = this.multiValues.stream()
                .map(values -> "(" + values.stream().map(value -> "'" + value + "'")
                        .collect(Collectors.joining(",")) + ")")
                .collect(Collectors.joining(","));
        sb.append(items);
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
