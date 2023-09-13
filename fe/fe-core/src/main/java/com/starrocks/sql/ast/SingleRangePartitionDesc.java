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

import com.starrocks.catalog.PartitionType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class SingleRangePartitionDesc extends SinglePartitionDesc {
    private PartitionKeyDesc partitionKeyDesc;

    public SingleRangePartitionDesc(boolean ifNotExists, String partName, PartitionKeyDesc partitionKeyDesc,
                                    Map<String, String> properties) {
        this(ifNotExists, partName, partitionKeyDesc, properties, NodePosition.ZERO);
    }

    public SingleRangePartitionDesc(boolean ifNotExists, String partName, PartitionKeyDesc partitionKeyDesc,
                                    Map<String, String> properties, NodePosition pos) {
        super(ifNotExists, partName, properties, pos);
        this.type = PartitionType.RANGE;
        this.partitionKeyDesc = partitionKeyDesc;
    }

    public PartitionKeyDesc getPartitionKeyDesc() {
        return partitionKeyDesc;
    }

    public void analyze(int partColNum, Map<String, String> tableProperties) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }

        FeNameFormat.checkPartitionName(getPartitionName());
        partitionKeyDesc.analyze(partColNum);

        if (partColNum == 1) {
            analyzeProperties(tableProperties, partitionKeyDesc);
        } else {
            analyzeProperties(tableProperties, null);
        }
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

        if (partitionKeyDesc.getPartitionType() == PartitionKeyDesc.PartitionRangeType.LESS_THAN) {
            sb.append(" VALUES LESS THEN ");
        } else {
            sb.append(" VALUES ");
        }
        sb.append(partitionKeyDesc.toString());

        Map<String, String> properties = getProperties();
        if (properties != null && !properties.isEmpty()) {
            sb.append(" (");
            sb.append(new PrintableMap<>(properties, "=", true, false));
            sb.append(")");
        }
        return sb.toString();
    }
}
